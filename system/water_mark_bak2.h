#ifndef WATER_MARK_H
#define WATER_MARK_H

// 这个是一个以水印类，实现模式是存储一段连续的事务号，用pair的形式实现。
// 每个pair是一个事务号，对应他是否完成某个阶段的标记。
// 通过维护一个最小水印minSid，来表示当前已经完成的最大事务号。
// 当一个新的事务号到来时，必须大于minSid，然后作为一个pair插入到数组中。
// 当一个事务号完成某个阶段时，找到对应的pair，标记为完成。
// 然后检查minSid对应的pair是否完成，如果完成，则将minSid向后移动，直到遇到一个未完成的pair为止。
// 这样就能高效地维护一个水印，表示已经完成的最大事务号。

// 目前这个watermark是给Aria的流式执行用的
#include <vector>
#include <utility>
#include <algorithm>
#include <stdint.h>
#include <pthread.h>
#include "helper.h"
// #include "small_lock_list.h"
#include "message.h"
// #include "global.h"
// #include 

// 写一个带key或者水印时间的，包括事务TxnManager的结构体
enum WaterMarkStatus : int { UNKNOWN = 0, PENDING, COMPLETED, REMOVED };
struct watermark_node_entry
{
public:
    /* data */
    uint64_t key; // 这里的key是事务号 (txn->get_batch_id() << 32) + (txn->return_id << 24) + txn->get_txn_id() + 1;
    WaterMarkStatus status;
    watermark_node_entry() : key(0), status(UNKNOWN) {}
    ~watermark_node_entry() {}
};

class WaterMarkList {
public:
    WaterMarkList() {
        for (uint64_t i = 0; i < g_node_cnt; i++) {
            water_mark[i] = 0;
        }
        // 初始化一下track_list，预分配内存，避免频繁分配
        list_size = g_inflight_max * g_node_cnt * 2;
        track_list = (watermark_node_entry*)mem_allocator.alloc(sizeof(watermark_node_entry) * list_size);
        memset(track_list, 0, sizeof(watermark_node_entry) * list_size);
        for (uint64_t i = 0; i < list_size; i++) {
            uint64_t batch_id = i / g_aria_batch_size;
            uint64_t txn_id = i % g_aria_batch_size;
            txn_id = txn_id * g_node_cnt + g_node_id; // 每个节点的事务号是连续的，所以可以直接除以节点数得到索引
            uint64_t key = get_batch_key(batch_id, 0, txn_id);
            track_list[i].key = i;
            track_list[i].status = UNKNOWN;
        }

        track_head = 0;
    }
    ~WaterMarkList() {}

    uint64_t get_current_watermark() {
        return water_mark[g_node_id];
    }
    uint64_t get_node_watermark(uint64_t nid) {
        return water_mark[nid];
    }
    uint64_t get_global_watermark() {
        // return UINT64_MAX - 10;
        // return glob_water_mark;
        return water_mark[g_node_id];
    }

    int key_to_index(uint64_t key) {
        std::vector<uint64_t> parts = split_batch_key(key);
        uint64_t batch_id = parts[0];
        uint64_t txn_id = parts[2];
        // 第一段是根据batch_id决定范围
        int index = batch_id * g_aria_batch_size;
        index += txn_id / g_node_cnt; // 每个节点的事务号是连续的，所以可以直接除以节点数得到索引
        return index;
    }

    // 这个是每个工作线程跑的
    bool update_local_watermark(uint64_t thd_id) {
        bool updated = false;
        uint64_t new_minSid = water_mark[g_node_id];
        // 先计算出除了当前节点以外，其他所有节点水印的最大值
        uint64_t max_other_watermark = 0;
        for (uint64_t i = 0; i < g_node_cnt; i++) {
            if (i == g_node_id) continue;
            max_other_watermark = max(max_other_watermark, water_mark[i]);
        }
        // DEBUG_SCH("[WaterMarkList] update %s max_other_watermark: %lu\n", name.c_str(), max_other_watermark);
        int pos = track_head;
        // 循环结束条件是，循环一周
        while (pos < track_head + list_size) {
            int index = pos % (list_size);
            watermark_node_entry* entry = &track_list[index];
            if (entry->key <= max_other_watermark) {
                // 如果其他节点上已经做完了这个事务号
                // 同时不是Pending或者Completed状态，代表这个事务不需要本地执行。
                // 所以直接设置为Completed状态，表示这个事务号已经完成了。
                if (entry->status == UNKNOWN) {
                    entry->status = COMPLETED;
                    DEBUG_SCH("[WaterMarkList] update %s key %lu to COMPLETED because other nodes have done it\n", name.c_str(), entry->key);
                } else {
                    DEBUG_SCH("[WaterMarkList] update %s key %lu failed status %d\n", name.c_str(), entry->key, entry->status);
                }
            }
            if (entry->status == COMPLETED) {
                // 说明这个节点已经完成了，可以把它标记为UNKNOWN
                new_minSid = max(new_minSid, entry->key);
                entry->status = UNKNOWN;
                entry->key += list_size; // 应该是给这个key加上list_size，表示这个key是给+list_size的事务预留的
                pos++;
                DEBUG_SCH("[WaterMarkList] update %s key %lu to UNKNOWN because it is COMPLETED\n", name.c_str(), entry->key);
                continue;
            }
            // if (entry->key > water_mark[g_node_id]) {
            //     water_mark[g_node_id] = entry->key;
            //     updated = true;
                // DEBUG_SCH("[WaterMarkList] update %s minsid to %lu\n", name.c_str(),water_mark[g_node_id]);
            // }
            // DEBUG_SCH("[WaterMarkList] update %s stop at key %lu, status: %d\n", name.c_str(), entry->key, entry->status);
            break;
        }
        // 更新head
        track_head = pos % (list_size);
        if (water_mark[g_node_id] < new_minSid) {
            // 说明water_mark[g_node_id]对应的节点已经被删除了，可以把water_mark[g_node_id]更新到下一个节点的key了
            water_mark[g_node_id] = new_minSid;
            updated = true;
            DEBUG_SCH("[WaterMarkList] update %s minsid to %lu\n", name.c_str(),water_mark[g_node_id]);
        }
        // update_global_watermark();
        return updated;
    }
    Message* broadcast_watermark() {
        // 这里可以直接广播minSid给所有节点，或者通过消息队列发送给所有节点
        // 这里假设有一个全局的消息队列msg_queue，可以用来发送消息
        WaterMarkMessage * msg =  (WaterMarkMessage*)Message::create_message(WATERMARK);
        msg->set_watermark(water_mark[g_node_id]);
        return msg;
    }
    void receive_watermark(uint64_t nid, uint64_t sid, uint64_t thd_id) {
        // 这里可以直接更新对应节点的水印值，然后调用update_watermark来更新minSid
        water_mark[nid] = sid;
        // update_local_watermark(thd_id);
        DEBUG_SCH("[WaterMarkList] receive watermark from node %lu, sid: %lu\n", nid, sid);
        // update_global_watermark();
    }
    void insert_watermark(uint64_t key, uint64_t thd_id) {
        // 这里可以直接插入一个新的事务号到track_list中
        // int index = key % (list_size);
        int index = key_to_index(key);
        watermark_node_entry* entry = &track_list[index];
        // assert(entry->status == UNKNOWN && entry->key == 0 ||
                // (entry->status == PENDING && entry->key == key));
        assert(entry->key == key);
        // entry->key = key;
        entry->status = PENDING;
        DEBUG_SCH("[WaterMarkList] insert watermark %lu at index %d\n", key, index);
    }
    void mark_completed(uint64_t key, uint64_t thd_id) {
        // 这里可以直接标记一个事务号为完成状态
        // int index = key % (list_size);
        int index = key_to_index(key);
        watermark_node_entry* entry = &track_list[index];
        assert(entry->key == key);
        assert(entry->status == PENDING);
        entry->status = COMPLETED;
        DEBUG_SCH("[WaterMarkList] mark watermark %lu as completed at index %d\n", key, index);
    }
private:
    uint64_t water_mark[NODE_CNT];
    uint64_t glob_water_mark = 0; // 这个是全局的水印，表示所有节点都已经完成的最大事务号

    // 这玩意是循环数组
    watermark_node_entry *track_list; // 这个是每个节点的水印，表示该节点已经完成的最大事务号
    int track_head;
    int list_size;
    // int track_tail;

    string name = "WaterMarkList";
};


#endif
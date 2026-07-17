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
// enum WaterMarkStatus : int { UNKNOWN = 0, PENDING, COMPLETED, REMOVED };
// struct watermark_node_entry
// {
// public:
//     /* data */
//     uint64_t key; // 这里的key是事务号 (txn->get_batch_id() << 32) + (txn->return_id << 24) + txn->get_txn_id() + 1;
//     WaterMarkStatus status;
//     watermark_node_entry() : key(0), status(UNKNOWN) {}
//     ~watermark_node_entry() {}
// };

class WaterMarkList {
public:
    WaterMarkList() {
        for (uint64_t i = 0; i < g_node_cnt; i++) {
            water_mark[i] = 0;
        }
        for (uint64_t i = 0; i < g_thread_cnt; i++) {
            sids[i] = 0;
        }
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
        return glob_water_mark;
        // return water_mark[g_node_id];
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
        uint64_t old_sid = water_mark[g_node_id];
        uint64_t min = UINT64_MAX;
        for (uint64_t i = 0; i < g_thread_cnt; i++) {
            uint64_t current_sid = sids[i];
            if (current_sid < min) min = current_sid;
        }
        assert(min >= old_sid);
        if (min == old_sid) {
            return false;
        } else {
            water_mark[g_node_id] = min;
            DEBUG_SCH("[WaterMarkList] update local watermark to %lu\n", water_mark[g_node_id]);
            update_global_watermark();
        }
        return true;
    }
    void update_global_watermark() {
        uint64_t new_glob_water_mark = UINT64_MAX;
        for (uint64_t i = 0; i < g_node_cnt; i++) {
            new_glob_water_mark = min(new_glob_water_mark, water_mark[i]);
        }
        if (glob_water_mark < new_glob_water_mark) {
            glob_water_mark = new_glob_water_mark;
            DEBUG_SCH("[WaterMarkList] update global watermark to %lu\n", glob_water_mark);
        }
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
        update_global_watermark();
    }
    void mark_completed(uint64_t key, uint64_t thd_id) {
        uint64_t old_sid = sids[thd_id];
		assert(key > water_mark[g_node_id]);
		assert(key > sids[thd_id]);
		sids[thd_id] = key;
        DEBUG_SCH("[WaterMarkList] mark watermark %lu as completed by thread %lu, old sid: %lu, new sid: %lu, now sids: %s\n", key, thd_id, old_sid, sids[thd_id], get_sids_str().c_str());
    }
    std::string get_sids_str() {
        std::string str = "[";
        for (uint64_t i = 0; i < g_thread_cnt; i++) {
            str += std::to_string(sids[i]);
            if (i != g_thread_cnt - 1) str += ",";
        }
        str += "]";
        return str;
    }
private:
    uint64_t sids[THREAD_CNT]; // 每个线程的水印
    uint64_t water_mark[NODE_CNT]; // 远程的水印
    uint64_t glob_water_mark = 0; // 这个是全局的水印，表示所有节点都已经完成的最大事务号

    // 这玩意是循环数组
    // watermark_node_entry *track_list; // 这个是每个节点的水印，表示该节点已经完成的最大事务号
    // int track_head;
    // int list_size;
    // int track_tail;

    string name = "WaterMarkList";
};


#endif
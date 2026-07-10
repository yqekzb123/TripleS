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
#include "small_lock_list.h"
#include "message.h"
// #include "global.h"
// #include 

// 写一个带key或者水印时间的，包括事务TxnManager的结构体
struct watermark_node_entry
{
public:
    /* data */
    uint64_t key; // 这里的key是事务号 (txn->get_batch_id() << 32) + (txn->return_id << 24) + txn->get_txn_id() + 1;

    watermark_node_entry() : key(0){}
    ~watermark_node_entry() {}
};

class WaterMarkList : public LockList<watermark_node_entry*> {
public:
    WaterMarkList() : WaterMarkList("WaterMarkList") {
    }
    WaterMarkList(std::string list_name) : LockList<watermark_node_entry*>(list_name) {
        for (uint64_t i = 0; i < g_node_cnt; i++) {
            water_mark[i] = 0;
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
public:
    bool update_local_watermark() {
        // 直接读取head->next开始遍历，找到第一个不需要删除的节点
        bool updated = false;
        ListNode<watermark_node_entry*>* curr = head->next;;
        uint64_t new_minSid = water_mark[g_node_id];
        while (curr) {
            if (curr->status == NODE_REMOVED || 
                curr->status == NODE_TAKEN) {
                curr = curr->next;
                continue;
            }
            new_minSid = max(new_minSid, curr->data->key);
            break;
        }
        if (water_mark[g_node_id] < new_minSid) {
            // 说明water_mark[g_node_id]对应的节点已经被删除了，可以把water_mark[g_node_id]更新到下一个节点的key了
            water_mark[g_node_id] = new_minSid;
            updated = true;
            DEBUG_SCH("[WaterMarkList] update %s minsid to %lu\n", name.c_str(),water_mark[g_node_id]);
        }
        update_global_watermark();
        return updated;
    }
    Message* broadcast_watermark() {
        // 这里可以直接广播minSid给所有节点，或者通过消息队列发送给所有节点
        // 这里假设有一个全局的消息队列msg_queue，可以用来发送消息
        WaterMarkMessage * msg =  (WaterMarkMessage*)Message::create_message(WATERMARK);
        msg->set_watermark(water_mark[g_node_id]);
        return msg;
    }
    void receive_watermark(uint64_t nid, uint64_t sid) {
        // 这里可以直接更新对应节点的水印值，然后调用update_watermark来更新minSid
        water_mark[nid] = sid;
        DEBUG_SCH("[WaterMarkList] receive watermark from node %lu, sid: %lu\n", nid, sid);
        update_global_watermark();
    }
    void update_global_watermark() {
        // 这里可以直接遍历所有节点的水印值，找到最小的那个作为全局水印
        uint64_t new_glob_water_mark = UINT64_MAX;
        for (uint64_t i = 0; i < g_node_cnt; i++) {
            new_glob_water_mark = min(new_glob_water_mark, water_mark[i]);
        }
        if (glob_water_mark < new_glob_water_mark) {
            glob_water_mark = new_glob_water_mark;
            DEBUG_SCH("[WaterMarkList] update global watermark to %lu\n", glob_water_mark);
        }
    }
private:
    uint64_t water_mark[NODE_CNT];
    uint64_t glob_water_mark = 0; // 这个是全局的水印，表示所有节点都已经完成的最大事务号
};


#endif
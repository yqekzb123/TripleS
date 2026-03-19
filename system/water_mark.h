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

#if 0
class WaterMark {
public:
    uint64_t minSid;
    pthread_mutex_t latch;
    std::vector<std::pair<uint64_t, bool> > sid_list; // pair<sid, is_completed>

    WaterMark() {
        minSid = 0;
        pthread_mutex_init(&latch, NULL);
        sid_list.clear();
    }   
    void init() {
        minSid = 0;
        pthread_mutex_init(&latch, NULL);
        sid_list.clear();
    }
    // 插入一个新的事务号，应该是服务器在接收到事务时插入。
    bool insert_sid(uint64_t key) {
        pthread_mutex_lock(&latch);
        if (key < minSid) {
            assert(false);
            pthread_mutex_unlock(&latch);
            return false;
        }
        auto it = std::lower_bound(sid_list.begin(), sid_list.end(), std::make_pair(key, false));
        if (it != sid_list.end() && it->first == key) {
            // 这样的情况不应该存在
            assert(false);
            pthread_mutex_unlock(&latch);
            return true;
        }
        sid_list.insert(it, std::make_pair(key, false));
        pthread_mutex_unlock(&latch);
        return true;
    }

    // 标记一个事务号为完成，应该是在事务完成某个阶段时调用。
    // !目前感觉最大的瓶颈在这里，因为每次标记完成都要遍历sid_list。
    // 不过还没验证。
    bool mark_completed(uint64_t key) {
        pthread_mutex_lock(&latch);
        auto it = std::lower_bound(sid_list.begin(), sid_list.end(), std::make_pair(key, false));
        if (it == sid_list.end() || it->first != key) {
            // 这样的情况不应该存在
            assert(false);
            pthread_mutex_unlock(&latch);
            return false;   
        }
        it->second = true;
        // 更新minSid，是把连续为true的都删除，然后把minSid改成连续为true里面最大的
        uint64_t oldSid = minSid;
        uint64_t temp = minSid;

        while (!sid_list.empty() && sid_list.front().second) {
            temp = temp > sid_list.front().first ? temp : sid_list.front().first;
            sid_list.erase(sid_list.begin());
        }
        minSid = temp;
        pthread_mutex_unlock(&latch);
        return true;
    }
};
#endif
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
    WaterMarkList() : LockList<watermark_node_entry*>("WaterMarkList") {}
    WaterMarkList(std::string list_name) : LockList<watermark_node_entry*>(list_name) {}
    ~WaterMarkList() {}

    uint64_t minSid;
public:
    void update_minsid() {
        // 直接读取head->next开始遍历，找到第一个不需要删除的节点
        ListNode<watermark_node_entry*>* curr = head->next;;
        uint64_t new_minSid = minSid;
        while (curr) {
            if (curr->status == NODE_REMOVED || 
                curr->status == NODE_TAKEN) {
                curr = curr->next;
                continue;
            }
            new_minSid = max(new_minSid, curr->data->key);
            break;
        }
        minSid = new_minSid;
        DEBUG_SCH("[WaterMarkList] update %s minsid to %lu\n", name.c_str(),minSid);
    }
};


#endif
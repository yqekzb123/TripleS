/*
   Copyright 2016 Massachusetts Institute of Technology

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#ifndef _CARACAL_H_
#define _CARACAL_H_

#include "row.h"
#include "semaphore.h"
#include <vector>
#include <unordered_set>
#include <unordered_map>

// For simplicity, the txn hisotry for OCC is oganized as follows:
// 1. history is never deleted.
// 2. hisotry forms a single directional list.
//		history head -> hist_1 -> hist_2 -> hist_3 -> ... -> hist_n
//    The head is always the latest and the tail the youngest.
// 	  When history is traversed, always go from head -> tail order.

class TxnManager;
class row_t;
class Message;

struct RowPtrHash {
    std::size_t operator()(row_t* row) const noexcept {
        if (row == nullptr) return 0;
        // hash函数改一下，要把table也考虑进来
        return std::hash<uint64_t>{}(row->get_primary_key()) ^ std::hash<void*>{}(row->get_table());
        // return std::hash<uint64_t>{}(row->get_primary_key());
    }
};

struct RowPtrEqual {
    bool operator()(row_t* lhs, row_t* rhs) const noexcept {
        if (lhs == rhs) return true;
        if (lhs == nullptr || rhs == nullptr) return false;
        return lhs->get_primary_key() == rhs->get_primary_key() && 
                lhs->get_table() == rhs->get_table();
    }
};
typedef std::unordered_set<row_t*,RowPtrHash,RowPtrEqual> RowSet;
// Map from primary key (uint64_t) to row pointer. Use default hash/equality for uint64_t.
typedef std::unordered_map<uint64_t, row_t*> RowSetMap;

class CaracalThreadContent{
public:
	CaracalThreadContent(){
        msg_list.reserve(100); // 先预分配一些空间，避免频繁扩容
        msg_list.clear();
    }
    // 记录当前线程是否完成了当前阶段
    bool phase_done = false;
	// 这里得记录这一个epoch内，当前线程跑哪些事务
    std::vector<Message*> msg_list;
    // 用于init phase, batch append机制，用于记录还有哪些row只是append到了临时队列上
    RowSet tmp_row_list;

    RowSet access_row_list;

    RowSetMap hot_row_list;
};

// struct CaracalTxnValue {
//     std::atomic<uint32_t> caracal_expected_rsp_cnt;
//     CaracalTxnValue() : caracal_expected_rsp_cnt(0) {}
// };

// class CaracalTxnAck {
//     std::unordered_map<uint64_t, CaracalTxnValue> ack_map;
//     // track responses that arrived before the txn key was initialized
//     std::unordered_map<uint64_t, uint32_t> pending_decrements;

//     pthread_mutex_t *ack_map_mutex;
// public:
//     CaracalTxnAck() {
//         ack_map_mutex = new pthread_mutex_t;
//         pthread_mutex_init(ack_map_mutex, NULL);
//     }
//     bool try_init_txn_key(uint64_t batch_id, uint64_t txn_id, uint32_t expected_rsp_cnt) {
//         return try_init_txn_key(get_batch_key(batch_id, 0, txn_id), expected_rsp_cnt);
//     }

//     bool try_init_txn_key(uint64_t key, uint32_t expected_rsp_cnt) {
//         uint32_t expected = 0;
//         pthread_mutex_lock(ack_map_mutex);
//         bool result = ack_map[key].caracal_expected_rsp_cnt.compare_exchange_strong(expected, expected_rsp_cnt);
//         pthread_mutex_unlock(ack_map_mutex);
//         return result;
//     }

//     void decrement_rsp_cnt(uint64_t batch_id, uint64_t txn_id) {
//         decrement_rsp_cnt(get_batch_key(batch_id, 0, txn_id));
//     }

//     void decrement_rsp_cnt(uint64_t key) {
//         pthread_mutex_lock(ack_map_mutex);
//         auto it = ack_map.find(key);
//         if (it != ack_map.end()) {
//             // key exists, safe to decrement without holding mutex
//             pthread_mutex_unlock(ack_map_mutex);
//             it->second.caracal_expected_rsp_cnt.fetch_sub(1);
//         } else {
//             // key not yet initialized: record a pending decrement so that when try_init
//             // initializes the key we can apply the early responses.
//             pending_decrements[key]++;
//             pthread_mutex_unlock(ack_map_mutex);
//             // don't assert - this can legitimately happen if remote responses arrive
//             // before the local analysis initializes the expected count.
//             DEBUG_WRK("Ack map missing key %lu: recorded pending decrement (total %u)\n", key, pending_decrements[key]);
//         }
//     }

//     uint32_t get_rsp_cnt(uint64_t batch_id, uint64_t txn_id) {
//         return get_rsp_cnt(get_batch_key(batch_id, 0, txn_id));
//     }

//     uint32_t get_rsp_cnt(uint64_t key) {
//         pthread_mutex_lock(ack_map_mutex);
//         auto it = ack_map.find(key);
//         if (it != ack_map.end()) {
//             pthread_mutex_unlock(ack_map_mutex);
//             return it->second.caracal_expected_rsp_cnt.load();
//         } else {
//             // if not initialized yet, return 0 to indicate no outstanding expected responses
//             // (callers should normally only call this for initialized txns)
//             uint32_t pending = 0;
//             auto pit = pending_decrements.find(key);
//             if (pit != pending_decrements.end()) pending = pit->second;
//             pthread_mutex_unlock(ack_map_mutex);
//             DEBUG_WRK("get_rsp_cnt: key %lu not initialized, pending decrements %u\n", key, pending);
//             return 0;
//         }
//     }
// };

// 用来存各个线程上的一些信息
class Caracal {
public:
	void init(int thread_cnt) {
		this->thread_cnt = thread_cnt;
		caracal_thread_list = new CaracalThreadContent[thread_cnt];
	}
	void free() {
		delete[] caracal_thread_list;
	}
	void insert_msg(uint64_t thd_id, Message* msg) {
		caracal_thread_list[thd_id % thread_cnt].msg_list.push_back(msg);
	}
	void insert_temp_row(uint64_t thd_id, row_t* row) {
		caracal_thread_list[thd_id % thread_cnt].tmp_row_list.insert(row);
	}
    void insert_access_row(uint64_t thd_id, row_t* row) {
        caracal_thread_list[thd_id % thread_cnt].access_row_list.insert(row);
    }
    CaracalThreadContent* get_thread_content(uint64_t thd_id) {
        return &caracal_thread_list[thd_id % thread_cnt];
    }

    bool in_hot_row_map(uint64_t thd_id, uint64_t key) {
        return caracal_thread_list[thd_id % thread_cnt].hot_row_list.find(key) != caracal_thread_list[thd_id % thread_cnt].hot_row_list.end();
    }

    bool is_phase_done(uint64_t thd_id) {
        return caracal_thread_list[thd_id % thread_cnt].phase_done;
    }
    void set_phase_done(uint64_t thd_id) {
        caracal_thread_list[thd_id % thread_cnt].phase_done = true;
        DEBUG_WRK("Thread %ld set phase_done to true\n", thd_id);
    }

    void set_phase_undone_for_all() {
        DEBUG_WRK("System set all phase_done to false\n");
        for (int i = 0; i < thread_cnt; i++) {
            caracal_thread_list[i].phase_done = false;
        }
    }

	int thread_cnt;
    CaracalThreadContent* caracal_thread_list;	
    // CaracalTxnAck caracal_txn_ack_man;
};

#endif

/*
    Copyright 2026 Shandong University

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

#if CC_ALG == SDOCC// || CC_ALG == SILO
#ifndef _SDOCC_SEQUENCER_H_
#define _SDOCC_SEQUENCER_H_

#include "global.h"
#include "query.h"
#include "sequencer.h"
#include <unordered_map>
#include <boost/lockfree/queue.hpp>

class Workload;
class BaseQuery;
class Message;

#define SIMPLE_SDOCC_BATCH 1
#if SIMPLE_SDOCC_BATCH

class SDOCCSequencer{
 public:
    void init(Workload *wl);
    void put_one_txn_to_batch(uint64_t _thd_id, Message * msg);
 private:
    void check_participants(Message * msg, Workload * wl);
    Workload * _wl;

    uint64_t batch_id = 0; // 下一个batch的id，也可以理解为当前batch的id，因为当前batch还没有发出去，只有等下一个事务来了才会发出当前batch
    volatile uint64_t next_txn_id; // 给当前batch内的事务分配id，保证同一batch内的事务id递增
    // 是不是可以不维护batch队列？只留一个事务计数即可，每到1000个事务，就pipeline_next_batch_id加1，然后清空计数器
    volatile uint64_t pipeline_txn_count = 0;
};
#else
typedef struct sdocc_txn_entry {
    BaseQuery * qry;
    uint32_t client_id;
    uint64_t client_startts;
    uint64_t seq_startts;
    uint64_t seq_first_startts;
    // uint64_t skew_startts;
    uint64_t total_batch_time;
    // uint32_t server_ack_cnt;
    uint32_t abort_cnt;
    Message * msg;
} sdocc_txn;

struct PBatch {
    uint64_t id;
    std::vector<sdocc_txn*> txns;
    uint64_t txns_cnt;
    uint64_t complete_cnt;
    bool sent;
    PBatch(uint64_t _id) : id(_id), txns_cnt(0), complete_cnt(0), sent(false) {
        txns.clear();
    }
    void txn_complete() {
        ATOM_ADD(complete_cnt, 1);
        // assert(complete_cnt <= txns.size());
    }
    void txn_start() {
        ATOM_ADD(txns_cnt, 1);
    }
    bool batch_complete() {
        return complete_cnt >= g_aria_batch_size;
    }
};


// 维护batch队列的写法
class SDOCCSequencer{
 public:
    void init(Workload *wl);
	void process_ack(Message * msg, uint64_t thd_id);
	void send_next_batch(uint64_t thd_id);
    // 
    void put_one_txn_to_batch(uint64_t _thd_id, Message * msg);
    bool is_batch_ready();
    void advance_seq_epoch();
 private:
    void check_participants(Message * msg, Workload * wl);
    Workload * _wl;

    uint64_t pipeline_next_batch_id = 0; // 下一个batch的id，也可以理解为当前batch的id，因为当前batch还没有发出去，只有等下一个事务来了才会发出当前batch
    volatile uint64_t next_txn_id; // 给当前batch内的事务分配id，保证同一batch内的事务id递增

    std::vector<PBatch*> pipeline_batches;  // 所有batch
    PBatch * pipeline_current_batch = nullptr;  // 当前正在准备的batch

    uint64_t last_batch_time;      // 用于统计batch时间
    uint64_t pipeline_txns_left = 0;     // 当前定序器中剩余的事务数量
};
#endif
#endif
#endif
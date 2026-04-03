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

#if CC_ALG == SDOCC
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
    uint64_t txns_left;
    bool sent;
    PBatch(uint64_t _id) : id(_id), txns_left(0), sent(false) {}
};

class SDOCCSequencer{
 public:
    void init(Workload *wl);
	void process_ack(Message * msg, uint64_t thd_id);
	void send_next_batch(uint64_t thd_id);
    // 
    void put_one_txn_to_batch(uint64_t _thd_id) ;
 private:
    void check_participants(Message * msg, Workload * wl);

    volatile uint64_t next_txn_id;
    volatile uint64_t batch_id;
    uint64_t last_batch_time;
    uint64_t txns_left;
    Workload * _wl;

    std::vector<PBatch*> pipeline_batches;
    PBatch * pipeline_current_batch = nullptr;
    uint64_t pipeline_next_batch_id = 0;
    uint64_t pipeline_txns_left = 0;
};
#endif
#endif
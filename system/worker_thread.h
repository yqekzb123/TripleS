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

#ifndef _WORKERTHREAD_H_
#define _WORKERTHREAD_H_

#include "global.h"
#include "thread.h"
#include "ordered_list.h"
#include "txn.h"
class Workload;
class Message;

class WorkerThread : public Thread {
public:
    RC run();
    void setup();
    void statqueue(uint64_t thd_id, Message * msg, uint64_t starttime);
    RC process(Message * msg);
    void check_if_done(RC rc);
    void release_txn_man();
    void commit();
    void abort();
    TxnManager * get_transaction_manager(Message * msg);
    void calvin_wrapup();
    void calvin_abort();
    RC process_rfin(Message * msg);
    RC process_rfwd(Message * msg);
    RC process_rack_rfin(Message * msg);
    RC process_rack_prep(Message * msg);
    RC process_rqry_rsp(Message * msg);
    RC process_rqry(Message * msg);
    RC process_rqry_cont(Message * msg);
    RC process_rinit(Message * msg);
    RC process_rprepare(Message * msg);
    RC process_rtxn(Message * msg);
    RC process_calvin_rtxn(Message * msg);
#if CC_ALG == ARIA
    RC process_aria_rtxn(Message * msg);
    RC process_aria_ack(Message * msg);
#endif
    RC process_rtxn_cont(Message * msg);
    RC process_log_msg(Message * msg);
    RC process_log_msg_rsp(Message * msg);
    RC process_log_flushed(Message * msg);
    RC init_phase();
    uint64_t get_next_txn_id();
    bool is_cc_new_timestamp();
private:
    uint64_t _thd_txn_id;
    ts_t        _curr_ts;
    ts_t        get_next_ts();
    TxnManager * txn_man;

    #if CC_ALG == SDOCC
    // 帮我写一个进行比较的函数
    struct CompareTxnManager {
        // bool operator() (TxnManager* a, uint64_t watermark) const {
        //     uint64_t key_a = get_batch_key(a->get_batch_id(), a->return_id, a->get_txn_id());
        //     return key_a < watermark;
        // }
        bool operator() (TxnManager* a, TxnManager* b) const {
            uint64_t key_a = get_batch_key(a->get_batch_id(), a->return_id, a->get_txn_id());
            uint64_t key_b = get_batch_key(b->get_batch_id(), b->return_id, b->get_txn_id());
            return key_a < key_b;
        }
    };
    struct CompareTxnWater {
        bool operator() (TxnManager* a, uint64_t watermark) const {
            uint64_t key_a = get_batch_key(a->get_batch_id(), a->return_id, a->get_txn_id());
            return key_a <= watermark;
        }
    };
    // 用来放还不能重试的事务
    OrderedList<TxnManager*,CompareTxnManager,CompareTxnWater> tmp_txn_list;
    uint64_t tmp_txn_list_size = 0;
    // std::vector<TxnManager*> tmp_txn_list;

    void handle_tmp_txn(uint64_t current_minSid, uint64_t &old_minSid);
    #endif
};

class StatsPerIntervalThread : public Thread {
public:
    RC run();
    void setup();

};
#endif

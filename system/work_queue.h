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

#ifndef _WORK_QUEUE_H_
#define _WORK_QUEUE_H_

#include "global.h"
#include "helper.h"
#include <queue>
#include <boost/lockfree/queue.hpp>
#include <boost/circular_buffer.hpp>
#include "semaphore.h"
#include "small_lock_list.h"
#include "circle_list.h"
#include "ordered_list.h"
//#include "message.h"

class BaseQuery;
class Workload;
class Message;

struct work_queue_entry {
    Message * msg;
    uint64_t batch_id;
    uint64_t txn_id;
    RemReqType rtype;
    uint64_t starttime;

    uint64_t original_return_node_id;
    string get_message_name() {
        return rtype_to_string(rtype);
    }
    // 帮我重载 << 运算符，方便打印，只需要打印bid和tid就行了
    friend ostream& operator<<(ostream& os, const work_queue_entry& entry) {
        os << "work_queue_entry{" << entry.batch_id << "," << entry.txn_id << "}";
        return os;
    }
};

typedef boost::circular_buffer<work_queue_entry*> WCircularBuffer;
#if CC_ALG == CARACAL
struct CompareWorkQueueEntry {
    bool operator() (work_queue_entry* a, work_queue_entry* b) const {
        uint64_t key_a = get_batch_key(a->batch_id, a->original_return_node_id, a->txn_id);
        uint64_t key_b = get_batch_key(b->batch_id, b->original_return_node_id, b->txn_id);
        return key_a < key_b;
    }
};
struct CompareWorkQueue {
    bool operator() (work_queue_entry* a, uint64_t watermark) const {
        return true;
    }
};
// 用来放还不能重试的事务
typedef OrderedList<work_queue_entry*,CompareWorkQueueEntry,CompareWorkQueue> CaracalQueue;
#endif
class QWorkQueue {
public:
    void init();
    void statqueue(uint64_t thd_id, work_queue_entry * entry);
    void enqueue(uint64_t thd_id,Message * msg,bool busy);
    Message * dequeue(uint64_t thd_id);
    void sched_enqueue(uint64_t thd_id, Message * msg);
    Message * sched_dequeue(uint64_t thd_id);
    void sequencer_enqueue(uint64_t thd_id, Message * msg);
    Message * sequencer_dequeue(uint64_t thd_id);

    // 判断能不能取出事务的两个条件函数
    void insert_list_lockfree(uint64_t thd_id,
                      TxnMsgLockList * list,
                      Message * msg, TxnManager * txn);
    TxnManager * get_txn_from_list_lockfree(uint64_t thd_id, TxnMsgLockList * list, uint64_t &key);
    Message * get_msg_from_list_lockfree(uint64_t thd_id, TxnMsgLockList * list, uint64_t &key);
    // 用于SDPCC的

#if CC_ALG == SDPCC
    void insert_sdpcc_list_lockfree(uint64_t thd_id, TxnManager * txn);
    TxnManager * get_from_sdpcc_list_lockfree(uint64_t thd_id, uint64_t &key);
    Message * sdpcc_sched_dequeue(uint64_t thd_id);
#endif

#if CC_ALG == ARIA
    Message * txn_dequeue(uint64_t thd_id);
    void work_enqueue(uint64_t thd_id, Message * msg, bool not_ready, ARIA_PHASE phase);
    Message * work_dequeue(uint64_t thd_id);
#endif

#if CC_ALG == CARACAL
    Message * txn_dequeue(uint64_t thd_id);
    void work_enqueue(uint64_t thd_id, Message * msg, bool not_ready, CARACAL_PHASE phase);
    Message * work_dequeue(uint64_t thd_id);
#endif

#if CC_ALG == SDOCC// || CC_ALG == SILO
    Message * sdocc_sequencer_dequeue(uint64_t thd_id);
    Message * txn_dequeue(uint64_t thd_id);
    void sdocc_enqueue(uint64_t thd_id, Message* msg, bool not_ready);
    Message * sdocc_dequeue(uint64_t thd_id);
    void insert_sdocc_list_lockfree(uint64_t thd_id, TxnManager * txn);
    TxnManager * get_from_sdocc_list_lockfree(uint64_t thd_id);
#endif

    uint64_t get_cnt() {return get_wq_cnt() + get_rem_wq_cnt() + get_new_wq_cnt();}
    uint64_t get_wq_cnt() {return 0;}
    //uint64_t get_wq_cnt() {return work_queue.size();}
    uint64_t get_txn_cnt() {return txn_queue_size;}

    uint64_t get_enwq_cnt() {return work_enqueue_size;}
    uint64_t get_dewq_cnt() {return work_dequeue_size;}
    uint64_t get_entxn_cnt() {return txn_enqueue_size;}
    uint64_t get_detxn_cnt() {return txn_dequeue_size;}

    void set_enwq_cnt() { work_enqueue_size = 0;}
    void set_dewq_cnt() { work_dequeue_size = 0;}
    void set_entxn_cnt() { txn_enqueue_size = 0;}
    void set_detxn_cnt() { txn_dequeue_size = 0;}
    uint64_t get_sched_wq_cnt() {return 0;}
    uint64_t get_rem_wq_cnt() {return 0;}
    uint64_t get_new_wq_cnt() {return 0;}
    Message* top_element;
    // uint64_t get_rem_wq_cnt() {return remote_op_queue.size();}
    // uint64_t get_new_wq_cnt() {return new_query_queue.size();}

    // PIP Calvin相关
    #if CC_ALG == SDPCC
    bool sched_ready;
    TxnMsgLockList * sdpcc_scheduled_list_lockfree;
    CircleList* sdpcc_list;
    #endif

    #if CC_ALG == SDOCC// || CC_ALG == SILO
    bool sdocc_ready;
    TxnMsgLockList * sdocc_lockfree;
    #endif

    #if CC_ALG == ARIA
    bool read_ready;
    TxnMsgLockList * aria_read_lockfree;
    bool reserve_ready;
    TxnMsgLockList * aria_reserve_lockfree;
    bool check_ready;
    TxnMsgLockList * aria_check_lockfree;
    bool commit_ready;
    TxnMsgLockList * aria_commit_lockfree;
    #endif


private:
    boost::lockfree::queue<work_queue_entry* > * work_queue;
    boost::lockfree::queue<work_queue_entry* > * new_txn_queue;
    boost::lockfree::queue<work_queue_entry* > * seq_queue;
    boost::lockfree::queue<work_queue_entry* > ** sched_queue;
#if CC_ALG == SDOCC// || CC_ALG == SILO
    boost::lockfree::queue<work_queue_entry* > * sdocc_queue;
#endif

#if CC_ALG == ARIA
    boost::lockfree::queue<work_queue_entry* > * aria_read_queue;
    boost::lockfree::queue<work_queue_entry* > * aria_reserve_queue;
    boost::lockfree::queue<work_queue_entry* > * aria_check_queue;
    boost::lockfree::queue<work_queue_entry* > * aria_commit_queue;
#endif

#if CC_ALG == CARACAL
    boost::lockfree::queue<work_queue_entry* > * caracal_init_queue;
    CaracalQueue* caracal_execute_queues;
    // boost::lockfree::queue<work_queue_entry* > * caracal_execute_queue;
#endif
    uint64_t sched_ptr;
    BaseQuery * last_sched_dq;
    uint64_t curr_epoch;

    sem_t 	_semaphore;
    volatile uint64_t work_queue_size;
    volatile uint64_t txn_queue_size;

    uint64_t work_enqueue_size;
    uint64_t work_dequeue_size;
    uint64_t txn_enqueue_size;
    uint64_t txn_dequeue_size;
};
#endif

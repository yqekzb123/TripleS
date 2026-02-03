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
};

struct CompareSchedEntry {
  bool operator()(const work_queue_entry* lhs, const work_queue_entry* rhs) {
    if (lhs->batch_id == rhs->batch_id) return lhs->starttime > rhs->starttime;
    return lhs->batch_id < rhs->batch_id;
  }
};
struct CompareWQEntry {
#if PRIORITY == PRIORITY_FCFS
  bool operator()(const work_queue_entry* lhs, const work_queue_entry* rhs) {
    return lhs->starttime < rhs->starttime;
  }
#elif PRIORITY == PRIORITY_ACTIVE
  bool operator()(const work_queue_entry* lhs, const work_queue_entry* rhs) {
    if ((lhs->rtype == CL_QRY || lhs->rtype == CL_QRY_O) && (rhs->rtype != CL_QRY && rhs->rtype != CL_QRY_O)) return true;
    if ((rhs->rtype == CL_QRY || rhs->rtype == CL_QRY_O) && (lhs->rtype != CL_QRY && lhs->rtype != CL_QRY_O)) return false;
    return lhs->starttime < rhs->starttime;
  }
#elif PRIORITY == PRIORITY_HOME
  bool operator()(const work_queue_entry* lhs, const work_queue_entry* rhs) {
    if (ISLOCAL(lhs->txn_id) && !ISLOCAL(rhs->txn_id)) return true;
    if (ISLOCAL(rhs->txn_id) && !ISLOCAL(lhs->txn_id)) return false;
    return lhs->starttime < rhs->starttime;
  }
#endif
};
typedef boost::circular_buffer<work_queue_entry*> WCircularBuffer;
class QWorkQueue {
public:
  void init();
  void statqueue(uint64_t thd_id, work_queue_entry * entry);
  void enqueue(uint64_t thd_id,Message * msg,bool busy);
  Message * dequeue(uint64_t thd_id);
  Message * queuetop(uint64_t thd_id);
  void sched_enqueue(uint64_t thd_id, Message * msg);
  Message * sched_dequeue(uint64_t thd_id);
  void sequencer_enqueue(uint64_t thd_id, Message * msg);
  Message * sequencer_dequeue(uint64_t thd_id);
#if LONG_TXN_SCHEDULE
  // 判断能不能取出事务的两个条件函数
  void insert_list_lockfree(uint64_t thd_id, 
									  TxnMsgLockList * list, 
									  Message * msg, TxnManager * txn);
  TxnManager * get_txn_from_list_lockfree(uint64_t thd_id, TxnMsgLockList * list, uint64_t &key);
  Message * get_msg_from_list_lockfree(uint64_t thd_id, TxnMsgLockList * list, uint64_t &key);
  // 用于Calvin的
  void insert_calvin_list_lockfree(uint64_t thd_id, TxnManager * txn);
  TxnManager * get_from_calvin_list_lockfree(uint64_t thd_id, uint64_t &key);
#endif
#if (LONG_TXN_SORT || LONG_TXN_SPLIT)
  void order_enqueue(uint64_t thd_id, Message * msg);
  Message * order_dequeue(uint64_t thd_id);
#endif

#if CC_ALG == ARIA
  Message * txn_dequeue(uint64_t thd_id);
  #if LONG_TXN_WORKLOAD && LONG_TXN_SCHEDULE
  // 在流水线模式下，可以随时从任何队列里取事务。
  void work_enqueue_lockfree_list(uint64_t thd_id, Message * msg, bool not_ready, ARIA_PHASE phase);
  Message * work_dequeue_lockfree_list(uint64_t thd_id, ARIA_PHASE phase);
  #else
  void work_enqueue(uint64_t thd_id, Message * msg, bool not_ready, ARIA_PHASE phase);
  Message * work_dequeue(uint64_t thd_id);
  #endif
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

#if CC_ALG == HDCC
  void calvin_enqueue(uint64_t thd_id, Message * msg, bool busy);
  Message * calvin_dequeue(uint64_t thd_id);
#endif

#if LONG_TXN_WORKLOAD && LONG_TXN_SCHEDULE
  bool sched_ready;
  TxnMsgLockList * calvin_scheduled_list_lockfree;

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
#endif

private:
  boost::lockfree::queue<work_queue_entry* > * work_queue;
  boost::lockfree::queue<work_queue_entry* > * new_txn_queue;
  boost::lockfree::queue<work_queue_entry* > * seq_queue;
  boost::lockfree::queue<work_queue_entry* > ** sched_queue;

#if CC_ALG == ARIA
  boost::lockfree::queue<work_queue_entry* > * aria_read_queue;
  boost::lockfree::queue<work_queue_entry* > * aria_reserve_queue;
  boost::lockfree::queue<work_queue_entry* > * aria_check_queue;
  boost::lockfree::queue<work_queue_entry* > * aria_commit_queue;
#endif

#if LONG_TXN_WORKLOAD && (LONG_TXN_SORT || LONG_TXN_SPLIT)
  boost::lockfree::queue<work_queue_entry* > * order_queue;
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

#if CC_ALG == HDCC
  boost::lockfree::queue<work_queue_entry* > * calvin_txn_queue;
  boost::lockfree::queue<work_queue_entry* > * calvin_work_queue;
  sem_t 	_calvin_semaphore;
  volatile uint64_t calvin_txn_queue_size;
  uint64_t calvin_txn_enqueue_size;
  uint64_t calvin_txn_dequeue_size;
  volatile uint64_t calvin_work_queue_size;
  uint64_t calvin_work_enqueue_size;
  uint64_t calvin_work_dequeue_size;
#endif

};


#endif

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

#include "worker_thread.h"

#include "abort_queue.h"
#include "global.h"
#include "helper.h"
#include "logger.h"
#include "manager.h"
#include "math.h"
#include "message.h"
#include "msg_queue.h"
#include "msg_thread.h"
#include "query.h"
#include "tpcc_query.h"
#include "txn.h"
#include "wl.h"
#include "work_queue.h"
#include "ycsb_query.h"
#include "small_lock_list.h"
#include "water_mark.h"
#include "ordered_list.h"
#include "caracal.h"
#include "caracal_sequencer.h"
#include "row.h"

void WorkerThread::setup() {
	if( get_thd_id() == 0) {
    send_init_done_to_all_nodes();
  }
  _thd_txn_id = 0;
}

void WorkerThread::statqueue(uint64_t thd_id, Message * msg, uint64_t starttime) {
  if (msg->rtype == RTXN_CONT ||
      msg->rtype == RQRY_RSP || msg->rtype == RACK_PREP  ||
      msg->rtype == RACK_FIN || msg->rtype == RTXN  ||
      msg->rtype == CL_RSP) {
    uint64_t queue_time = get_sys_clock() - starttime;
		INC_STATS(thd_id,trans_local_process,queue_time);
  } else if (msg->rtype == RQRY || msg->rtype == RQRY_CONT ||
             msg->rtype == RFIN || msg->rtype == RPREPARE ||
             msg->rtype == RFWD){
    uint64_t queue_time = get_sys_clock() - starttime;
		INC_STATS(thd_id,trans_remote_process,queue_time);
  } else if (msg->rtype == CL_QRY) {
    uint64_t queue_time = get_sys_clock() - starttime;
    INC_STATS(thd_id,trans_process_client,queue_time);
  }
}

RC WorkerThread::process(Message * msg) {
  RC rc __attribute__ ((unused));
  DEBUG_WRK("%ld Processing %ld,%ld %s\n",get_thd_id(),msg->get_batch_id(),msg->get_txn_id(),msg->get_message_name().c_str());
#if CC_ALG == ARIA
  assert(msg->get_rtype() == CL_QRY  || msg->get_rtype() == ARIA_ACK || msg->get_txn_id() != UINT64_MAX);
#else
  assert(msg->get_rtype() == CL_QRY  || msg->get_txn_id() != UINT64_MAX);
#endif
  uint64_t starttime = get_sys_clock();
		switch(msg->get_rtype()) {
			case RPREPARE:
        rc = process_rprepare(msg);
				break;
			case RFWD:
      #if CC_ALG == CALVIN || CC_ALG == SDPCC
        rc = process_rfwd(msg);
      #elif CC_ALG == CARACAL
        rc = process_caracal_rfwd(msg);
      #endif
				break;
			case RQRY:
        rc = process_rqry(msg);
				break;
			case RQRY_CONT:
        rc = process_rqry_cont(msg);
				break;
			case RQRY_RSP:
        rc = process_rqry_rsp(msg);
				break;
			case RFIN:
        rc = process_rfin(msg);
				break;
			case RACK_PREP:
        rc = process_rack_prep(msg);
				break;
			case RACK_FIN:
        rc = process_rack_rfin(msg);
				break;
			case RTXN_CONT:
        rc = process_rtxn_cont(msg);
				break;
      case CL_QRY:
			case RTXN:
#if CC_ALG == CALVIN || CC_ALG == SDPCC
        rc = process_calvin_rtxn(msg);
#elif CC_ALG == ARIA
        rc = process_aria_rtxn(msg);
#elif CC_ALG == CARACAL
        rc = process_caracal_rtxn(msg);
#else
        rc = process_rtxn(msg);
#endif
				break;
      case CARACAL_SUB_TXN:
      #if CC_ALG == CARACAL
        rc = process_caracal_rsubtxn(msg);
      #endif
        break;
			case LOG_FLUSHED:
        rc = process_log_flushed(msg);
				break;
			case LOG_MSG:
        rc = process_log_msg(msg);
				break;
			case LOG_MSG_RSP:
        rc = process_log_msg_rsp(msg);
				break;
			default:
        printf("Msg: %d\n",msg->get_rtype());
        fflush(stdout);
				assert(false);
				break;
		}
  statqueue(get_thd_id(), msg, starttime);
  uint64_t timespan = get_sys_clock() - starttime;
  INC_STATS(get_thd_id(),worker_process_cnt,1);
  INC_STATS(get_thd_id(),worker_process_time,timespan);
  INC_STATS(get_thd_id(),worker_process_cnt_by_type[msg->rtype],1);
  INC_STATS(get_thd_id(),worker_process_time_by_type[msg->rtype],timespan);
  DEBUG("%ld EndProcessing %s %ld\n",get_thd_id(),msg->get_message_name().c_str(),msg->get_txn_id());
  return rc;
}

void WorkerThread::check_if_done(RC rc) {
  if (txn_man->waiting_for_response()) return;
  if (rc == Commit) {
    txn_man->txn_stats.finish_start_time = get_sys_clock();
    commit();
  }
  if (rc == Abort) {
    txn_man->txn_stats.finish_start_time = get_sys_clock();
    abort();
  }
  // #if CC_ALG == SDOCC
  // if (rc == RETRY) {
  //   rc = WAIT;
  // }
  // #endif
}

void WorkerThread::release_txn_man() {
  #if CC_ALG == CARACAL && OPEN_SPLIT_ON_DEMAND
  txn_table.release_transaction_manager(get_thd_id(), txn_man->get_txn_id(),
                                        txn_man->get_batch_id(), txn_man->get_sub_txn_id());
  txn_man = NULL;                   
  #else
  txn_table.release_transaction_manager(get_thd_id(), txn_man->get_txn_id(),
                                        txn_man->get_batch_id());
  txn_man = NULL;
  #endif
}

void WorkerThread::calvin_wrapup() {
  txn_man->release_locks(RCOK);
  txn_man->commit_stats();
  DEBUG("(%ld,%ld) calvin ack to %ld\n", txn_man->get_txn_id(), txn_man->get_batch_id(),
        txn_man->return_id);
  if(txn_man->return_id == g_node_id) {
    work_queue.sequencer_enqueue(_thd_id,Message::create_message(txn_man,CALVIN_ACK));
  } else {
    msg_queue.enqueue(get_thd_id(), Message::create_message(txn_man, CALVIN_ACK),
                      txn_man->return_id);
  }
  release_txn_man();
}

void WorkerThread::calvin_abort() {
  
  // only txn_manager's abort is called, worker thread's abort function is neglected
  // since we replace it by sending aborted txn to sequencer directly
  txn_man->abort();
  if (txn_man->return_id == g_node_id) {
    work_queue.sequencer_enqueue(_thd_id, Message::create_message(txn_man, CALVIN_ABORT));
    // INC_STATS(get_thd_id(), deterministic_abort_cnt_calvin, 1);
  }
  release_txn_man();
}

// Can't use txn_man after this function
void WorkerThread::commit() {
  assert(txn_man);
  assert(IS_LOCAL(txn_man->get_txn_id()));

  uint64_t timespan = get_sys_clock() - txn_man->txn_stats.starttime;
  DEBUG_WRK("COMMIT %ld,%ld %f -- %f\n",  txn_man->get_batch_id(), txn_man->get_txn_id(),
        simulation->seconds_from_start(get_sys_clock()), (double)timespan / BILLION);

  // trans total time
  uint64_t end_time = get_sys_clock();
  uint64_t timespan_short  = end_time - txn_man->txn_stats.restart_starttime;
  uint64_t two_pc_timespan  = end_time - txn_man->txn_stats.prepare_start_time;
  uint64_t finish_timespan  = end_time - txn_man->txn_stats.finish_start_time;
  uint64_t prepare_timespan = txn_man->txn_stats.finish_start_time - txn_man->txn_stats.prepare_start_time;
	INC_STATS(get_thd_id(), trans_process_time, txn_man->txn_stats.trans_process_time);
  INC_STATS(get_thd_id(), trans_process_count, 1);

  INC_STATS(get_thd_id(), trans_prepare_time, prepare_timespan);
  INC_STATS(get_thd_id(), trans_prepare_count, 1);

  INC_STATS(get_thd_id(), trans_2pc_time, two_pc_timespan);
  INC_STATS(get_thd_id(), trans_finish_time, finish_timespan);
  INC_STATS(get_thd_id(), trans_commit_time, finish_timespan);
  INC_STATS(get_thd_id(), trans_total_run_time, timespan_short);

  INC_STATS(get_thd_id(), trans_2pc_count, 1);
  INC_STATS(get_thd_id(), trans_finish_count, 1);
  INC_STATS(get_thd_id(), trans_commit_count, 1);
  INC_STATS(get_thd_id(), trans_total_count, 1);

  INC_STATS(get_thd_id(), sdocc_total_txn_cnt, 1);
  #if CC_ALG == SDOCC 
  INC_STATS(get_thd_id(), trans_abort_count, ((ClientQueryMessage*)txn_man->last_msg)->retry_cnt);
  uint64_t retry_cnt = ((ClientQueryMessage*)txn_man->last_msg)->retry_cnt;
  INC_STATS(get_thd_id(), sdocc_total_retry_cnt, retry_cnt);
  if (retry_cnt >= 0 && retry_cnt < 99) {
    INC_STATS(get_thd_id(), sdocc_retry_cnt[retry_cnt], 1);
  } else if (retry_cnt >= 99) {
    INC_STATS(get_thd_id(), sdocc_retry_cnt[99], 1);
  } else {
    assert(false);
  }
  #endif

  // Send result back to client
#if CC_ALG == ARIA
#else
  // work_queue.sequencer_enqueue(_thd_id,Message::create_message(txn_man,PIP_ACK));
  DEBUG_WRK("ACK to %ld for (%ld,%ld) to client %ld\n", get_thd_id(), txn_man->get_batch_id(), txn_man->get_txn_id(), txn_man->client_id);
  msg_queue.enqueue(get_thd_id(),Message::create_message(txn_man,CL_RSP),txn_man->client_id);
#endif
  // #if CC_ALG != SDOCC  && CC_ALG != SILO
  release_txn_man();
  // #endif
  
  // Do not use txn_man after this
}

void WorkerThread::abort() {
  DEBUG("ABORT %ld -- %f\n", txn_man->get_txn_id(),
        (double)get_sys_clock() - run_starttime / BILLION);
  // TODO: TPCC Rollback here

  ++txn_man->abort_cnt;
  txn_man->reset();

  uint64_t end_time = get_sys_clock();
  uint64_t timespan_short  = end_time - txn_man->txn_stats.restart_starttime;
  uint64_t two_pc_timespan  = end_time - txn_man->txn_stats.prepare_start_time;
  uint64_t finish_timespan  = end_time - txn_man->txn_stats.finish_start_time;
  uint64_t prepare_timespan = txn_man->txn_stats.finish_start_time - txn_man->txn_stats.prepare_start_time;
	INC_STATS(get_thd_id(), trans_process_time, txn_man->txn_stats.trans_process_time);
  INC_STATS(get_thd_id(), trans_process_count, 1);

  INC_STATS(get_thd_id(), trans_prepare_time, prepare_timespan);
  INC_STATS(get_thd_id(), trans_prepare_count, 1);

  INC_STATS(get_thd_id(), trans_2pc_time, two_pc_timespan);
  INC_STATS(get_thd_id(), trans_finish_time, finish_timespan);
  INC_STATS(get_thd_id(), trans_abort_time, finish_timespan);
  INC_STATS(get_thd_id(), trans_total_run_time, timespan_short);

  INC_STATS(get_thd_id(), trans_2pc_count, 1);
  INC_STATS(get_thd_id(), trans_finish_count, 1);
  INC_STATS(get_thd_id(), trans_abort_count, 1);
  INC_STATS(get_thd_id(), trans_total_count, 1);
  
  #if CC_ALG != ARIA
  uint64_t penalty =
      abort_queue.enqueue(get_thd_id(), txn_man->get_txn_id(), txn_man->get_batch_id(), txn_man->get_abort_cnt());
  txn_man->txn_stats.total_abort_time += penalty;
  #endif
}

TxnManager * WorkerThread::get_transaction_manager(Message * msg) {
#if CC_ALG == CARACAL && OPEN_SPLIT_ON_DEMAND
  TxnManager* local_txn_man = txn_table.get_transaction_manager(get_thd_id(), msg->get_txn_id(), msg->get_batch_id(),msg->get_sub_txn_id());
#elif CC_ALG == CALVIN || CC_ALG == ARIA || CC_ALG == SDOCC || CC_ALG == SDPCC || CC_ALG == CARACAL // || CC_ALG == SILO
  TxnManager* local_txn_man = txn_table.get_transaction_manager(get_thd_id(), msg->get_txn_id(), msg->get_batch_id());
#else
  TxnManager * local_txn_man = txn_table.get_transaction_manager(get_thd_id(),msg->get_txn_id(),0);
#endif
  return local_txn_man;
}

char type2char(DATxnType txn_type)
{
  switch (txn_type)
  {
    case DA_READ:
      return 'R';
    case DA_WRITE:
      return 'W';
    case DA_COMMIT:
      return 'C';
    case DA_ABORT:
      return 'A';
    case DA_SCAN:
      return 'S';
    default:
      return 'U';
  }
}

#if CC_ALG == SDPCC
RC WorkerThread::run() {
  tsetup();
  printf("Running WorkerThread %ld\n",_thd_id);

  uint64_t ready_starttime;
  uint64_t idle_starttime = 0;
  
	while(!simulation->is_done()) {
    txn_man = NULL;
    heartbeat();
    enum class message_original {no_msg, work_queue, LockfreeQueue};
    progress_stats();
    Message* msg = NULL;
    uint64_t key = 0;
    message_original msg_orig = message_original::no_msg;
    // tmd，应该先拿远程操作。。
    {
      msg = work_queue.dequeue(get_thd_id());
      if (msg) {
        msg_orig = message_original::work_queue;
        txn_man = get_transaction_manager(msg);
      }
    }
    // 如果没有msg，再去拿本地的事务
    // if (!msg) {
    //   txn_man = work_queue.get_from_sdpcc_list_lockfree(_thd_id, key);
    //   if (txn_man) {
    //     msg_orig = message_original::LockfreeQueue;
    //     msg = txn_man->last_msg;
    //   }
    // }
    if (txn_man == NULL) {
      if (idle_starttime == 0) idle_starttime = get_sys_clock();
        continue;
    }
    // 拿到了
    simulation->last_da_query_time = get_sys_clock();
    if(idle_starttime > 0) {
      INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
      idle_starttime = 0;
    }
    assert(msg);
    txn_man->txn_stats.clear_short();
    txn_man->txn_stats.work_queue_cnt += 1;

    ready_starttime = get_sys_clock();
    bool ready = txn_man->unset_ready();
    INC_STATS(get_thd_id(),worker_activate_txn_time,get_sys_clock() - ready_starttime);
    if(!ready) {
      // Return to work queue, end processing
      work_queue.enqueue(get_thd_id(),msg,true);
      continue;
    }
    txn_man->register_thread(this);

    process(msg);

    ready_starttime = get_sys_clock();
    if(txn_man) {
      bool ready = txn_man->set_ready();
      assert(ready);
    }
    INC_STATS(get_thd_id(),worker_deactivate_txn_time,get_sys_clock() - ready_starttime);
  }
  printf("FINISH %ld:%ld\n",_node_id,_thd_id);
  fflush(stdout);
  return FINISH;
}
#elif CC_ALG == CARACAL
RC WorkerThread::split_txn(uint64_t thd_id, Message* msg, const RowSetMap &hot_row) {
  RC rc = RCOK;
  // !暂时只支持YCSB
  // 1. 首先是准备工作，拿到原事务的信息
  assert(WORKLOAD == YCSB);
  assert(msg->get_rtype() == CL_QRY);
  YCSBClientQueryMessage* qry_msg = (YCSBClientQueryMessage*)msg;
  // 2. 先确定有几个热点线程
  uint64_t hot_thread_cnt = ceil(HOT_THREAD_PERCENT * g_thread_cnt);
  // 现在用一套简单的实现，用一套事务管理器，那么下面只需要标记，事务需要发送给哪几个热点线程即可。
  bool assigned_hot_thread[hot_thread_cnt];
  memset(assigned_hot_thread, false, sizeof(assigned_hot_thread));

  qry_msg->involved_thread.clear(); // 记录这个事务涉及到哪些线程，最后需要把这个信息发送给
  for (auto row_pair : hot_row) {
    row_t* row = row_pair.second;
    // 这里需要找出来事务访问了哪些热数据项
    uint64_t key = row->get_primary_key();
    // 划分给哪个线程处理可以根据key进行hash，保证同一个数据项总是被同一个线程处理，减少冲突和重试的开销
    uint64_t assigned_thread_id = key % hot_thread_cnt;
    // for (auto req : qry_msg->requests) {
    for (int i = 0; i < qry_msg->requests.size(); i++) {
      auto req = qry_msg->requests[i];
      if (req->key == key &&
        assigned_thread_id != get_thd_id()) { // 这个线程负责的部分不需要拆分
        assigned_hot_thread[assigned_thread_id] = true;
        DEBUG_WRK("Worker %ld find hot %s to row %ld in req %d for txn %ld,%ld, assign to hot thread %ld\n", get_thd_id(),  req->acctype == WR ? "WR" : "RD", key, i, msg->get_batch_id(), msg->get_txn_id(), assigned_thread_id);
      }
    }
  }

  for (int i = 0; i < hot_thread_cnt; i++) {
    if (assigned_hot_thread[i]) {
      uint64_t assigned_thread_id = i;
      // 这个事务需要被分配给assigned_thread_id这个线程来处理，记录一下
      qry_msg->involved_thread.push_back(assigned_thread_id);
      // DEBUG_WRK("Worker %ld assign hot thread %ld for txn %ld,%ld\n", get_thd_id(), assigned_thread_id, msg->get_batch_id(), msg->get_txn_id());
    }
  }

  // 此时已经构造出来每个子事务需要访问的热数据项了，接下来是创建子事务并将这些热数据项分配给子事务
  uint64_t sub_txn_id = 1;
  qry_msg->caracal_commit_rsp_cnt = qry_msg->involved_thread.size() + 1;
  for (auto i : qry_msg->involved_thread) {
    assert(assigned_hot_thread[i]);
    assert(i != get_thd_id()); // 这个线程负责的部分不需要拆分
    YCSBClientQueryMessage* sub_qry_msg = (YCSBClientQueryMessage*) Message::create_message(CARACAL_SUB_TXN);
    sub_qry_msg->copy_from_msg(qry_msg);
    sub_qry_msg->sub_txn_id = sub_txn_id ++ ;
    sub_qry_msg->involved_thread.clear();
    sub_qry_msg->rtype = CARACAL_SUB_TXN;
    // 这里需要设置期望的响应计数指针，这个计数器是用来判断子事务的执行结果的，只有当所有子事务都执行完成后，主事务才能继续往下走
    sub_qry_msg->caracal_expected_rsp_ptr = &qry_msg->caracal_expected_rsp_cnt;
    sub_qry_msg->caracal_commit_rsp_ptr = &qry_msg->caracal_commit_rsp_cnt;
    // involved_thread这个拷贝一下
    for (auto thread_id : qry_msg->involved_thread) {
      sub_qry_msg->involved_thread.push_back(thread_id);
    }
    sub_qry_msg->requests.clear();
    // 这里需要把这个子事务需要访问的热数据项放到requests里
    for (auto row_pair : hot_row) {
      row_t* row = row_pair.second;
      uint64_t key = row->get_primary_key();
      uint64_t assigned_thread_id = key % hot_thread_cnt;
      if (assigned_thread_id == i) {
        // 这个数据项需要被分配给assigned_thread_id这个线程来处理，需要放到对应的子事务里
        for (int j = 0; j < qry_msg->requests.size(); j++) {
          auto req = qry_msg->requests[j];
          if (req->key == key) {
            sub_qry_msg->requests.add(req);
            DEBUG_WRK("Worker %ld assign req %d to hot thread %ld for txn %ld,%ld\n", get_thd_id(), j, assigned_thread_id, msg->get_batch_id(), msg->get_txn_id());
          }
        }
      }
    }
    assert(sub_qry_msg->requests.size() > 0); // 每个子事务至少要有一个请求
    assert(sub_qry_msg->txn_id == qry_msg->txn_id);
    assert(sub_qry_msg->rtype == CARACAL_SUB_TXN);
    work_queue.work_enqueue(i, sub_qry_msg, false, CARACAL_EXECUTION);
    DEBUG_WRK("Worker %ld enqueue sub txn for hot thread %ld for txn %ld,%ld\n", get_thd_id(), i, msg->get_batch_id(), msg->get_txn_id());
  }
  DEBUG_WRK("Worker %ld split txn (%ld,%ld) into %ld sub txns for hot threads\n", get_thd_id(), qry_msg->batch_id, qry_msg->txn_id, qry_msg->involved_thread.size());
  return rc;
}

RC WorkerThread::batch_append_and_split_on_demand() {
  // !1. batch_append部分
  // printf("Worker %ld try to perform batch append\n", get_thd_id());
  DEBUG_SCH("Worker %ld try to perform batch append\n", get_thd_id());
  for (auto row : caracal_man.get_thread_content(get_thd_id())->tmp_row_list) {
    row->batch_append(get_thd_id());
    DEBUG_WRK("Worker %ld batch append reservation for row %ld\n", get_thd_id(), row->get_primary_key());
  }
  // !2. split_on_demand部分
  if (OPEN_SPLIT_ON_DEMAND) {
    if (simulation->current_batch_id == 1) {
      for (auto row : caracal_man.get_thread_content(get_thd_id())->access_row_list) {
        if (row->get_version_cnt() > HOT_ITEM_THRESHOLD) {
          // 接下来是拆分事务的逻辑，只要访问了这些热数据项的事务都要被拆分成两个，交给第一个或者第二个线程来执行
          caracal_man.get_thread_content(get_thd_id())->hot_row_list.insert(std::pair<uint64_t, row_t*>(row->get_primary_key(), row));
          DEBUG_WRK("Worker %ld find hot row %ld with version cnt %ld\n", get_thd_id(), row->get_primary_key(), row->get_version_cnt());
        }
      }
    }
    for (auto msg : caracal_man.get_thread_content(get_thd_id())->msg_list) {
      split_txn(get_thd_id(), msg, caracal_man.get_thread_content(get_thd_id())->hot_row_list);
    }
  }
  caracal_man.get_thread_content(get_thd_id())->msg_list.clear();
  return RCOK;
}

RC WorkerThread::run() {
  tsetup();
  printf("Running WorkerThread %ld\n",_thd_id);

  uint64_t ready_starttime;
  uint64_t idle_starttime = 0;
  uint64_t bool_phase_end = false;
  
	while(!simulation->is_done()) {
    txn_man = NULL;
    heartbeat();
    progress_stats();

    if (simulation->caracal_phase == CARACAL_APPEND &&
        !caracal_man.is_phase_done(get_thd_id())) {
      caracal_man.set_phase_done(get_thd_id());
      batch_append_and_split_on_demand();
      
      caracal_man.get_thread_content(get_thd_id())->tmp_row_list.clear();
      simulation->finish_append_cnt.fetch_add(1);
      // ATOM_ADD_FETCH(simulation->finish_append_cnt, 1);
    }

    if (simulation->caracal_phase == CARACAL_EXECUTION ||
        simulation->caracal_phase == CARACAL_EXECUTION_SYNC ||
        simulation->caracal_phase == CARACAL_APPEND_SYNC) {
      assert(caracal_man.get_thread_content(get_thd_id())->tmp_row_list.empty());
    }
    
    Message* msg = NULL;
    uint64_t key = 0;
    {
      uint64_t dequeue_starttime = get_sys_clock();
      msg = work_queue.work_dequeue(get_thd_id());
      if(!msg) {
        if (idle_starttime == 0) idle_starttime = get_sys_clock();
        uint64_t dequeue_endtime = get_sys_clock();
        INC_STATS(get_thd_id(),workqueue_dequeue_time,dequeue_endtime - dequeue_starttime);
        // dequeue_starttime = dequeue_endtime;
        continue;
      }
      // if (msg) {
      //   txn_man = get_transaction_manager(msg);
      // }
    }

    // 拿到了
    simulation->last_da_query_time = get_sys_clock();
    if(idle_starttime > 0) {
      INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
      idle_starttime = 0;
    }

    if (msg->rtype == CL_QRY || 
        msg->rtype == CARACAL_SUB_TXN) {
      txn_man = get_transaction_manager(msg);

      // if (txn_man->caracal_phase != simulation->caracal_phase) {
      //   // printf("thd: %ld, txn: %ld runs twice\n", get_thd_id(), txn_man->get_txn_id());
      //   work_queue.work_enqueue(get_thd_id(), msg, false, txn_man->caracal_phase);
      //   continue;
      // }
      if (msg->rtype == CARACAL_SUB_TXN) {
        txn_man->caracal_phase = CARACAL_EXECUTION;
      }

      txn_man->txn_stats.clear_short();
      txn_man->txn_stats.msg_queue_time += msg->mq_time;
      txn_man->txn_stats.msg_queue_time_short += msg->mq_time;
      msg->mq_time = 0;
      txn_man->txn_stats.work_queue_time += msg->wq_time;
      txn_man->txn_stats.work_queue_time_short += msg->wq_time;
      //txn_man->txn_stats.network_time += msg->ntwk_time;
      msg->wq_time = 0;
      txn_man->txn_stats.work_queue_cnt += 1;

      ready_starttime = get_sys_clock();
      bool ready = txn_man->unset_ready();
      INC_STATS(get_thd_id(),worker_activate_txn_time,get_sys_clock() - ready_starttime);
      if(!ready) {
        DEBUG_WRK("Thd %ld txn %ld in phase %d re-enqueue to list because not ready\n",
          get_thd_id(), txn_man->get_txn_id(), txn_man->caracal_phase);
        work_queue.work_enqueue(get_thd_id(),msg,true,txn_man->caracal_phase);
        continue;
      }
      
      txn_man->register_thread(this);
    }

    RC rc = process(msg);
    
    // 这里其实是说跑完了，放进下一个阶段
    if (rc == RCOK && (msg->rtype == CL_QRY)) {
    // if (rc == RCOK && (msg->rtype == CL_QRY || msg->rtype == RFWD)) {
      // !这个阶段我得再想想，用不用合并到其他阶段里
      if (txn_man != nullptr && simulation->caracal_phase <= CARACAL_INIT_SYNC &&
        txn_man->caracal_phase == CARACAL_EXECUTION) {
        // assert(txn_man->get_txn_id() != UINT64_MAX);
        work_queue.work_enqueue(get_thd_id(), msg, false, txn_man->caracal_phase);
        DEBUG_WRK("Thd %ld txn %ld,%ld in phase %d enqueue to list for next phase %d\n",
          get_thd_id(), txn_man->get_batch_id(), txn_man->get_txn_id(), simulation->caracal_phase, txn_man->caracal_phase);
      }
      // // !这里得事务做完才能batch_process_count++;
      // if ((IS_LOCAL(msg->get_txn_id()) && simulation->caracal_phase <= CARACAL_INIT_SYNC)) {
      //   assert(simulation->current_batch_id == msg->get_batch_id());
      //   ATOM_ADD_FETCH(simulation->batch_process_count, 1);
      //   ATOM_ADD_FETCH(simulation->batch_local_process_count, 1);
      //   DEBUG_WRK("Worker %ld finished processing local txn %ld,%ld phase %d, now batch_process_count %ld/%ld, batch_local_process_count %ld, batch_remote_process_count %ld, batch_remote_send_count %ld\n", get_thd_id(), msg->get_batch_id(), msg->get_txn_id(), simulation->caracal_phase, simulation->batch_process_count, caracal_seq.get_total_ack_count(), simulation->batch_local_process_count, simulation->batch_remote_process_count, simulation->batch_remote_send_count);
      // }
    }

    ready_starttime = get_sys_clock();
    if(txn_man) {
      bool ready = txn_man->set_ready();
      assert(ready);
    }
    INC_STATS(get_thd_id(),worker_deactivate_txn_time,get_sys_clock() - ready_starttime);
  }
  printf("FINISH %ld:%ld\n",_node_id,_thd_id);
  fflush(stdout);
  return FINISH;
}
#else
RC WorkerThread::run() {
  tsetup();
  printf("Running WorkerThread %ld\n",_thd_id);

  uint64_t ready_starttime;
  uint64_t idle_starttime = 0;

  #if CC_ALG == SDOCC
  // uint64_t current_minSid = 0;
  uint64_t old_minSid = 0;
  #endif

	while(!simulation->is_done()) {
    txn_man = NULL;
    heartbeat();

    #if CC_ALG == SDOCC
    uint64_t current_minSid = check_water_mark->get_global_watermark();
    if (current_minSid != old_minSid) {
      handle_tmp_txn(current_minSid,old_minSid);
    }
    #endif
    progress_stats();
    Message* msg;
    uint64_t dequeue_starttime = get_sys_clock();
    #if CC_ALG == SDOCC// || CC_ALG == SILO
      msg = work_queue.sdocc_dequeue(get_thd_id());
    #elif CC_ALG == ARIA
      msg = work_queue.work_dequeue(get_thd_id());
    #else
      msg = work_queue.dequeue(get_thd_id());
    #endif
    if(!msg) {
      if (idle_starttime == 0) idle_starttime = get_sys_clock();
      uint64_t dequeue_endtime = get_sys_clock();
      INC_STATS(get_thd_id(),workqueue_dequeue_time,dequeue_endtime - dequeue_starttime);
      // dequeue_starttime = dequeue_endtime;
      //todo: add sleep 0.01ms
      continue;
    }
    simulation->last_da_query_time = get_sys_clock();
    if(idle_starttime > 0) {
      INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
      idle_starttime = 0;
    }
#if CC_ALG == ARIA
    if (msg->rtype == ARIA_ACK) {
      process_aria_ack(msg);
      continue;
    }
#endif
    //uint64_t starttime = get_sys_clock();
    int algo = CC_ALG;
    if((msg->rtype != CL_QRY) || algo == CALVIN) {
      txn_man = get_transaction_manager(msg);

      if (CC_ALG != CALVIN && IS_LOCAL(txn_man->get_txn_id())) {
        if (msg->rtype != RTXN_CONT &&
            ((msg->rtype != RACK_PREP) || (txn_man->get_rsp_cnt() == 1))) {
          txn_man->txn_stats.work_queue_time_short += msg->lat_work_queue_time;
          txn_man->txn_stats.cc_block_time_short += msg->lat_cc_block_time;
          txn_man->txn_stats.cc_time_short += msg->lat_cc_time;
          txn_man->txn_stats.msg_queue_time_short += msg->lat_msg_queue_time;
          txn_man->txn_stats.process_time_short += msg->lat_process_time;
          /*
          if (msg->lat_network_time/BILLION > 1.0) {
            printf("%ld %d %ld -> %ld: %f %f\n",msg->txn_id, msg->rtype,
          msg->return_node_id,get_node_id() ,msg->lat_network_time/BILLION,
          msg->lat_other_time/BILLION);
          }
          */
          txn_man->txn_stats.network_time_short += msg->lat_network_time;
        }

      } else {
          txn_man->txn_stats.clear_short();
      }
      if (CC_ALG != CALVIN) {
        txn_man->txn_stats.lat_network_time_start = msg->lat_network_time;
        txn_man->txn_stats.lat_other_time_start = msg->lat_other_time;
      }
      txn_man->txn_stats.msg_queue_time += msg->mq_time;
      txn_man->txn_stats.msg_queue_time_short += msg->mq_time;
      msg->mq_time = 0;
      txn_man->txn_stats.work_queue_time += msg->wq_time;
      txn_man->txn_stats.work_queue_time_short += msg->wq_time;
      //txn_man->txn_stats.network_time += msg->ntwk_time;
      msg->wq_time = 0;
      txn_man->txn_stats.work_queue_cnt += 1;


      ready_starttime = get_sys_clock();
      bool ready = txn_man->unset_ready();
      INC_STATS(get_thd_id(),worker_activate_txn_time,get_sys_clock() - ready_starttime);
      if(!ready) {
        // Return to work queue, end processing
        DEBUG("Thd %ld txn %ld,%ld unset ready failed, re-enqueue enqueue, msg type %s\n",
          get_thd_id(), txn_man->get_batch_id(),txn_man->get_txn_id(), msg->get_message_name().c_str());
        work_queue.enqueue(get_thd_id(),msg,true);
        continue;
      } else {
        DEBUG("Thd %ld txn %ld,%ld unset ready\n",
          get_thd_id(), txn_man->get_batch_id(),txn_man->get_txn_id());
      }
      txn_man->register_thread(this);
    }
#if CC_ALG == ARIA
    else if (msg->rtype == CL_QRY) {
      txn_man = get_transaction_manager(msg);
      if (txn_man->aria_phase != simulation->aria_phase) {
        // printf("thd: %ld, txn: %ld runs twice\n", get_thd_id(), txn_man->get_txn_id());
        work_queue.work_enqueue(get_thd_id(), msg, false, txn_man->aria_phase);
        continue;
      }

      txn_man->txn_stats.clear_short();
      txn_man->txn_stats.msg_queue_time += msg->mq_time;
      txn_man->txn_stats.msg_queue_time_short += msg->mq_time;
      msg->mq_time = 0;
      txn_man->txn_stats.work_queue_time += msg->wq_time;
      txn_man->txn_stats.work_queue_time_short += msg->wq_time;
      //txn_man->txn_stats.network_time += msg->ntwk_time;
      msg->wq_time = 0;
      txn_man->txn_stats.work_queue_cnt += 1;

      if (txn_man->participants_cnt != 0) {
        DEBUG_WRK("Thd %ld txn %ld in phase %d re-enqueue to list because participants_cnt %ld\n",
          get_thd_id(), txn_man->get_txn_id(), txn_man->aria_phase,txn_man->participants_cnt);
        work_queue.work_enqueue(get_thd_id(), msg, true, txn_man->aria_phase);
        continue;
      }

      ready_starttime = get_sys_clock();
      bool ready = txn_man->unset_ready();
      INC_STATS(get_thd_id(),worker_activate_txn_time,get_sys_clock() - ready_starttime);
      if(!ready) {
        DEBUG_WRK("Thd %ld txn %ld in phase %d re-enqueue to list because not ready\n",
          get_thd_id(), txn_man->get_txn_id(), txn_man->aria_phase);
        work_queue.work_enqueue(get_thd_id(),msg,true,txn_man->aria_phase);
        continue;
      }
      
      txn_man->register_thread(this);
    }
#endif
    RC rc = process(msg);

#if CC_ALG == ARIA  
    if (msg->rtype == CL_QRY) {// && txn_man) {
      if (simulation->aria_phase != ARIA_COMMIT) {
         work_queue.work_enqueue(get_thd_id(), msg, false, txn_man->aria_phase);
      }

      // if we processed all local transactions in this phase, go to next phase and reset the count
      if (ATOM_ADD_FETCH(simulation->batch_process_count, 1) == g_aria_batch_size) {
        bool isAriaCommit = simulation->aria_phase == ARIA_COMMIT;
        bool success = ATOM_CAS(simulation->batch_process_count, g_aria_batch_size, 0);
        DEBUG_SCH("Worker %ld finished aria batch %ld phase %d, batch_process_count %ld, success: %d\n", get_thd_id(), simulation->current_batch_id, simulation->aria_phase, simulation->batch_process_count, success);
        assert(success);
        if (!isAriaCommit) {
          // 如果不是Commit阶段，而且是跨节点事务，且是ARIA_RESERVATION和ARIA_CHECK阶段，就走两阶段，和其他所有节点发送ACK
          if (g_mpr != 0 && (simulation->aria_phase == ARIA_RESERVATION || simulation->aria_phase == ARIA_CHECK)) {
            for (uint64_t i = 0; i < g_node_cnt; i++) {
              if (i == g_node_id) continue;
              Message* msg = Message::create_message(ARIA_ACK); 
              ((AckMessage*)msg)->batch_id = simulation->current_batch_id;
              ((AckMessage*)msg)->aria_phase = simulation->aria_phase;
              msg_queue.enqueue(_thd_id, msg, i);
              DEBUG_SCH("Worker %ld sent ARIA_ACK to node %ld for batch %ld phase %d\n", get_thd_id(), i, simulation->current_batch_id, simulation->aria_phase);
            }
            uint64_t index = 0;
            if (simulation->aria_phase == ARIA_RESERVATION) {
              index = 0;
            } else if (simulation->aria_phase == ARIA_CHECK) {
              index = 1;
            } else {
              assert(false);
            }
            while (simulation->aria_barrier[index].barrier_count != g_node_cnt - 1 && !simulation->is_done()) {}
            // while (simulation->barrier_count != g_node_cnt - 1 && !simulation->is_done()) {}
            // simulation->barrier_count = 0;
            // memset(simulation->barriers, 0, sizeof(uint64_t) * g_node_cnt);
            simulation->next_aria_phase();
          } else {
            simulation->next_aria_phase();
          }
        }
      }
    }
#endif
    ready_starttime = get_sys_clock();
    if(txn_man) {
      bool ready = txn_man->set_ready();
      assert(ready);
      DEBUG("Thd %ld txn %ld,%ld set ready\n", get_thd_id(), txn_man->get_batch_id(),txn_man->get_txn_id());
    }
    INC_STATS(get_thd_id(),worker_deactivate_txn_time,get_sys_clock() - ready_starttime);
    #if CC_ALG == SDOCC
    // 如果需要重试
    if (rc == RETRY && IS_LOCAL(txn_man->get_txn_id())) {
      bool expected_value = false; 
      assert(txn_man->last_msg);
      if (((ClientQueryMessage*)txn_man->last_msg)->has_re_enqueued.compare_exchange_strong(expected_value, 1)) {
        DEBUG_SCH("Thd %ld txn %ld,%ld needs retry, re-enqueue to list, retry_cnt: %ld\n", get_thd_id(), txn_man->get_batch_id(), txn_man->get_txn_id(), ((ClientQueryMessage*)txn_man->last_msg)->retry_cnt);
        ((ClientQueryMessage*)txn_man->last_msg)->retry_cnt++;
        txn_man->retry_cnt = ((ClientQueryMessage*)txn_man->last_msg)->retry_cnt;
        txn_man->enter_tmp_queue_time = get_server_clock();
        tmp_txn_list.insert(txn_man);
        tmp_txn_list_size++;
        // tmp_txn_list.push_back(txn_man);
      }
    }
    #endif
    // delete message
    ready_starttime = get_sys_clock();
    #if CC_ALG == ARIA || CC_ALG == SDOCC //|| CC_ALG == SILO
      if (msg->rtype != CL_QRY) {
        msg->release();
        delete msg;
      }
    #elif CC_ALG != CALVIN
      msg->release();
      delete msg;
    #endif
    INC_STATS(get_thd_id(),worker_release_msg_time,get_sys_clock() - ready_starttime);

	}
  printf("FINISH %ld:%ld\n",_node_id,_thd_id);
  fflush(stdout);
  return FINISH;
}

#endif

RC WorkerThread::process_rfin(Message * msg) {
  DEBUG("RFIN %ld\n",msg->get_txn_id());
  assert(CC_ALG != CALVIN && CC_ALG != SDPCC && CC_ALG != CARACAL);

  M_ASSERT_V(!IS_LOCAL(msg->get_txn_id()), "RFIN local: %ld %ld/%d\n", msg->get_txn_id(),
             msg->get_txn_id() % g_node_cnt, g_node_id);
#if CC_ALG == SILO
  txn_man->set_commit_timestamp(((FinishMessage*)msg)->commit_timestamp);
#endif

  if(((FinishMessage*)msg)->rc == Abort) {
    txn_man->abort();
    txn_man->reset();
    txn_man->reset_query();
    msg_queue.enqueue(get_thd_id(), Message::create_message(txn_man, RACK_FIN),
                      GET_NODE_ID(msg->get_txn_id()));
    return Abort;
  }
  #if CC_ALG == SDOCC
  txn_man->sdocc_phase = SDOCC_COMMIT;
  #endif
  txn_man->commit();
  //if(!txn_man->query->readonly() || CC_ALG == OCC)
  if (!((FinishMessage*)msg)->readonly || CC_ALG == SILO || CC_ALG == ARIA || CC_ALG == SDOCC) {
    msg_queue.enqueue(get_thd_id(), Message::create_message(txn_man, RACK_FIN),
                      GET_NODE_ID(msg->get_txn_id()));
  }
  release_txn_man();

  return RCOK;
}


#if CC_ALG != ARIA
RC WorkerThread::process_rack_prep(Message * msg) {
  DEBUG("RACK_PREP %ld\n",msg->get_txn_id());

  RC rc = RCOK;

  int responses_left = txn_man->received_response(((AckMessage*)msg)->rc);
  assert(responses_left >=0);
#if CC_ALG == SILO
  uint64_t max_tid = ((AckMessage*)msg)->max_tid;
  txn_man->find_tid_silo(max_tid);
#endif
  if (responses_left > 0) return WAIT;
  #if CC_ALG == SDOCC
  if (txn_man->retry_cnt != ((AckMessage*)msg)->retry_cnt) {
    // 说明这个ACK是之前重试的消息发出的，已经过时了，直接丢弃
    DEBUG("Thd %ld txn %ld,%ld received outdated RACK_PREP, retry_cnt in msg: %ld, current retry_cnt: %ld\n", get_thd_id(), txn_man->get_batch_id(), txn_man->get_txn_id(), ((AckMessage*)msg)->retry_cnt, txn_man->retry_cnt);
    return WAIT;
  }
  #endif
  INC_STATS(get_thd_id(), trans_validation_network, get_sys_clock() - txn_man->txn_stats.trans_validate_network_start_time);
  INC_STATS(get_thd_id(), remote_round_cnt, 1);
  INC_STATS(get_thd_id(), remote_validate_cnt, 1);
  // Done waiting
  #if CC_ALG == SDOCC
  // 对于SDOCC来说，即使远程因为水印需要retry，本地也得先验证一次
    rc = txn_man->validate();
  #else
  if(txn_man->get_rc() == RCOK) {
      rc = txn_man->validate();
  }
  #endif

  #if CC_ALG == SDOCC
  bool watermark_passed = false;
  uint64_t key = 0;
  if(rc == RCOK) {
    // ! 检查水印
    key = get_batch_key(txn_man->get_batch_id(), txn_man->return_id, txn_man->get_txn_id());
    watermark_passed = key <= check_water_mark->get_global_watermark();
    // watermark_passed 
  }
  if(!watermark_passed || rc == RETRY || txn_man->get_rc() == RETRY) {
    // !事务重新入队
    // assert(false);
    rc = RETRY;
  } else {
    assert(rc == RCOK);
    // 可以提交了
    txn_man->start_sdocc_commit();
  }
  return rc;
  #else
  uint64_t finish_start_time = get_sys_clock();
  txn_man->txn_stats.finish_start_time = finish_start_time;
  // uint64_t prepare_timespan  = finish_start_time - txn_man->txn_stats.prepare_start_time;
  // INC_STATS(get_thd_id(), trans_prepare_time, prepare_timespan);
  // INC_STATS(get_thd_id(), trans_prepare_count, 1);
  if(rc == Abort || txn_man->get_rc() == Abort) {
    txn_man->txn->rc = Abort;
    rc = Abort;
  }
  if (rc == Abort || txn_man->get_rc() == Abort) txn_man->txn_stats.trans_abort_network_start_time = get_sys_clock();
  else txn_man->txn_stats.trans_commit_network_start_time = get_sys_clock();
  txn_man->send_finish_messages();
  if(rc == Abort) {
    txn_man->abort();
  } else {
    txn_man->commit();
  }
  return rc;
  #endif
}
#else
RC WorkerThread::process_rack_prep(Message * msg) {
  DEBUG("RPREP_ACK %ld\n",msg->get_txn_id());

  RC rc = RCOK;
  txn_man->participants_cnt--;
  assert(txn_man->participants_cnt >= 0);
  
  // If the transaction is already abort, just return
  if (txn_man->txn->rc == Abort) {
    return Abort;
  }

  AckMessage * ack = (AckMessage *)msg;
  if (ack->raw == true) {
    txn_man->raw = true;
  }
  if (ack->war == true) {
    txn_man->war = true;
  }

  if (ack->rc == Abort || (txn_man->raw && txn_man->war)) {
    txn_man->txn->rc = Abort;
    rc = Abort;
  }
  return rc;
}
#endif

RC WorkerThread::process_rack_rfin(Message * msg) {
  DEBUG("RFIN_ACK %ld\n",msg->get_txn_id());

  RC rc = RCOK;

  int responses_left = txn_man->received_response(((AckMessage*)msg)->rc);
  assert(responses_left >=0);
  if (responses_left > 0) return WAIT;

  // Done waiting
  txn_man->txn_stats.twopc_time += get_sys_clock() - txn_man->txn_stats.wait_starttime;
  INC_STATS(get_thd_id(), remote_round_cnt, 1);
  INC_STATS(get_thd_id(), remote_commit_cnt, 1);

#if CC_ALG == ARIA
  Message * message = Message::create_message(txn_man, ARIA_ACK);
#endif
  if(txn_man->get_rc() == RCOK) {
    INC_STATS(get_thd_id(), trans_commit_network, get_sys_clock() - txn_man->txn_stats.trans_commit_network_start_time);
    //txn_man->commit();
#if LOGGING
    if (txn_man->log_flushed) {
      commit();
    }
#else
    commit();
#endif
  } else {
    INC_STATS(get_thd_id(), trans_abort_network, get_sys_clock() - txn_man->txn_stats.trans_abort_network_start_time);
    //txn_man->abort();
    abort();
  }
#if CC_ALG == ARIA
  work_queue.sequencer_enqueue(get_thd_id(),message);
#endif
  return rc;
}

#if CC_ALG != ARIA
RC WorkerThread::process_rqry_rsp(Message * msg) {
  DEBUG("RQRY_RSP %ld\n",msg->get_txn_id());
  assert(IS_LOCAL(msg->get_txn_id()));
  INC_STATS(get_thd_id(), trans_process_network, get_sys_clock() - txn_man->txn_stats.trans_process_network_start_time);
  txn_man->txn_stats.remote_wait_time += get_sys_clock() - txn_man->txn_stats.wait_starttime;

  INC_STATS(get_thd_id(), remote_round_cnt, 1);
  INC_STATS(get_thd_id(), remote_execution_cnt, 1);

  if(((QueryResponseMessage*)msg)->rc == Abort) {
    txn_man->start_abort();
    return Abort;
  }
  txn_man->send_RQRY_RSP = false;
  RC rc = RCOK;
  #if CC_ALG == SDOCC
  #if RWSET_KNOWN
  if (txn_man->query->rwset_known) txn_man->sdocc_expected_rsp_cnt--;
  #endif
  rc = txn_man->run_sdocc_txn();
  // if (!txn_man->query->rwset_known) printf("txn %ld,%ld rwset unknown in RQRY_RSP\n", txn_man->get_batch_id(), txn_man->get_txn_id());
  #else
  rc = txn_man->run_txn();
  #endif
  
  check_if_done(rc);
  return rc;
}
#else
RC WorkerThread::process_rqry_rsp(Message * msg) {
  RC rc = RCOK;
  DEBUG("RQRY_RSP %ld\n",msg->get_txn_id());
  assert(IS_LOCAL(msg->get_txn_id()));
  if (txn_man->participants_cnt == 1) {
    INC_STATS(get_thd_id(), trans_process_network, get_sys_clock() - txn_man->txn_stats.trans_process_network_start_time);
  }
  txn_man->txn_stats.remote_wait_time += get_sys_clock() - txn_man->txn_stats.wait_starttime;

  QueryResponseMessage * resp = (QueryResponseMessage *)msg;
  if (resp->rc == Abort) {
    txn_man->txn->rc = Abort;
    rc = Abort;
  }

  txn_man->participants_cnt--;
  assert(txn_man->participants_cnt >= 0);
  return rc;
}
#endif

#if CC_ALG != ARIA
RC WorkerThread::process_rqry(Message * msg) {
  DEBUG("RQRY %ld\n",msg->get_txn_id());
  M_ASSERT_V(!IS_LOCAL(msg->get_txn_id()), "RQRY local: %ld %ld/%d\n", msg->get_txn_id(),
             msg->get_txn_id() % g_node_cnt, g_node_id);
  assert(!IS_LOCAL(msg->get_txn_id()));
  RC rc = RCOK;

  msg->copy_to_txn(txn_man);

  txn_man->send_RQRY_RSP = true;
  #if CC_ALG == SDOCC
  rc = txn_man->run_sdocc_txn();
  // if (!txn_man->query->rwset_known) printf("txn %ld,%ld rwset unknown in RQRY\n", txn_man->get_batch_id(), txn_man->get_txn_id());
  #else
  rc = txn_man->run_txn();
  #endif

  // Send response
  if(rc != WAIT) {
    msg_queue.enqueue(get_thd_id(),Message::create_message(txn_man,RQRY_RSP),txn_man->return_id);
  }
  return rc;
}
#else
RC WorkerThread::process_rqry(Message * msg) {
  DEBUG("RQRY %ld\n",msg->get_txn_id());
  assert(!IS_LOCAL(msg->get_txn_id()));
  RC rc = RCOK;
  msg->copy_to_txn(txn_man);
  QueryMessage * ycsb_query = (QueryMessage * ) msg;
  rc = txn_man->process_aria_remote(ycsb_query->aria_phase);
  msg_queue.enqueue(get_thd_id(),Message::create_message(txn_man,RQRY_RSP),txn_man->return_id);
  return rc;
}
#endif

RC WorkerThread::process_rqry_cont(Message * msg) {
  DEBUG("RQRY_CONT %ld\n",msg->get_txn_id());
  assert(!IS_LOCAL(msg->get_txn_id()));
  RC rc = RCOK;

  txn_man->run_txn_post_wait();
  txn_man->send_RQRY_RSP = false;
  #if CC_ALG == SDOCC
  rc = txn_man->run_sdocc_txn();
  // if (!txn_man->query->rwset_known) printf("txn %ld,%ld rwset unknown in RQRY_CONT\n", txn_man->get_batch_id(), txn_man->get_txn_id());
  #else
  rc = txn_man->run_txn();
  #endif

  // Send response
  if(rc != WAIT) {
    msg_queue.enqueue(get_thd_id(),Message::create_message(txn_man,RQRY_RSP),txn_man->return_id);
  }
  return rc;
}

RC WorkerThread::process_rtxn_cont(Message * msg) {
  DEBUG("RTXN_CONT %ld\n",msg->get_txn_id());
  assert(IS_LOCAL(msg->get_txn_id()));

  txn_man->txn_stats.local_wait_time += get_sys_clock() - txn_man->txn_stats.wait_starttime;

  txn_man->run_txn_post_wait();
  txn_man->send_RQRY_RSP = false;
  RC rc = RCOK;
  #if CC_ALG == SDOCC
  rc = txn_man->run_sdocc_txn();
  // if (!txn_man->query->rwset_known) printf("txn %ld,%ld rwset unknown in RTXN_CONT\n", txn_man->get_batch_id(), txn_man->get_txn_id());
  #else
  rc = txn_man->run_txn();
  #endif
  check_if_done(rc);
  return RCOK;
}


#if CC_ALG != ARIA
RC WorkerThread::process_rprepare(Message * msg) {
  DEBUG("RPREP %ld\n",msg->get_txn_id());
    RC rc = RCOK;
#if LOGGING && CC_ALG != CALVIN && CC_ALG != SDPCC
    LogRecord * record = logger.createRecord(msg->get_txn_id(),L_FLUSH,0,0);
    if(g_repl_cnt > 0) {
      msg_queue.enqueue(get_thd_id(), Message::create_message(record, LOG_MSG),
                        g_node_id + g_node_cnt + g_client_node_cnt);
    }
    logger.enqueueRecord(record);

#endif
    // Validate transaction
    #if CC_ALG == SDOCC
    if (txn_man->retry_cnt > ((PrepareMessage *)msg)->retry_cnt) {
      // 说明这个消息是之前重试的消息发出的，已经过时了，直接丢弃
      DEBUG("Thd %ld txn %ld,%ld received outdated RPREP, retry_cnt in msg: %ld, current retry_cnt: %ld\n", get_thd_id(), txn_man->get_batch_id(),txn_man->get_txn_id(), ((PrepareMessage *)msg)->retry_cnt,  txn_man->retry_cnt);
      return WAIT;
    } else {
      txn_man->retry_cnt = ((PrepareMessage *)msg)->retry_cnt;
    }
    txn_man->set_rc(RCOK);
    #endif
    rc  = txn_man->validate();
    txn_man->set_rc(rc);
    msg_queue.enqueue(get_thd_id(),Message::create_message(txn_man,RACK_PREP),msg->return_node_id);
    // Clean up as soon as abort is possible
    if(rc == Abort) {
      txn_man->abort();
    }

    return rc;
}
#else
RC WorkerThread::process_rprepare(Message* msg) {
  DEBUG("RPREP %ld\n",msg->get_txn_id());
  RC rc = RCOK;

  rc = txn_man->process_aria_remote(ARIA_CHECK);
  msg_queue.enqueue(get_thd_id(),Message::create_message(txn_man,RACK_PREP),msg->return_node_id);

  return rc;
}
#endif


uint64_t WorkerThread::get_next_txn_id() {
  uint64_t txn_id =
      (get_node_id() + get_thd_id() * g_node_cnt) + (g_thread_cnt * g_node_cnt * _thd_txn_id);
  ++_thd_txn_id;
  return txn_id;
}

RC WorkerThread::process_rtxn(Message * msg) {
  RC rc = RCOK;
  uint64_t txn_id = UINT64_MAX;
  // !
  #if CC_ALG == SDOCC// || CC_ALG == SILO
  uint64_t batch_id = msg->get_batch_id();
  #else
  uint64_t batch_id = 0;
  #endif
  if(msg->get_rtype() == CL_QRY ) {
    // This is a new transaction
    // Only set new txn_id when txn first starts
    // !
    #if CC_ALG == SDOCC// || CC_ALG == SILO
    txn_id = msg->txn_id;
    #else
    txn_id = get_next_txn_id();
    msg->txn_id = txn_id;
    #endif
    // Put txn in txn_table
    if (!txn_man)
    {
      txn_man = txn_table.get_transaction_manager(get_thd_id(),txn_id,batch_id);
      txn_man->register_thread(this);
    }
    uint64_t ready_starttime = get_sys_clock();
    bool ready = txn_man->unset_ready();
    INC_STATS(get_thd_id(),worker_activate_txn_time,get_sys_clock() - ready_starttime);
    assert(ready);
    DEBUG("Thd %ld txn %ld,%ld unset ready\n",
          get_thd_id(), txn_man->get_batch_id(),txn_man->get_txn_id());

    if (CC_ALG == WAIT_DIE) {
      txn_man->set_timestamp(get_next_ts());
    }
    #if CC_ALG == SDOCC
      if (txn_man->sdocc_phase == SDOCC_PHASE::SDOCC_INIT) {
        msg->copy_to_txn(txn_man);
        txn_man->sdocc_phase = SDOCC_PHASE::SDOCC_EXECUTION;
        txn_man->return_id = msg->return_node_id;
        txn_man->sdocc_send_remote = false;
        txn_man->sdocc_expected_rsp_cnt = 0;
        txn_man->txn_stats.starttime = get_sys_clock();
        txn_man->txn_stats.restart_starttime = txn_man->txn_stats.starttime;
      } else {
        assert(txn_man->sdocc_phase == SDOCC_PHASE::SDOCC_CHECK);
        // txn_man->txn_stats.starttime = get_sys_clock();
        txn_man->txn_stats.restart_starttime = get_server_clock();
        // txn_man->txn_stats.restart_starttime = txn_man->txn_stats.starttime;
      }
      // 这里是下一次重试的入口，将has_re_enqueued置为false，以便于允许下一次重试重新入队
      if (txn_man->last_msg) {
        ((ClientQueryMessage*)txn_man->last_msg)->has_re_enqueued.store(false, std::memory_order_relaxed);
      }
      // txn_man->has_re_enqueued = false;
      // txn_man->retry_cnt++;
    #else
      txn_man->txn_stats.starttime = get_sys_clock();
      txn_man->txn_stats.restart_starttime = txn_man->txn_stats.starttime;
      msg->copy_to_txn(txn_man);
    #endif
    DEBUG_WRK("START %ld,%ld %p %f %lu\n", txn_man->get_batch_id(),txn_man->get_txn_id(),txn_man,
          simulation->seconds_from_start(get_sys_clock()), txn_man->txn_stats.starttime);
   
    INC_STATS(get_thd_id(), local_txn_start_cnt, 1);
  } else {
    assert(CC_ALG != SDOCC);
    txn_man->txn_stats.restart_starttime = get_sys_clock();
    DEBUG_WRK("RESTART %ld %f %lu\n", txn_man->get_txn_id(),
        simulation->seconds_from_start(get_sys_clock()), txn_man->txn_stats.starttime);
  }
  // Get new timestamps
  if(is_cc_new_timestamp()) {
    txn_man->set_timestamp(get_next_ts());
  }

#if CC_ALG == OCC 
    txn_man->set_start_timestamp(get_next_ts());
#endif

  rc = init_phase();
  // for SDOCC
  #if CC_ALG == SDOCC
  txn_man->last_msg = msg;
  #endif

  txn_man->txn_stats.init_complete_time = get_sys_clock();
  INC_STATS(get_thd_id(),trans_init_time, txn_man->txn_stats.init_complete_time - txn_man->txn_stats.restart_starttime);
  INC_STATS(get_thd_id(),trans_init_count, 1);
  if (rc != RCOK) return rc;
  // Execute transaction
  txn_man->send_RQRY_RSP = false;
  
  #if CC_ALG == SDOCC
  rc = txn_man->run_sdocc_txn();
  // if (!txn_man->query->rwset_known) printf("txn %ld,%ld rwset unknown in RTXN\n", txn_man->get_batch_id(), txn_man->get_txn_id());
  #else
  rc = txn_man->run_txn();
  #endif

  check_if_done(rc);
  return rc;
}

RC WorkerThread::init_phase() {
  RC rc = RCOK;
  //m_query->part_touched[m_query->part_touched_cnt++] = m_query->part_to_access[0];
  return rc;
}


RC WorkerThread::process_log_msg(Message * msg) {
  assert(ISREPLICA);
  DEBUG("REPLICA PROCESS %ld\n",msg->get_txn_id());
  LogRecord * record = logger.createRecord(&((LogMessage*)msg)->record);
  logger.enqueueRecord(record);
  return RCOK;
}

RC WorkerThread::process_log_msg_rsp(Message * msg) {
  DEBUG("REPLICA RSP %ld\n",msg->get_txn_id());
  txn_man->repl_finished = true;
  if (txn_man->log_flushed) commit();
  return RCOK;
}

RC WorkerThread::process_log_flushed(Message * msg) {
  DEBUG("LOG FLUSHED %ld\n",msg->get_txn_id());
  if(ISREPLICA) {
    msg_queue.enqueue(get_thd_id(), Message::create_message(msg->txn_id, LOG_MSG_RSP),
                      GET_NODE_ID(msg->txn_id));
    return RCOK;
  }

  txn_man->log_flushed = true;
  if (!txn_man->is_multi_part() || txn_man->get_rsp_cnt() == 0) {
    txn_man->txn_stats.finish_start_time = get_sys_clock();
    commit();
  }
  return RCOK;
}

RC WorkerThread::process_rfwd(Message * msg) {
  DEBUG("RFWD (%ld,%ld)\n",msg->get_batch_id(),msg->get_txn_id());
  txn_man->txn_stats.remote_wait_time += get_sys_clock() - txn_man->txn_stats.wait_starttime;
  assert(CC_ALG == CALVIN || CC_ALG == SDPCC);
  int responses_left = txn_man->received_response(((ForwardMessage*)msg)->rc);
  assert(responses_left >=0);
  if(txn_man->calvin_collect_phase_done()) {
    assert(ISSERVERN(txn_man->return_id));
    RC rc = txn_man->run_calvin_txn();
    if(rc == RCOK && txn_man->calvin_exec_phase_done()) {
      calvin_wrapup();
      return RCOK;
    }
  }
  return WAIT;
}

RC WorkerThread::process_calvin_rtxn(Message * msg) {
  DEBUG("START %ld %f %lu\n", txn_man->get_txn_id(),
        simulation->seconds_from_start(get_sys_clock()), txn_man->txn_stats.starttime);
  assert(ISSERVERN(txn_man->return_id));
  #if CC_ALG == SDPCC
  uint64_t key = get_batch_key(txn_man->get_batch_id(), txn_man->return_id, txn_man->get_txn_id());
  // assert(key <= minSid);
  assert(txn_man->lock_ready_cnt <= 0);
  #endif
  txn_man->txn_stats.local_wait_time += get_sys_clock() - txn_man->txn_stats.wait_starttime;
  // Execute
  RC rc = txn_man->run_calvin_txn();
  // if((txn_man->phase==6 && rc == RCOK) || txn_man->active_cnt == 0 || txn_man->participant_cnt ==
  // 1) {
  if(rc == RCOK && txn_man->calvin_exec_phase_done()) {
  #if DETERMINISTIC_ABORT_MODE
    if (txn_man->query->isDeterministicAbort) {
      txn_man->query->isDeterministicAbort = false;
      calvin_abort();
      return Abort;
    }
  #endif
    calvin_wrapup();
  }
  return RCOK;
}

#if CC_ALG == ARIA
RC WorkerThread::process_aria_rtxn(Message * msg) {
  DEBUG("START %ld %f %lu\n", txn_man->get_txn_id(),
        simulation->seconds_from_start(get_sys_clock()), txn_man->txn_stats.starttime);
  if (simulation->aria_phase == ARIA_READ && txn_man->txn_stats.abort_cnt == 0) {
    // printf("txn: %ld copy msg to txn\n", txn_man->get_txn_id());
    msg->copy_to_txn(txn_man);
    assert(ISSERVERN(txn_man->return_id));
  }
  txn_man->txn_stats.local_wait_time += get_sys_clock() - txn_man->txn_stats.wait_starttime;
  // Execute
  RC rc = txn_man->run_aria_txn();
  if (simulation->aria_phase == ARIA_COMMIT) {
    if (rc != WAIT_REM) {
      work_queue.sequencer_enqueue(get_thd_id(),Message::create_message(txn_man, ARIA_ACK));
      if (txn_man->get_rc() == Abort) {
        abort();
      } else {
        commit();
      }
    }
  }
  return RCOK;
}

RC WorkerThread::process_aria_ack(Message * msg) {
  AckMessage * ack = (AckMessage *)msg;
  // 考虑几种情况吧，消息落后于当前阶段了，这个明显不对

  int index = 0;
  if (ack->aria_phase == ARIA_RESERVATION) {
    index = 0;
  } else if (ack->aria_phase == ARIA_CHECK) {
    index = 1;
  } else {
    assert(false);
  }
  assert(ack->batch_id == simulation->current_batch_id || ack->batch_id == simulation->current_batch_id + 1);

  if (ack->batch_id < simulation->current_batch_id || 
     (ack->batch_id == simulation->current_batch_id && ack->aria_phase < simulation->aria_phase)) {
    assert(false);
  } else if (ack->batch_id > simulation->current_batch_id || 
     (ack->batch_id == simulation->current_batch_id && ack->aria_phase > simulation->aria_phase)) {
    // 说明这个ACK是下一轮的
    DEBUG_SCH("Worker %ld received future ARIA_ACK for node %ld, ack batch %ld phase %ld, current batch %ld phase %d\n", get_thd_id(), ack->get_return_id(), ack->batch_id, ack->aria_phase, simulation->current_batch_id, simulation->aria_phase);
  } else {
    DEBUG_SCH("Worker %ld received ARIA_ACK for node %ld, batch %ld phase %ld\n", get_thd_id(), ack->get_return_id(), ack->batch_id, ack->aria_phase);
  }
  assert(ack->batch_id == simulation->aria_barrier[index].batch_id);
  simulation->aria_barrier[index].set_barrier(ack->get_return_id());
  std::string str = simulation->aria_barrier[0].get_barrier_str("0") + simulation->aria_barrier[1].get_barrier_str("1");
  DEBUG_SCH("%s\n", str.c_str());
  msg->release();
  delete msg;
  // } else if (ack->batch_id == simulation->current_batch_id && ack->aria_phase == simulation->aria_phase) {
  //   // 说明这个ACK是当前轮的，直接处理
  //   if (simulation->aria_barrier[simulation->aria_barrier_index].check_barrier(ack->get_return_id())) {
  //     // 如果对应的节点已经到达屏障了，说明这个ACK是重复的，出了问题
  //     assert(false);
  //   } else {
  //     DEBUG_SCH("Worker %ld received ARIA_ACK for node %ld, batch %ld phase %ld\n", get_thd_id(), ack->get_return_id(), ack->batch_id, ack->aria_phase);
  //     // DEBUG_SCH("Worker %ld reached barrier for node %ld\n", get_thd_id(), msg->get_return_id());
  //     simulation->aria_barrier[simulation->aria_barrier_index].set_barrier(ack->get_return_id());
  //     std::string str = simulation->aria_barrier[0].get_barrier_str("0") + simulation->aria_barrier[1].get_barrier_str("1");
  //     DEBUG_SCH("%s\n", str.c_str());
  //     msg->release();
  //     delete msg;
  //   }
  // }


  // if (simulation->barriers[ack->get_return_id()]) {
  //   work_queue.enqueue(_thd_id, msg, false);
  //   DEBUG_SCH("Worker %ld received ARIA_ACK for node %ld, but already reached barrier, re-enqueueing, now phase %d\n", get_thd_id(), ack->get_return_id(), simulation->aria_phase);
  // } else {
  //   simulation->barriers[ack->get_return_id()] = true;
  //   // DEBUG_SCH("Worker %ld reached barrier for node %ld\n", get_thd_id(), msg->get_return_id());
  //   simulation->barrier_count++;
  //   msg->release();
  //   delete msg;
  // }
  return RCOK;
}
#endif

#if CC_ALG == CARACAL 
RC WorkerThread::process_caracal_rsubtxn(Message * msg) {
  DEBUG_WRK("START CARACAL_SUB_TXN %ld,%ld\n",msg->get_batch_id(),msg->get_txn_id());
  assert(simulation->caracal_phase == CARACAL_EXECUTION || simulation->caracal_phase == CARACAL_EXECUTION_SYNC);
  if (txn_man->last_msg == nullptr) {
  // if (txn_man->caracal_txn_phase < CARACAL_TXN_RD) {
    msg->copy_to_txn(txn_man);
    txn_man->last_msg = msg;
  }

  RC rc = txn_man->run_sub_caracal_txn();
    // 如果是跑完了
  if (rc == RCOK) {
    assert(txn_man->caracal_txn_phase == CARACAL_TXN_DONE);
    caracal_wrapup();
    // caracal_wrapup_subtxn();
  } else if (rc == WAIT_SUB) {
    // 如果在等子事务同步，就直接结束，由最后一个完成的子事务来做wrapup
    assert(txn_man->caracal_txn_phase == CARACAL_TXN_WR_SYNC);
    caracal_wrapup_subtxn();
  } else if (rc == WAIT) {
    // 说明这个事务在caracal执行阶段wait了，需要重新入队
    work_queue.work_enqueue(get_thd_id(), msg, false, txn_man->caracal_phase);
  } else if (rc == WAIT_REM) {
    // 说明这个事务在caracal执行阶段wait了，并且是等待远程消息的，需要重新入队
    work_queue.work_enqueue(get_thd_id(), msg, false, txn_man->caracal_phase);
  } else {
    assert(false);
  }
  return rc;
}
RC WorkerThread::process_caracal_rtxn(Message * msg) {
  DEBUG_WRK("START CARACAL_TXN %ld,%ld %f %lu\n", msg->get_batch_id(),msg->get_txn_id(),
        simulation->seconds_from_start(get_sys_clock()), txn_man->txn_stats.starttime);
  if (simulation->caracal_phase <= CARACAL_INIT_SYNC 
     && txn_man->txn_stats.abort_cnt == 0) {
    // printf("txn: %ld copy msg to txn\n", txn_man->get_txn_id());
    msg->copy_to_txn(txn_man);
    assert(ISSERVERN(txn_man->return_id));
    assert(msg->rtype == CL_QRY);
    caracal_man.insert_msg(get_thd_id(),msg);
    txn_man->last_msg = msg;
  }
  txn_man->txn_stats.local_wait_time += get_sys_clock() - txn_man->txn_stats.wait_starttime;
  // Execute
  CARACAL_PHASE current_phase = txn_man->caracal_phase;
  RC rc = txn_man->run_caracal_txn();
  // ! 如果执行阶段，caracal wait状态了，应该重新被塞回消息队列里
  if (current_phase <= CARACAL_INIT_SYNC) {
    // init阶段，不存在重试，所以必然成功
    assert(simulation->caracal_phase <= CARACAL_INIT_SYNC);
    if (!IS_LOCAL(msg->get_txn_id())) {
      AckMessage* ack = (AckMessage*) Message::create_message(txn_man,CARACAL_TXN_ACK);
      ack->batch_id = txn_man->get_batch_id();
      ack->caracal_phase = CARACAL_INIT;
      ATOM_ADD_FETCH(simulation->batch_remote_send_count,1);
      DEBUG_WRK("Worker %ld init txn %ld,%ld send CARACAL_TXN_ACK to node %ld, now batch_remote_send_count %ld\n", get_thd_id(), txn_man->get_batch_id(), txn_man->get_txn_id(), msg->return_node_id, simulation->batch_remote_send_count);

      msg_queue.enqueue(get_thd_id(), ack, msg->return_node_id);
    }
    {
      if ((IS_LOCAL(msg->get_txn_id()))) {
        assert(simulation->current_batch_id == msg->get_batch_id());
        ATOM_ADD_FETCH(simulation->batch_process_count, 1);
        ATOM_ADD_FETCH(simulation->batch_local_process_count, 1);
        DEBUG_WRK("Worker %ld finished processing local txn %ld,%ld phase %d, now batch_process_count %ld/%ld, batch_local_process_count %ld, batch_remote_process_count %ld, batch_remote_send_count %ld\n", get_thd_id(), msg->get_batch_id(), msg->get_txn_id(), simulation->caracal_phase, simulation->batch_process_count, caracal_seq.get_total_ack_count(), simulation->batch_local_process_count, simulation->batch_remote_process_count, simulation->batch_remote_send_count);
      }
    }
    return RCOK;
  }
  else if (current_phase == CARACAL_EXECUTION ||
           current_phase == CARACAL_EXECUTION_SYNC) {
    // 如果是跑完了
    assert(simulation->caracal_phase == CARACAL_EXECUTION || simulation->caracal_phase == CARACAL_EXECUTION_SYNC);
    if (rc == RCOK) {
      assert(txn_man->caracal_txn_phase == CARACAL_TXN_DONE);
      caracal_wrapup();
    } else if (rc == WAIT_SUB) {
      // assert(false);
      assert(txn_man->caracal_txn_phase == CARACAL_TXN_WR_SYNC);
      caracal_wrapup_subtxn();
    } else if (rc == WAIT) {
      // 说明这个事务在caracal执行阶段wait了，需要重新入队
      work_queue.work_enqueue(get_thd_id(), msg, false, txn_man->caracal_phase);
    } else if (rc == WAIT_REM) {
      // 说明这个事务在caracal执行阶段wait了，并且是等待远程消息的，需要重新入队
      work_queue.work_enqueue(get_thd_id(), msg, false, txn_man->caracal_phase);
    } else {
      assert(false);
    }
    return rc;
  } else {
    assert(false);
  }
}

RC WorkerThread::process_caracal_rfwd(Message * msg) {
  DEBUG("RFWD (%ld,%ld)\n",msg->get_batch_id(),msg->get_txn_id());
  txn_man->txn_stats.remote_wait_time += get_sys_clock() - txn_man->txn_stats.wait_starttime;
  assert(CC_ALG == CARACAL);
  // int responses_left = txn_man->received_response(((ForwardMessage*)msg)->rc);
  // assert(responses_left >=0);
  assert(txn_man->caracal_phase == CARACAL_EXECUTION);
  ATOM_SUB_FETCH(txn_man->last_msg->caracal_expected_rsp_cnt, 1);
  // caracal_man.caracal_txn_ack_man.decrement_rsp_cnt(txn_man->get_batch_id(), txn_man->get_txn_id());
  // txn_man->caracal_expected_rsp_cnt.fetch_sub(1);

  return WAIT;
}

void WorkerThread::caracal_wrapup() {
  txn_man->release_locks(RCOK);
  txn_man->commit_stats();
  DEBUG_WRK("COMMIT %ld,%ld %p %f %lu\n", txn_man->get_batch_id(),txn_man->get_txn_id(),txn_man,
        simulation->seconds_from_start(get_sys_clock()), txn_man->txn_stats.starttime);
  
  Message* msg = Message::create_message(txn_man,CARACAL_DONE); 
  ((AckMessage*)msg)->batch_id = txn_man->get_batch_id();
  ((AckMessage*)msg)->caracal_phase = CARACAL_EXECUTION;

  if(txn_man->return_id == g_node_id) {
    work_queue.sequencer_enqueue(_thd_id,msg);
    DEBUG_WRK("Worker %ld commit txn %ld,%ld send CARACAL_DONE\n", get_thd_id(), txn_man->get_batch_id(), txn_man->get_txn_id());
  } else {
    msg_queue.enqueue(get_thd_id(), msg, txn_man->return_id);
    DEBUG_WRK("Worker %ld commit txn %ld,%ld send CARACAL_DONE to node %ld\n", get_thd_id(), txn_man->get_batch_id(), txn_man->get_txn_id(), txn_man->return_id);
  }
  release_txn_man();
}

void WorkerThread::caracal_wrapup_subtxn() {
  txn_man->release_locks(RCOK);
  // txn_man->commit_stats();
  DEBUG_WRK("COMMIT sub txn %ld,%ld %p %f %lu\n", txn_man->get_batch_id(),txn_man->get_txn_id(),txn_man,
        simulation->seconds_from_start(get_sys_clock()), txn_man->txn_stats.starttime);
  release_txn_man();
}
#endif

bool WorkerThread::is_cc_new_timestamp() {
  return false;
  // return (CC_ALG == MVCC || CC_ALG == TIMESTAMP || CC_ALG == DTA || CC_ALG == WOOKONG);
}

ts_t WorkerThread::get_next_ts() {
	if (g_ts_batch_alloc) {
		if (_curr_ts % g_ts_batch_num == 0) {
			_curr_ts = glob_manager.get_ts(get_thd_id());
			_curr_ts ++;
		} else {
			_curr_ts ++;
		}
		return _curr_ts - 1;
	} else {
		_curr_ts = glob_manager.get_ts(get_thd_id());
		return _curr_ts;
	}
}

#if CC_ALG == SDOCC

void WorkerThread::handle_tmp_txn(uint64_t current_minSid, uint64_t &old_minSid) {
  double start_time = get_sys_clock();
  int out_cnt = 0;
  std::vector<TxnManager*> to_reenqueue = tmp_txn_list.pop_less_than(current_minSid);
  for (auto txn_man:to_reenqueue){
  // TxnManager* txn_man = nullptr;
  // for (;tmp_txn_list.get_next(current_minSid,txn_man);){
		uint64_t key = get_batch_key(txn_man->get_batch_id(), txn_man->return_id, txn_man->get_txn_id());
		DEBUG_SCH("[SDOCCThread] %ld handle txn %ld,%ld, key %ld, current_minSid %ld\n", _thd_id, txn_man->get_batch_id(), txn_man->get_txn_id(), key, current_minSid);

    // 如果水印过了，说明是因为其他原因导致的重试，这种情况直接放回work queue
    assert(txn_man->sdocc_phase == SDOCC_CHECK);
    assert(txn_man->last_msg->get_rtype() == CL_QRY);
    INC_STATS(get_thd_id(), tmp_txn_time, get_sys_clock()-txn_man->enter_tmp_queue_time);
    INC_STATS(get_thd_id(), tmp_txn_cnt, 1);
    work_queue.sdocc_enqueue(get_thd_id(), txn_man->last_msg, false);
    tmp_txn_list_size--;
    out_cnt++;
    DEBUG_SCH("[SDOCCThread] %ld enqueue txn %ld,%ld\n", _thd_id, txn_man->get_batch_id(), txn_man->get_txn_id());
  }
	old_minSid = current_minSid;
  double end_time = get_sys_clock();
  DEBUG_SCH("[SDOCCThread] %ld handle tmp_txn_list, current_minSid %ld, old_minSid %ld, tmp_txn_list size %ld, out_cnt %d, cosume time %lf\n", _thd_id,current_minSid, old_minSid, tmp_txn_list_size, out_cnt, (end_time - start_time)/BILLION);
}
#endif

void StatsPerIntervalThread::setup(){

}

RC StatsPerIntervalThread::run(){
  printf("Running StatsPerIntervalThread %ld\n",_thd_id);
  uint64_t last_millisecond = get_sys_clock();
  uint64_t last_second = get_sys_clock();
  uint64_t now_time;
  uint64_t txn_cnt_last_time = 0, txn_cnt_this_time = 0;
  uint64_t silo_cnt_last_time = 0, silo_cnt_this_time = 0, calvin_cnt_last_time = 0, calvin_cnt_this_time = 0;
  uint64_t loop = 0;

  txn_cnt_last_time = stats.get_txn_cnts();
  tsetup();
  
  while (!simulation->is_done()){
    now_time = get_sys_clock();
    if(now_time - last_second > ONE_SECOND){
      //tput every interval
      txn_cnt_this_time = stats.get_txn_cnts();
      INC_STATS(_thd_id, tputs[loop], (txn_cnt_this_time - txn_cnt_last_time));
      txn_cnt_last_time = txn_cnt_this_time;
      txn_cnt_this_time = 0;
      last_second = now_time;

      #if CC_ALG == SDPCC
      // work_queue.sdpcc_scheduled_list_lockfree->DEBUG_PRINT_LIST_LENGTH();
      // work_queue.sdpcc_list->DEBUG_PRINT_LIST_LENGTH();
      #endif
      #if CC_ALG == SDOCC
      work_queue.sdocc_lockfree->DEBUG_PRINT_LIST_LENGTH();
      #endif
      DEBUG_TIME("------StatsPerIntervalThread %ld seconds--------\n",loop);
      loop++;
    }
    // if (now_time - last_millisecond > ONE_MILLISECOND) {
    #if CC_ALG == SDOCC 
      check_water_mark->remove_consumed();
      bool updated = check_water_mark->update_local_watermark();
      if (updated) {
        for (uint64_t i = 0; i < g_node_cnt; i++) {
          if (i == g_node_id) continue;
          Message * msg = check_water_mark->broadcast_watermark();
          DEBUG_SCH("Worker %ld broadcast watermark %ld\n", get_thd_id(), check_water_mark->get_global_watermark());
          if (msg) {
            msg_queue.enqueue(_thd_id, msg, i);
          }
        }
      }
    #endif 
    #if CC_ALG == SDPCC
      // work_queue.sdpcc_list->mark_head();
      // 把第一个调度器的水印更新塞到这来，保证每个调度器的水印都能及时更新
      uint64_t min = UINT64_MAX;
			for (uint64_t i = 0; i < g_scheduler_thread_cnt; i++) {
				uint64_t current_sid = sids[i];
				if (current_sid < min) min = current_sid;
			}
			assert(min >= minSid);
			minSid = min;
    #endif
      // last_millisecond = now_time;
    // }
  }
  printf("FINISH %ld:%ld\n",_node_id,_thd_id);
  fflush(stdout);
  return FINISH;
}

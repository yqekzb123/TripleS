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

#include "global.h"
#include "manager.h"
#include "thread.h"
#include "sdpcc_thread.h"
#include "txn.h"
#include "wl.h"
#include "query.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "mem_alloc.h"
#include "transport.h"
#include "math.h"
#include "helper.h"
#include "msg_thread.h"
#include "msg_queue.h"
#include "sdpcc_sequencer.h"
#include "logger.h"
#include "message.h"
#include "work_queue.h"
#include <vector>

#if CC_ALG == SDPCC
void SDPCCLockThread::setup() {}

RC SDPCCLockThread::run() {
	tsetup();

	RC rc = RCOK;
	TxnManager * txn_man;
	uint64_t prof_starttime = get_sys_clock();
	uint64_t idle_starttime = 0;

	uint64_t id = _thd_id % g_scheduler_thread_cnt;

	uint64_t old_minSid = 0;

	while(!simulation->is_done()) {
		txn_man = NULL;

		Message * msg = work_queue.sdpcc_sched_dequeue(_thd_id);

		if(!msg) {
			if (idle_starttime == 0) idle_starttime = get_sys_clock();
			continue;
		}
		if(idle_starttime > 0) {
			INC_STATS(_thd_id,sched_idle_time,get_sys_clock() - idle_starttime);
			idle_starttime = 0;
		}

		prof_starttime = get_sys_clock();
		assert(msg->get_txn_id() != UINT64_MAX);
		txn_man =
				txn_table.get_transaction_manager(get_thd_id(), msg->get_txn_id(), msg->get_batch_id());
		assert(msg->get_rtype() == CL_QRY);

		while (!txn_man->unset_ready()) {
		}
		assert(ISSERVERN(msg->get_return_id()));
		txn_man->txn_stats.starttime = get_sys_clock();

		txn_man->txn_stats.lat_network_time_start = msg->lat_network_time;
		txn_man->txn_stats.lat_other_time_start = msg->lat_other_time;

		msg->copy_to_txn(txn_man);
		txn_man->register_thread(this);
		assert(ISSERVERN(txn_man->return_id));

		INC_STATS(get_thd_id(),sched_txn_table_time,get_sys_clock() - prof_starttime);
		prof_starttime = get_sys_clock();

		rc = RCOK;
		// Acquire locks
		{
			ATOM_CAS(txn_man->lock_ready, true, false);
			txn_man->incr_lr();
		}
		if (!txn_man->isRecon()) {
			rc = txn_man->acquire_locks();
		}

		// 更新水印minSid
		uint64_t old_sid = sids[id];
		uint64_t key = get_batch_key(txn_man->get_batch_id(), txn_man->return_id, txn_man->get_txn_id());
		// sids[id] = (txn_man->get_batch_id() << 32) + (txn_man->return_id << 24) + txn_man->get_txn_id() + 1;
		assert(key > minSid);
		assert(key > sids[id]);
		sids[id] = key;

		uint64_t current_minSid = minSid;
		DEBUG_SCH("[SDPCCThread] %ld set sid from %ld to %ld, now minSid %ld\n", _thd_id, old_sid, sids[id], minSid);
		//Update minSid
		// if (_thd_id == the_first_scheduler_id) {
		// 	uint64_t min = UINT64_MAX;
		// 	// #if DEBUG_SCHEDULER
		// 	// std::string sid_log = "[SDPCCThread] " + std::to_string(_thd_id) + " minSid update: sids = ";
		// 	// #endif
		// 	for (uint64_t i = 0; i < g_scheduler_thread_cnt; i++) {
		// 		uint64_t current_sid = sids[i];
		// 		// #if DEBUG_SCHEDULER
		// 		// sid_log += std::to_string(current_sid) + " ";
		// 		// #endif
		// 		if (current_sid < min) min = current_sid;
		// 	}
		// 	assert(min >= minSid);
		// 	minSid = min;
		// 	// #if DEBUG_SCHEDULER
		// 	// sid_log += "| new minSid = " + std::to_string(minSid);
		// 	// std::vector<uint64_t> ids = split_batch_key(minSid);
		// 	// sid_log += "| now (" + std::to_string(ids[0]) + "," + std::to_string(ids[2]) +") can running\n";
		// 	// std::cout << sid_log;
		// 	// #endif
		// }

		txn_man->last_msg = msg;
		
		// // 这个是水印对应的 lock
		

		// if (tmp_txn_list.size() > 0){
		// 	TxnManager* last_txn = tmp_txn_list.back();
		// 	uint64_t last_key = get_batch_key(last_txn->get_batch_id(), last_txn->return_id, last_txn->get_txn_id());
		// 	assert(key > last_key);
		// }

		tmp_txn_list.push_back(txn_man);
		DEBUG_SCH("[SDPCCThread] %ld txn %ld,%ld enter tmp_txn_list\n", _thd_id, txn_man->get_batch_id(), txn_man->get_txn_id());
		if (current_minSid != old_minSid) {
			// !检查是否要塞入队列的逻辑
			handle_tmp_txn(current_minSid, old_minSid);
		}
		// work_queue.insert_sdpcc_list_lockfree(_thd_id, txn_man);	
		// if (rc == RCOK) {
		// 	work_queue.enqueue(_thd_id,txn_man->last_msg,false);
		// }

		txn_man->set_ready();

		INC_STATS(_thd_id,mtx[33],get_sys_clock() - prof_starttime);
		prof_starttime = get_sys_clock();
	}
	printf("FINISH %ld:%ld\n",_node_id,_thd_id);
	fflush(stdout);
	return FINISH;
}

void SDPCCLockThread::handle_tmp_txn(uint64_t current_minSid, uint64_t &old_minSid) {
	DEBUG_SCH("[SDPCCThread] %ld handle tmp_txn_list, current_minSid %ld, old_minSid %ld\n", _thd_id,current_minSid,old_minSid);
	// 开始尝试遍历vector中key小于current_minSid的，然后根据有没有加到锁，塞到队列里去.
	uint64_t idx = 0;
	for(idx = 0; idx < tmp_txn_list.size(); idx++) {
	// for (auto txn_man:tmp_txn_list){
		TxnManager *txn_man = tmp_txn_list[idx];
		uint64_t key = get_batch_key(txn_man->get_batch_id(), txn_man->return_id, txn_man->get_txn_id());
		DEBUG_SCH("[SDPCCThread] %ld handle txn %ld,%ld, lock_ready_cnt %d, key %ld, current_minSid %ld\n", _thd_id, txn_man->get_batch_id(), txn_man->get_txn_id(), txn_man->lock_ready_cnt ,key, current_minSid);
		if (key > current_minSid) break;
		if (txn_man->decr_lr() == 0) {
			// 塞到队列里
			if(ATOM_CAS(txn_man->lock_ready,false,true)) {
				work_queue.enqueue(_thd_id,txn_man->last_msg,false);
				// work_queue.insert_sdpcc_list_lockfree(_thd_id, txn_man);
				DEBUG_SCH("[SDPCCThread] %ld enqueue txn %ld,%ld\n", _thd_id, txn_man->get_batch_id(), txn_man->get_txn_id());
			} else {
				DEBUG_SCH("[SDPCCThread] %ld handle txn %ld,%ld failed, lock_ready_cnt %d, key %ld, current_minSid %ld\n", _thd_id, txn_man->get_batch_id(), txn_man->get_txn_id(), txn_man->lock_ready_cnt ,key, current_minSid);
			}
		} else {
			DEBUG_SCH("[SDPCCThread] %ld handle txn %ld,%ld failed, lock_ready_cnt %d, key %ld, current_minSid %ld\n", _thd_id, txn_man->get_batch_id(), txn_man->get_txn_id(), txn_man->lock_ready_cnt ,key, current_minSid);
		}
		// idx++;
	}
	// !还差一段，把小于idx的事务，都从队列里删掉
	// tmp_txn_list.erase(0, idx - 1);
	tmp_txn_list.erase(tmp_txn_list.begin(), tmp_txn_list.begin() + idx);

	old_minSid = current_minSid;
}

void SDPCCSequencerThread::setup() {}

bool SDPCCSequencerThread::is_batch_ready() {
	bool ready1 = get_wall_clock() - simulation->last_seq_epoch_time >= g_seq_batch_time_limit;
	bool ready2 = sdpcc_seq_man.is_batch_ready();
	// return ready2;
	return ready1;
}

RC SDPCCSequencerThread::run() {
	tsetup();

	Message * msg;
	uint64_t idle_starttime = 0;
	uint64_t prof_starttime = 0;

	while(!simulation->is_done()) {

		prof_starttime = get_sys_clock();

		if(is_batch_ready()) {
			simulation->advance_seq_epoch();
			//last_batchtime = get_wall_clock();
			seq_man.send_next_batch(_thd_id);
			// sdpcc_seq_man.send_next_batch(_thd_id);
		}

		INC_STATS(_thd_id,mtx[30],get_sys_clock() - prof_starttime);
		prof_starttime = get_sys_clock();

		msg = work_queue.sequencer_dequeue(_thd_id);

		INC_STATS(_thd_id,mtx[31],get_sys_clock() - prof_starttime);
		prof_starttime = get_sys_clock();

		if(!msg) {
			if (idle_starttime == 0) idle_starttime = get_sys_clock();
				continue;
		}
		if(idle_starttime > 0) {
			INC_STATS(_thd_id,seq_idle_time,get_sys_clock() - idle_starttime);
			idle_starttime = 0;
		}

		auto rtype = msg->get_rtype();
		switch (rtype) {
			case CL_QRY:
				// Query from client
				DEBUG("SEQ process_txn\n");
				seq_man.process_txn(msg,get_thd_id(),0,0,0,0);
				// sdpcc_seq_man.process_txn(msg,get_thd_id(),0,0,0,0);
				// Don't free message yet
				break;
			case CALVIN_ACK:
				// Ack from server
				DEBUG("SEQ process_ack (%ld,%ld) from %ld\n", msg->get_batch_id(),msg->get_txn_id(), 
							msg->get_return_id());
				// sdpcc_seq_man.process_ack(msg,get_thd_id());
				seq_man.process_ack(msg,get_thd_id());
				// Free message here
				msg->release();
				break;
			case CALVIN_ABORT:
				// sdpcc_seq_man.process_abort(msg, get_thd_id());
				seq_man.process_abort(msg, get_thd_id());
				// Don't free message yet
				break;
			default:
				assert(false);
		}

		INC_STATS(_thd_id,mtx[32],get_sys_clock() - prof_starttime);
		prof_starttime = get_sys_clock();
	}
	printf("FINISH %ld:%ld\n",_node_id,_thd_id);
	fflush(stdout);
	return FINISH;

}

#endif
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
#include "water_mark.h"
#include "sdpcc_long_hole.h"

#if SDPCC_FAMILY
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
			#if CC_ALG == SDMVCC
			uint64_t current_minSid = minSid;
			if (current_minSid != old_minSid) handle_tmp_txn(current_minSid, old_minSid);
			#endif
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
		uint64_t key = get_batch_key(txn_man->get_batch_id(), txn_man->return_id, txn_man->get_txn_id());
		bool long_hole = false;
		#if CC_ALG == SDPCC && !OPEN_DISTRIBUTED_WATERMARK
		long_hole = sdpcc_long_hole_man->should_publish(id, txn_man);
		if (long_hole) sdpcc_long_hole_man->publish(id, key, txn_man);
		#endif
		if (!txn_man->isRecon()) {
			rc = txn_man->acquire_locks();
		}
		#if CC_ALG == SDPCC && !OPEN_DISTRIBUTED_WATERMARK
		if (long_hole) {
			// All local lock requests are registered; row queues now enforce conflicts.
			sdpcc_long_hole_man->clear(id, key);
		}
		#endif
		#if CC_ALG == SDPCC && OPEN_DISTRIBUTED_WATERMARK
		check_water_mark->mark_completed(key, get_thd_id());
		DEBUG_SCH("[SDPCCThread] %ld mark %ld,%ld key %ld complete\n", _thd_id, txn_man->get_batch_id(),txn_man->get_txn_id(), key);
		uint64_t current_minSid = check_water_mark->get_global_watermark();
		#elif CC_ALG == SDPCC
		// 更新水印minSid
		uint64_t old_sid = sids[id];
		assert(long_hole ? key >= minSid : key > minSid);
		if (!long_hole) sdpcc_long_hole_man->advance_normal(id, key);
		uint64_t current_minSid = minSid;
		DEBUG_SCH("[SDPCCThread] %ld set sid from %ld to %ld, now minSid %ld\n", _thd_id, old_sid, sids[id], minSid);
		#else
		uint64_t old_sid = sids[id];
		assert(key > old_sid);
		sids[id] = key;
		uint64_t current_minSid = minSid;
		#endif
		txn_man->last_msg = msg;

		bool bypass = false;
		#if CC_ALG == SDPCC && !OPEN_DISTRIBUTED_WATERMARK
		if (!long_hole && current_minSid < key && sdpcc_long_hole_man->enabled() &&
				sdpcc_long_hole_man->should_check(id, key)) {
			bypass = sdpcc_long_hole_man->can_bypass(id, txn_man, key);
		}
		#endif
		if (bypass) {
			if (txn_man->decr_lr() == 0 && ATOM_CAS(txn_man->lock_ready, false, true)) {
				work_queue.enqueue(_thd_id, txn_man->last_msg, false);
			}
		} else {
			PendingTxn pending = {txn_man, get_sys_clock()};
			tmp_txn_list.push_back(pending);
			DEBUG_SCH("[SDPCCThread] %ld txn %ld,%ld enter tmp_txn_list\n", _thd_id, txn_man->get_batch_id(), txn_man->get_txn_id());
		}
		if (!bypass && current_minSid != old_minSid) {
			// !检查是否要塞入队列的逻辑
			handle_tmp_txn(current_minSid, old_minSid);
		}
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
		TxnManager *txn_man = tmp_txn_list[idx].txn;
		uint64_t key = get_batch_key(txn_man->get_batch_id(), txn_man->return_id, txn_man->get_txn_id());
		DEBUG_SCH("[SDPCCThread] %ld handle txn %ld,%ld, lock_ready_cnt %d, key %ld, current_minSid %ld\n", _thd_id, txn_man->get_batch_id(), txn_man->get_txn_id(), txn_man->lock_ready_cnt ,key, current_minSid);
		if (key > current_minSid) break;
		#if CC_ALG == SDPCC && !OPEN_DISTRIBUTED_WATERMARK
		sdpcc_long_hole_man->record_watermark_wait(
				_thd_id % g_scheduler_thread_cnt,
				get_sys_clock() - tmp_txn_list[idx].wait_start);
		#endif
		#if CC_ALG == SDMVCC
		txn_man->arm_sdmvcc_intents();
		#endif
		if (txn_man->decr_lr() == 0) {
			// 塞到队列里
			if(ATOM_CAS(txn_man->lock_ready,false,true)) {
				DEBUG_SCH("[SDPCCThread] %ld enqueue txn %ld,%ld\n", _thd_id, txn_man->get_batch_id(), txn_man->get_txn_id());
				work_queue.enqueue(_thd_id,txn_man->last_msg,false);
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

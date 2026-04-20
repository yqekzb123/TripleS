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
#include "sequencer.h"
#include "logger.h"
#include "message.h"
#include "work_queue.h"

#if CC_ALG == SDPCC
void SDPCCLockThread::setup() {}


RC SDPCCLockThread::run() {
	tsetup();

	RC rc = RCOK;
	TxnManager * txn_man;
	uint64_t prof_starttime = get_sys_clock();
	uint64_t idle_starttime = 0;

	uint64_t id = _thd_id % g_scheduler_thread_cnt;

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
		if (!txn_man->isRecon()) {
			rc = txn_man->acquire_locks();
		}

		uint64_t old_sid = sids[id];
		uint64_t key = get_batch_key(txn_man->get_batch_id(), txn_man->return_id, txn_man->get_txn_id());
		// sids[id] = (txn_man->get_batch_id() << 32) + (txn_man->return_id << 24) + txn_man->get_txn_id() + 1;
		assert(key > minSid);
		assert(key > sids[id]);
		sids[id] = key;
		DEBUG_SCH("[SDPCCThread] %ld set sid from %ld to %ld, now minSid %ld\n", _thd_id, old_sid, sids[id], minSid);
		//Update minSid
		if (_thd_id == the_first_scheduler_id) {

			uint64_t min = UINT64_MAX;
			#if DEBUG_SCHEDULER
			std::string sid_log = "[SDPCCThread] " + std::to_string(_thd_id) + " minSid update: sids = ";
			#endif
			for (uint64_t i = 0; i < g_scheduler_thread_cnt; i++) {
				uint64_t current_sid = sids[i];
				#if DEBUG_SCHEDULER
				sid_log += std::to_string(current_sid) + " ";
				#endif
				if (current_sid < min) min = current_sid;
			}
			assert(min >= minSid);
			minSid = min;
			#if DEBUG_SCHEDULER
			sid_log += "| new minSid = " + std::to_string(minSid);
			std::vector<uint64_t> ids = split_batch_key(minSid);
			sid_log += "| now (" + std::to_string(ids[0]) + "," + std::to_string(ids[2]) +") can running\n";
			std::cout << sid_log;
			#endif
		}

		txn_man->last_msg = msg;
		// printf("Try to Enqueue txn %ld,%ld, sid=%lu rc=%d\n", txn_man->get_batch_id(),txn_man->get_txn_id(), key, rc);
		// if (rc == RCOK) {
		// 	work_queue.insert_sdpcc_list_lockfree(_thd_id, txn_man);	
		// 	printf("Enqueue txn %ld,%ld, sid=%lu\n", txn_man->get_batch_id(),txn_man->get_txn_id(), key);
		// }
		// work_queue.enqueue(_thd_id,msg,false);
		work_queue.insert_sdpcc_list_lockfree(_thd_id, txn_man);	
			

		txn_man->set_ready();

		INC_STATS(_thd_id,mtx[33],get_sys_clock() - prof_starttime);
		prof_starttime = get_sys_clock();
	}
	printf("FINISH %ld:%ld\n",_node_id,_thd_id);
	fflush(stdout);
	return FINISH;
}

void SDPCCSequencerThread::setup() {}

bool SDPCCSequencerThread::is_batch_ready() {
	bool ready = get_wall_clock() - simulation->last_seq_epoch_time >= g_seq_batch_time_limit;
	return ready;
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
				// Don't free message yet
				break;
			case CALVIN_ACK:
				// Ack from server
				DEBUG("SEQ process_ack (%ld,%ld) from %ld\n", msg->get_batch_id(),msg->get_txn_id(), 
							msg->get_return_id());
				seq_man.process_ack(msg,get_thd_id());
				// Free message here
				msg->release();
				break;
			case CALVIN_ABORT:
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
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

#include <vector>
#include <unordered_set>

#include "global.h"
#include "sequencer.h"
#include "ycsb_query.h"
#include "da_query.h"
#include "tpcc_query.h"
#include "pps_query.h"
#include "mem_alloc.h"
#include "transport.h"
#include "wl.h"
#include "helper.h"
#include "msg_queue.h"
#include "msg_thread.h"
#include "work_queue.h"
#include "message.h"
#include "stats.h"
#include <boost/lockfree/queue.hpp>
#include "reorder.h"
#include "manager.h"
#if CC_ALG == HDCC || CC_ALG == SNAPPER
#include "cc_selector.h"
#endif

void Sequencer::init(Workload * wl) {
	next_txn_id = 0;
	rsp_cnt = g_node_cnt + g_client_node_cnt;
	_wl = wl;
	last_time_batch = 0;
	wl_head = NULL;
	wl_tail = NULL;
	fill_queue = new boost::lockfree::queue<Message*, boost::lockfree::capacity<65526> > [g_node_cnt];
	
#if CC_ALG == HDCC || CC_ALG == SNAPPER
	last_epoch_max_id = 0;
	blocked = false;
	validationCount = 0;
#endif
}

// Simple registry: txn_id -> TxnManager* (used to find dependent txn manager by id)
// static std::unordered_map<uint64_t, TxnManager*> txn_registry;
// static std::mutex txn_registry_mutex;

// Assumes 1 thread does sequencer work
void Sequencer::process_ack(Message * msg, uint64_t thd_id) {
	qlite_ll * en = wl_head;
	uint64_t batch_id = msg->original_batch_id == INVALID_ID ? msg->get_batch_id() : msg->original_batch_id;
	// uint64_t batch_id = msg->get_batch_id();
	while(en != NULL && en->epoch != batch_id) {
		en = en->next;
	}
	assert(en);
	qlite * wait_list = en->list;
	assert(wait_list != NULL);
	assert(en->txns_left > 0);

#if CC_ALG == HDCC || CC_ALG == SNAPPER
	uint64_t id = (msg->get_txn_id() - en->start_txn_id) / g_node_cnt;
#else
#if LONG_TXN_WORKLOAD && LONG_TXN_SPLIT
	uint64_t id = 0;
	if (msg->original_txn_id != INVALID_ID) {
		id = msg->original_txn_id / g_node_cnt;
	} else {
		id = msg->get_txn_id() / g_node_cnt;
	}
#else
	uint64_t id = msg->get_txn_id() / g_node_cnt;
#endif
#endif

	uint64_t prof_stat = get_sys_clock();
	assert(wait_list[id].server_ack_cnt > 0);

	// Decrement the number of acks needed for this txn
	uint32_t query_acks_left = ATOM_SUB_FETCH(wait_list[id].server_ack_cnt, 1);
	// 打印目前事务还差多少ack
	DEBUG_SEQ("Sequencer::process_ack() txn=[%ld-%ld] id=%ld original_txn=[%ld-%ld] ack_left=%d\n",
			 batch_id,msg->get_txn_id(), id, msg->original_batch_id,msg->original_txn_id, query_acks_left);

	if (wait_list[id].skew_startts == 0) {
		wait_list[id].skew_startts = get_sys_clock();
	}

	if (query_acks_left == 0) {
		en->txns_left--;
		ATOM_FETCH_ADD(total_txns_finished,1);
		INC_STATS(thd_id,seq_txn_cnt,1);
		// free msg, queries
#if WORKLOAD == YCSB
		YCSBClientQueryMessage* cl_msg = (YCSBClientQueryMessage*)wait_list[id].msg;
#if CC_ALG == HDCC || CC_ALG == SNAPPER
		if (msg->algo == CALVIN) {
#endif
			for(uint64_t i = 0; i < cl_msg->requests.size(); i++) {
				DEBUG_M("Sequencer::process_ack() ycsb_request free\n");
				mem_allocator.free(cl_msg->requests[i],sizeof(ycsb_request));
			}
#if CC_ALG == HDCC || CC_ALG == SNAPPER
		}
#endif
#elif WORKLOAD == TPCC
			TPCCClientQueryMessage* cl_msg = (TPCCClientQueryMessage*)wait_list[id].msg;
#if CC_ALG == HDCC || CC_ALG == SNAPPER
		if(msg->algo == CALVIN){
			if(cl_msg->txn_type == TPCC_NEW_ORDER) {
				for(uint64_t i = 0; i < cl_msg->items.size(); i++) {
						DEBUG_M("Sequencer::process_ack() items free\n");
						mem_allocator.free(cl_msg->items[i],sizeof(Item_no));
				}
			}
		}
#elif CC_ALG==CALVIN
		if(cl_msg->txn_type == TPCC_NEW_ORDER) {
				for(uint64_t i = 0; i < cl_msg->items.size(); i++) {
						DEBUG_M("Sequencer::process_ack() items free\n");
						mem_allocator.free(cl_msg->items[i],sizeof(Item_no));
				}
		}
#endif
#elif WORKLOAD == PPS
		PPSClientQueryMessage* cl_msg = (PPSClientQueryMessage*)wait_list[id].msg;

#elif WORKLOAD == DA
		DAClientQueryMessage* cl_msg = (DAClientQueryMessage*)wait_list[id].msg;
#endif
#if WORKLOAD == PPS
		if (WORKLOAD == PPS && CC_ALG == CALVIN &&
			((cl_msg->txn_type == PPS_GETPARTBYSUPPLIER) ||
				(cl_msg->txn_type == PPS_GETPARTBYPRODUCT) || (cl_msg->txn_type == PPS_ORDERPRODUCT)) &&
			(cl_msg->recon || ((AckMessage *)msg)->rc == Abort)) {
				int abort_cnt = wait_list[id].abort_cnt;
				if (cl_msg->recon) {
					// Copy over part keys
					cl_msg->part_keys.copy( ((AckMessage*)msg)->part_keys);
					DEBUG("Finished RECON (%ld,%ld)\n",msg->get_txn_id(),msg->get_batch_id());
			} else {
					uint64_t timespan = get_sys_clock() - wait_list[id].seq_startts;
					if (warmup_done) {
						INC_STATS_ARR(0,start_abort_commit_latency, timespan);
					}
					cl_msg->part_keys.clear();
					DEBUG("Aborted (%ld,%ld)\n",msg->get_txn_id(),msg->get_batch_id());
					INC_STATS(0,total_txn_abort_cnt,1);
					abort_cnt++;
				}

				cl_msg->return_node_id = wait_list[id].client_id;
				wait_list[id].total_batch_time += en->batch_send_time - wait_list[id].seq_startts;
					// restart
				process_txn(cl_msg, thd_id, wait_list[id].seq_first_startts, wait_list[id].seq_startts,
									wait_list[id].total_batch_time, abort_cnt);
		} else {
#endif
			uint64_t curr_clock = get_sys_clock();
			uint64_t timespan = curr_clock - wait_list[id].seq_first_startts;
			uint64_t timespan2 = curr_clock - wait_list[id].seq_startts;
			uint64_t skew_timespan = get_sys_clock() - wait_list[id].skew_startts;
			wait_list[id].total_batch_time += en->batch_send_time - wait_list[id].seq_startts;
			if (warmup_done) {
				INC_STATS_ARR(0,first_start_commit_latency, timespan);
				INC_STATS_ARR(0,last_start_commit_latency, timespan2);
				INC_STATS_ARR(0,start_abort_commit_latency, timespan2);
			}
			if (wait_list[id].abort_cnt > 0) {
				INC_STATS(0,unique_txn_abort_cnt,1);
			}

			INC_STATS(0,lat_l_loc_msg_queue_time,wait_list[id].total_batch_time);
			INC_STATS(0,lat_l_loc_process_time,skew_timespan);

			INC_STATS(0,lat_short_work_queue_time,msg->lat_work_queue_time);
			INC_STATS(0,lat_short_msg_queue_time,msg->lat_msg_queue_time);
			INC_STATS(0,lat_short_cc_block_time,msg->lat_cc_block_time);
			INC_STATS(0,lat_short_cc_time,msg->lat_cc_time);
			INC_STATS(0,lat_short_process_time,msg->lat_process_time);

		if (msg->return_node_id != g_node_id) {
			/*
				if (msg->lat_network_time/BILLION > 1.0) {
						printf("%ld %d %ld -> %d: %f %f\n",msg->txn_id, msg->rtype,
					msg->return_node_id,g_node_id ,msg->lat_network_time/BILLION,
					msg->lat_other_time/BILLION);
				}
			*/
			INC_STATS(0,lat_short_network_time,msg->lat_network_time);
		}
		INC_STATS(0,lat_short_batch_time,wait_list[id].total_batch_time);

			PRINT_LATENCY("lat_l_seq %ld %ld %d %f %f %f\n", msg->get_txn_id(), msg->get_batch_id(),
										wait_list[id].abort_cnt, (double)timespan / BILLION,
										(double)skew_timespan / BILLION,
										(double)wait_list[id].total_batch_time / BILLION);

#if CC_ALG == HDCC || CC_ALG == SNAPPER
			if (msg->algo == CALVIN) {
#endif
				cl_msg->release();
#if CC_ALG == HDCC || CC_ALG == SNAPPER
			}
#endif

#if CC_ALG == HDCC || CC_ALG == SNAPPER
			if (msg->algo == CALVIN) {
#endif
			ClientResponseMessage *rsp_msg =
					(ClientResponseMessage *)Message::create_message(msg->get_txn_id(), CL_RSP);
					rsp_msg->client_startts = wait_list[id].client_startts;
					msg_queue.enqueue(thd_id,rsp_msg,wait_list[id].client_id);
#if CC_ALG == HDCC || CC_ALG == SNAPPER
			}
#endif
#if WORKLOAD == PPS
			}
#endif

		INC_STATS(thd_id,seq_complete_cnt,1);

		DEBUG_SEQ("FINISHED txn=[%ld,%ld] origin_txn=[%ld,%ld] in BATCH %ld, left txn %d\n", msg->get_batch_id(),msg->get_txn_id(),msg->original_batch_id,msg->original_txn_id, en->epoch,en->txns_left);
	}

	// If we have all acks for this batch, send qry responses to all clients
	if (en->txns_left == 0) {
		DEBUG_SEQ("FINISHED BATCH %ld\n",en->epoch);
		LIST_REMOVE_HT(en,wl_head,wl_tail);
#if CC_ALG == HDCC
		blocked = true;
		while(validationCount > 0) {}
#endif
		mem_allocator.free(en->list,sizeof(qlite) * en->max_size);
		mem_allocator.free(en,sizeof(qlite_ll));
#if CC_ALG == HDCC
		blocked = false;
#endif
	}
	INC_STATS(thd_id,seq_ack_time,get_sys_clock() - prof_stat);
}

void Sequencer::process_abort(Message *msg, uint64_t thd_id) {
	qlite_ll * en = wl_head;
	uint64_t batch_id = msg->original_batch_id == INVALID_ID ? msg->get_batch_id() : msg->original_batch_id;
	// uint64_t batch_id = msg->get_batch_id();
	while(en != NULL && en->epoch != batch_id) {
		en = en->next;
	}
	assert(en);
	qlite * wait_list = en->list;
	assert(wait_list != NULL);
	assert(en->txns_left > 0);

#if CC_ALG == HDCC
	uint64_t id = (msg->get_txn_id() - en->start_txn_id) / g_node_cnt;
#else
#if LONG_TXN_WORKLOAD && LONG_TXN_SPLIT
	uint64_t id = 0;
	if (msg->original_txn_id != INVALID_ID) {
		id = msg->original_txn_id / g_node_cnt;
	} else {
		id = msg->get_txn_id() / g_node_cnt;
	}
#else
	uint64_t id = msg->get_txn_id() / g_node_cnt;
#endif
#endif
	// recover "return node id"
	msg->return_node_id = wait_list[id].client_id;


	uint64_t prof_stat = get_sys_clock();
	assert(wait_list[id].server_ack_cnt > 0);

	en->txns_left--;
	if (en->txns_left == 0) {
		DEBUG("FINISHED BATCH %ld\n",en->epoch);
		LIST_REMOVE_HT(en,wl_head,wl_tail);
#if CC_ALG == HDCC
		blocked = true;
		while(validationCount > 0) {}
#endif
		mem_allocator.free(en->list, sizeof(qlite) * en->max_size);
		mem_allocator.free(en, sizeof(qlite_ll));
#if CC_ALG == HDCC
		blocked = false;
#endif
	}
	INC_STATS(thd_id, seq_ack_time, get_sys_clock() - prof_stat);
	
	// set it to a new client query
	msg->rtype = CL_QRY;
	process_txn(msg, thd_id, 0, 0, 0, 0);
}

// Assumes 1 thread does sequencer work
void Sequencer::process_txn(Message *msg, uint64_t thd_id, uint64_t early_start,
														uint64_t last_start, uint64_t wait_time, uint32_t abort_cnt) {

	uint64_t starttime = get_sys_clock();
	DEBUG("SEQ Processing msg\n");
	qlite_ll * en = wl_tail;

	// LL is potentially a bottleneck here
	if(!en || en->epoch != simulation->get_seq_epoch()+1) {
		DEBUG("SEQ new wait list for epoch %ld\n",simulation->get_seq_epoch()+1);
		// First txn of new wait list
		en = (qlite_ll *) mem_allocator.alloc(sizeof(qlite_ll));
		en->epoch = simulation->get_seq_epoch()+1;
		en->max_size = 1000;
		en->size = 0;
		en->txns_left = 0;
		en->list = (qlite *) mem_allocator.alloc(sizeof(qlite) * en->max_size);
#if CC_ALG == HDCC || CC_ALG == SNAPPER
		en->start_txn_id = g_node_id + g_node_cnt * next_txn_id;
#endif
		LIST_PUT_TAIL(wl_head,wl_tail,en)
	}
	if(en->size == en->max_size) {
		en->max_size *= 2;
		en->list = (qlite *) mem_allocator.realloc(en->list,sizeof(qlite) * en->max_size);
	}

	txnid_t txn_id = g_node_id + g_node_cnt * next_txn_id;
#if CC_ALG == HDCC || CC_ALG == SNAPPER
	uint64_t id = next_txn_id - last_epoch_max_id;
	next_txn_id++;
	if (id >= en->max_size) {
		en->max_size *= 2;
		en->list = (qlite *) mem_allocator.realloc(en->list,sizeof(qlite) * en->max_size);
	}
#else
	next_txn_id++;
	uint64_t id = txn_id / g_node_cnt;
#endif
	msg->batch_id = en->epoch;
	msg->txn_id = txn_id;
	assert(txn_id != UINT64_MAX);

	// Register this message with Manager now that Sequencer assigned a real txn id
	{
		uint64_t reg_key = get_calvin_key(msg->get_batch_id(), msg->get_return_id(), msg->get_txn_id());
		Manager::register_txn_message(reg_key, msg);
	}

	// 如果是被拆分的子事务，就需要记录original_txn_id和original_batch_id
	if (msg->rtype == CL_QRY && ((ClientQueryMessage*)msg)->parent_marker != INVALID_ID) {
		auto it = parent_first_child_map.find(((ClientQueryMessage*)msg)->parent_marker);
		if (it == parent_first_child_map.end()) {
			// first child we see for this parent
			parent_first_child_map[((ClientQueryMessage*)msg)->parent_marker] = std::make_pair(msg->batch_id, msg->get_txn_id());
			msg->original_batch_id = msg->batch_id;
			msg->original_txn_id = msg->get_txn_id();
		} else {
			msg->original_batch_id = it->second.first;
			msg->original_txn_id = it->second.second;
		}

		id = msg->original_txn_id / g_node_cnt;
		DEBUG_SEQ("Sequencer::process_txn() child msg %p txn=[%ld-%ld] parent_marker %ld original_txn=[%ld-%ld]\n",
				msg, 
				msg->get_batch_id(),
				msg->get_txn_id(),
				((ClientQueryMessage*)msg)->parent_marker,
				msg->original_batch_id,
				msg->original_txn_id);
	} 

#if LONG_TXN_WORKLOAD
	if (id >= en->max_size) {
		en->max_size *= 2;
		en->list = (qlite *) mem_allocator.realloc(en->list,sizeof(qlite) * en->max_size);
	}
#endif

#if CC_ALG == HDCC
	if (cc_selector.get_best_cc(msg) == SILO) {
		msg->algo = SILO;
		if(msg->rtype == RTXN){
			msg->txn_id = msg->orig_txn_id;
			msg->batch_id = msg->orig_batch_id;
		}
		work_queue.enqueue(thd_id, msg, false);
		return;
	} else {
		msg->algo = CALVIN;
		msg->rtype = CL_QRY;
	}
#elif CC_ALG == SNAPPER
	if (cc_selector.get_best_cc(msg) == WAIT_DIE) {
		msg->algo = WAIT_DIE;
		if(msg->rtype == RTXN){
			msg->txn_id = msg->orig_txn_id;
			msg->batch_id = msg->orig_batch_id;
		}
		work_queue.enqueue(thd_id, msg, false);
		return;
	} else {
		msg->algo = CALVIN;
		msg->rtype = CL_QRY;
	}
#endif

#if WORKLOAD == YCSB
	std::set<uint64_t> participants = YCSBQuery::participants(msg,_wl);
#elif WORKLOAD == TPCC
	std::set<uint64_t> participants = TPCCQuery::participants(msg,_wl);
#elif WORKLOAD == PPS
	std::set<uint64_t> participants = PPSQuery::participants(msg,_wl);
#elif WORKLOAD == DA
	std::set<uint64_t> participants = DAQuery::participants(msg,_wl);
#endif
#if LONG_TXN_WORKLOAD && LONG_TXN_SPLIT
		uint32_t server_ack_cnt = 0;
	#if WORKLOAD == YCSB
		YCSBClientQueryMessage * cl_msg = (YCSBClientQueryMessage *) msg;
		if (cl_msg->parent_marker == INVALID_ID) {
			// Unsplittable full message or already a child produced by reorder
			server_ack_cnt = participants.size();
		} else {
			server_ack_cnt = cl_msg->sub_reqs_size;
		}
	#elif WORKLOAD == TPCC
	#else
	#endif
#else
	uint32_t server_ack_cnt = participants.size();
#endif
	assert(server_ack_cnt > 0);
	assert(ISCLIENTN(msg->get_return_id()) || 
			(ISCLIENTN(msg->origin_return_node_id) && msg->original_txn_id != INVALID_ID));

	#if LONG_TXN_SPLIT
	if ((msg->original_batch_id != INVALID_ID && msg->original_batch_id != en->epoch) ||
		// 如果不在同一个batch中，就不处理
		(msg->original_batch_id == en->epoch && msg->original_txn_id != INVALID_ID && en->list[id].msg == ((ClientQueryMessage*)msg)->parent_msg)
		// 如果在同一个batch中，是子事务，但已经被添加过，就不处理
	) {
		// 不需要重复添加
		// Note: Modifying msg!
		msg->return_node_id = g_node_id;
		msg->lat_network_time = 0;
		msg->lat_other_time = 0;

		DEBUG_SEQ("INSERT !SKIP! txn=[%ld-%ld] origin_txn=[%ld-%ld] from BATCH %ld, because its BATCH is %ld; en->list[%ld].msg=%p, orig_msg=%p\n", msg->get_batch_id(),msg->get_txn_id(),msg->original_batch_id,msg->original_txn_id, en->epoch, msg->original_batch_id,id,en->list[id].msg, ((ClientQueryMessage*)msg)->parent_msg);

		for(auto participant = participants.begin(); participant != participants.end(); participant++) {
			DEBUG("SEQ adding (%ld,%ld) to fill queue (recon: %d)\n", msg->get_txn_id(),
				msg->get_batch_id(), ((PPSClientQueryMessage *)msg)->recon);
			while (!fill_queue[*participant].push(msg) && !simulation->is_done()) {
			}
		}
		#if LOGGING
			char * data = (char *)malloc(sizeof(char) * 10);
			logger.writeToBuffer(thd_id, data, sizeof(data));
		#endif
		// 一些统计信息
		INC_STATS(thd_id,seq_process_cnt,1);
		INC_STATS(thd_id,seq_process_time,get_sys_clock() - starttime);
		ATOM_ADD(total_txns_received,1);
		return;
	} else {
		// 如果是被拆分的子事务，使用original_txn_id和original_batch_id
		if (msg->rtype == CL_QRY && ((ClientQueryMessage*)msg)->parent_marker != INVALID_ID) {
			// 如果是reorder产生的子事务，使用origin_return_node_id
			assert(msg->original_batch_id == en->epoch);
			en->list[id].client_id = msg->origin_return_node_id;
			en->list[id].msg = ((ClientQueryMessage*)msg)->parent_msg;
		} else {
			// 正常的事务，使用return_id
			en->list[id].client_id = msg->get_return_id();
			en->list[id].msg = msg;
		}
	} 
	#else
		en->list[id].client_id = msg->get_return_id();
		en->list[id].msg = msg;
	#endif
	en->list[id].client_startts = ((ClientQueryMessage*)msg)->client_startts;
	//en->list[id].seq_startts = get_sys_clock();

	en->list[id].total_batch_time = wait_time;
	en->list[id].abort_cnt = abort_cnt;
	en->list[id].skew_startts = 0;
	en->list[id].server_ack_cnt = server_ack_cnt;
	en->size++;
	en->txns_left++;
	
	
	// Note: Modifying msg!
	msg->return_node_id = g_node_id;
	msg->lat_network_time = 0;
	msg->lat_other_time = 0;
#if CC_ALG == CALVIN && WORKLOAD == PPS
	PPSClientQueryMessage* cl_msg = (PPSClientQueryMessage*) msg;
	if (cl_msg->txn_type == PPS_GETPARTBYSUPPLIER || cl_msg->txn_type == PPS_GETPARTBYPRODUCT ||
					cl_msg->txn_type == PPS_ORDERPRODUCT) {
			if (cl_msg->part_keys.size() == 0) {
					cl_msg->recon = true;
					en->list[id].seq_startts = get_sys_clock();
		} else {
					cl_msg->recon = false;
					en->list[id].seq_startts = last_time;
			}

	} else {
			cl_msg->recon = false;
			en->list[id].seq_startts = get_sys_clock();
	}
#else
	en->list[id].seq_startts = get_sys_clock();
#endif
	if (early_start == 0) {
		en->list[id].seq_first_startts = en->list[id].seq_startts;
	} else {
		en->list[id].seq_first_startts = early_start;
	}
	assert(en->size == en->txns_left);
	assert(en->size <= ((uint64_t)g_inflight_max * g_node_cnt));

	DEBUG_SEQ("INSERT txn=[%ld,%ld] origin_txn=[%ld,%ld] in BATCH %ld, left txn %d\n", msg->get_batch_id(),msg->get_txn_id(),msg->original_batch_id,msg->original_txn_id, en->epoch,en->txns_left);

	for(auto participant = participants.begin(); participant != participants.end(); participant++) {
		// DEBUG("SEQ adding (%ld,%ld) to fill queue (recon: %d)\n", msg->get_txn_id(),
			// msg->get_batch_id(), ((PPSClientQueryMessage *)msg)->recon);
		DEBUG("SEQ adding (%ld,%ld) to fill queue\n", msg->get_txn_id(),
			msg->get_batch_id());
		while (!fill_queue[*participant].push(msg) && !simulation->is_done()) {
		}
	}

#if LOGGING
	char * data = (char *)malloc(sizeof(char) * 10);
	logger.writeToBuffer(thd_id, data, sizeof(data));
#endif

	INC_STATS(thd_id,seq_process_cnt,1);
	INC_STATS(thd_id,seq_process_time,get_sys_clock() - starttime);
	ATOM_ADD(total_txns_received,1);
}

// Assumes 1 thread does sequencer work
void Sequencer::send_next_batch(uint64_t thd_id) {
	uint64_t prof_stat = get_sys_clock();
	qlite_ll * en = wl_tail;
#if LOGGING
#if CC_ALG == HDCC
	logger.enqueueRecord(logger.createRecord(thd_id, L_C_FLUSH, 0, 0, 0));
#else
	logger.enqueueRecord(logger.createRecord(thd_id, L_C_FLUSH, 0, 0));
#endif
#endif
	bool empty = true;
	if(en && en->epoch == simulation->get_seq_epoch()) {
		DEBUG("SEND NEXT BATCH %ld [%ld,%ld] %ld\n", thd_id, simulation->get_seq_epoch(), en->epoch,
					en->size);
#if CC_ALG == HDCC || CC_ALG == SNAPPER
		if (en->txns_left == 0) {
			DEBUG("FINISHED BATCH %ld\n",en->epoch);
			LIST_REMOVE_HT(en,wl_head,wl_tail);
			mem_allocator.free(en->list,sizeof(qlite) * en->max_size);
			mem_allocator.free(en,sizeof(qlite_ll));
		}else{
#endif
			empty = false;
			en->batch_send_time = prof_stat;
#if CC_ALG == HDCC || CC_ALG == SNAPPER
		}
#endif
	}

	Message * msg;
	for(uint64_t j = 0; j < g_node_cnt; j++) {
		while(fill_queue[j].pop(msg)) {
			if(j == g_node_id) {
				work_queue.sched_enqueue(thd_id,msg);
			} else {
				msg_queue.enqueue(thd_id,msg,j);
			}
		}
		if(!empty) {
			DEBUG("Seq RDONE %ld\n",simulation->get_seq_epoch())
		}
		msg = Message::create_message(RDONE);
		msg->batch_id = simulation->get_seq_epoch();
		if(j == g_node_id) {
			work_queue.sched_enqueue(thd_id,msg);
		} else {
			msg_queue.enqueue(thd_id,msg,j);
		}
	}

	if(last_time_batch > 0) {
		INC_STATS(thd_id,seq_batch_time,get_sys_clock() - last_time_batch);
	}
	last_time_batch = get_sys_clock();

	INC_STATS(thd_id,seq_batch_cnt,1);
	if(!empty) {
		INC_STATS(thd_id,seq_full_batch_cnt,1);
	}
	INC_STATS(thd_id,seq_prep_time,get_sys_clock() - prof_stat);
#if CC_ALG == CALVIN
	next_txn_id = 0;
#elif CC_ALG == HDCC || CC_ALG == SNAPPER
	last_epoch_max_id = next_txn_id;
#endif
}

#if CC_ALG == HDCC
bool Sequencer::checkDependency(uint64_t batch_id, uint64_t txn_id) {
	qlite_ll * en = wl_head;
	if (!en || en->epoch > batch_id) {
		return true;
	}
	else if (en->epoch < batch_id) {
		return false;
	} else {
		if (txn_id % g_node_cnt < g_node_id) {
			return true;
		} else if (txn_id %g_node_cnt > g_node_id) {
			return false;
		} else {
			uint64_t id = (txn_id - en->start_txn_id) / g_node_cnt;
			while(blocked) {}
			ATOM_ADD(validationCount, 1);
			if (!en || !en->list || en->txns_left == 0) {
				ATOM_SUB(validationCount, 1);
				return true;
			}
			for (uint64_t i = 0; i < id || i < en->max_size; i++) {
				if (en->list[i].server_ack_cnt > 0) {
					ATOM_SUB(validationCount, 1);
					return false;
				}
			}
			ATOM_SUB(validationCount, 1);
		}
	}
	return true;
}
#endif

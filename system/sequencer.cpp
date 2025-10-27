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
#if CC_ALG == HDCC || CC_ALG == SNAPPER || LONG_TXN_SORT
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

#if LONG_TXN_WORKLOAD && LONG_TXN_SORT
	// 当前批次内的事务列表
	current_batch.clear();
#endif
	
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
	while(en != NULL && en->epoch != msg->get_batch_id()) {
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
	if (msg->original_txn_id != UINT64_MAX) {
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
	// DEBUG_SEQ("Sequencer::process_ack() txn_id=%ld batch_id=%ld id=%ld original_txn=%ld ack_left=%d\n",
	// 		msg->get_txn_id(), msg->get_batch_id(), id, msg->original_txn_id, query_acks_left);

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

	}

	// If we have all acks for this batch, send qry responses to all clients
	if (en->txns_left == 0) {
			DEBUG("FINISHED BATCH %ld\n",en->epoch);
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
	while(en != NULL && en->epoch != msg->get_batch_id()) {
		en = en->next;
	}
	assert(en);
	qlite * wait_list = en->list;
	assert(wait_list != NULL);
	assert(en->txns_left > 0);

#if CC_ALG == HDCC
	uint64_t id = (msg->get_txn_id() - en->start_txn_id) / g_node_cnt;
#else
	uint64_t id = msg->get_txn_id() / g_node_cnt;
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
		if (cl_msg->steps.empty()) {
			server_ack_cnt = participants.size();
		} else {
			for (uint64_t i = 0; i < cl_msg->steps.size(); i++) {
				if (cl_msg->sub_reqs[i].size() > 0) {
					server_ack_cnt ++;
				}
			}
		}
	#elif WORKLOAD == TPCC

	#else
	#endif
#else
		uint32_t server_ack_cnt = participants.size();
#endif
		assert(server_ack_cnt > 0);
		assert(ISCLIENTN(msg->get_return_id()));
		en->list[id].client_id = msg->get_return_id();
		en->list[id].client_startts = ((ClientQueryMessage*)msg)->client_startts;
		//en->list[id].seq_startts = get_sys_clock();

		en->list[id].total_batch_time = wait_time;
		en->list[id].abort_cnt = abort_cnt;
		en->list[id].skew_startts = 0;
		en->list[id].server_ack_cnt = server_ack_cnt;
		en->list[id].msg = msg;
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

#if LONG_TXN_WORKLOAD && LONG_TXN_SPLIT
		cl_msg->original_txn_id = UINT64_MAX;
		cl_msg->deps_left.store(0);
#endif

#if LONG_TXN_WORKLOAD && LONG_TXN_SORT
		// 如果用了事务重排序，整个batch中的事务号重排序后，都比排序前大
		// 比如说原来分配的1-10，重排序后就是11-20.
		// 因此记录原始事务ID，方便后续追踪
		cl_msg->original_txn_id = txn_id;
#endif

#if LONG_TXN_WORKLOAD && LONG_TXN_SPLIT
	#if WORKLOAD == YCSB
		if (cl_msg->steps.empty()) {
			#if LONG_TXN_WORKLOAD && LONG_TXN_SORT
			// If LONG_TXN_SORT is enabled we collect subtransactions into current_batch
			// for later reordering/dispatch instead of immediately pushing them into
			// the per-node fill_queue.
			current_batch.push_back((Message*)msg);
			#else
			for(auto participant = participants.begin(); participant != participants.end(); participant++) {
				while (!fill_queue[*participant].push(msg) && !simulation->is_done()) {
				}
			}
			#endif
		} else {
			// 记录原始事务ID，方便后续追踪
			cl_msg->original_txn_id = cl_msg->txn_id;
			// !First pass: create all sub-messages and record them by original step index.
			// This avoids relying on integer indices (which would break if messages are
			// reordered) and allows us to link dependencies by Message* pointers.
			vector<YCSBClientQueryMessage*> created_by_step;
			created_by_step.resize(cl_msg->steps.size(), nullptr);
			for (uint64_t i = 0; i < cl_msg->steps.size(); i++) {
				// 如果当前步骤没有请求，跳过
				if (cl_msg->sub_reqs[i].size() == 0) continue;
				// 创建一个新的消息，代表一个子事务 (但暂不入队)
				YCSBClientQueryMessage * new_msg = (YCSBClientQueryMessage *)Message::create_message(CL_QRY);
				// Register child message in global registry so parents can notify it even if its TxnManager
				// hasn't been created yet.
				// 继承原事务的ID和批次号
				new_msg->original_txn_id = cl_msg->txn_id;
				new_msg->batch_id = cl_msg->batch_id;
				new_msg->rtype = CL_QRY;
				// new_msg->return_node_id = cl_msg->return_node_id;

				// 设置当前子事务还没完成
				new_msg->isDone = false;
				// 初始化依赖计数为0（稍后在第二遍会设置为实际依赖数）
				new_msg->deps_left.store(0);

				// 继承 steps 信息（用于表示读/写级别）
				new_msg->steps = vector<uint64_t>(1, cl_msg->steps[i]);

				// 为新子事务分配唯一的txn_id
				txnid_t txn_id = g_node_id + g_node_cnt * next_txn_id;
				next_txn_id++;
				new_msg->txn_id = txn_id;
				#if LONG_TXN_WORKLOAD && LONG_TXN_SORT
				new_msg->original_sub_txn_id = txn_id;
				#endif

				// 设置返回节点和延迟信息
				new_msg->return_node_id = g_node_id;
				new_msg->lat_network_time = 0;
				new_msg->lat_other_time = 0;
				// 初始化请求数组，分配空间
				new_msg->requests.init(cl_msg->sub_reqs[i].size());
				new_msg->requests.init(g_req_per_query);
				// 把当前步骤的所有请求加入新消息
				for (uint64_t j = 0; j < cl_msg->sub_reqs[i].size(); j++) {
					new_msg->requests.add(cl_msg->sub_reqs[i][j]);
				}
				// 缓存到按原始步骤索引的数组，稍后再建立依赖并入队
				created_by_step[i] = new_msg;

				uint64_t key = get_calvin_key(new_msg->get_batch_id(), new_msg->return_node_id, new_msg->get_txn_id());
				Manager::register_txn_message(key, new_msg);
			}

			// !Second pass: link dependencies by Message* pointers. For any write step
			// (steps==2) make it depend on all read steps (steps==1) that actually
			// produced sub-messages.
			for (uint64_t i = 0; i < cl_msg->steps.size(); i++) {
				YCSBClientQueryMessage * cur = created_by_step[i];
				if (!cur) continue;
				// 对于每一个依赖事务
				if (cl_msg->steps[i] == 2) {
					int depcount = 0;
					for (uint64_t k = 0; k < cl_msg->steps.size(); k++) {
						// 检查被依赖事务
						if (cl_msg->steps[k] == 1 && created_by_step[k] != nullptr) {
							uint64_t parent_id = created_by_step[k]->get_txn_id();
							depcount++;
							{
								pthread_mutex_lock(&created_by_step[k]->dependents_lock);
								uint64_t key = get_calvin_key(cur->get_batch_id(), cur->return_node_id, cur->get_txn_id());
								created_by_step[k]->dependents_ids.push_back(key);
								pthread_mutex_unlock(&created_by_step[k]->dependents_lock);
							}
							cl_msg->depends_on_messages.push_back(created_by_step[k]);
						}
					}
					cur->deps_left.store(depcount);
				}
			}

			// !Third pass: now that dependencies are set, compute participants and enqueue.
			// When LONG_TXN_SORT is enabled we collect subtransactions, also build a
			// comma-separated list of their txn_ids for debugging/tracing.
			std::string split_ids;
			for (uint64_t i = 0; i < cl_msg->steps.size(); i++) {
				YCSBClientQueryMessage * new_msg = created_by_step[i];
				if (!new_msg) continue;
				std::set<uint64_t> participants = YCSBQuery::participants(new_msg, _wl);
				if (!split_ids.empty()) split_ids += ",";
				split_ids += std::to_string(new_msg->get_txn_id());
				#if LONG_TXN_WORKLOAD && LONG_TXN_SORT
				// collect into current_batch for later reordering/dispatch
				current_batch.push_back((Message*)new_msg);
				// append txn id to split_ids
				#else
				// 在塞到fill_queue之前，注册到全局msg_registry
				for(auto participant = participants.begin(); participant != participants.end(); participant++) {
					while (!fill_queue[*participant].push(new_msg) && !simulation->is_done()) {
					}
				}
				#endif
			}
			// #if LONG_TXN_WORKLOAD && LONG_TXN_SORT
			if (!split_ids.empty()) {
				// DEBUG_SEQ("SEQ split txn (%ld,%ld) child txns: %s\n", cl_msg->batch_id, cl_msg->txn_id, split_ids.c_str());
			}
			// #endif
		}
	#elif WORKLOAD == TPCC

	#else

	#endif
#else
		// Add new txn to fill queue
		#if LONG_TXN_WORKLOAD && LONG_TXN_SORT
		// If LONG_TXN_SORT is enabled we collect subtransactions into current_batch
		// for later reordering/dispatch instead of immediately pushing them into
		// the per-node fill_queue.
		current_batch.push_back((Message*)msg);
		#else
		for(auto participant = participants.begin(); participant != participants.end(); participant++) {
			DEBUG("SEQ adding (%ld,%ld) to fill queue (recon: %d)\n", msg->get_txn_id(),
				msg->get_batch_id(), ((PPSClientQueryMessage *)msg)->recon);
			while (!fill_queue[*participant].push(msg) && !simulation->is_done()) {
			}
		}
		#endif
#endif

#if LOGGING
		char * data = (char *)malloc(sizeof(char) * 10);
		logger.writeToBuffer(thd_id, data, sizeof(data));
#endif

	INC_STATS(thd_id,seq_process_cnt,1);
	INC_STATS(thd_id,seq_process_time,get_sys_clock() - starttime);
	ATOM_ADD(total_txns_received,1);
}

// 这里加一个调用reorder给要发的batch重排序的函数
void Sequencer::reorder_batch() {
	#if LONG_TXN_WORKLOAD && LONG_TXN_SORT
	// delta值是执行器和调度器的最大值
	int delta = g_scheduler_thread_cnt > g_thread_cnt ? g_scheduler_thread_cnt : g_thread_cnt;
	if (current_batch.empty()) return;

	std::vector<Message*> reorder_batch = Reorder::schedule_transactions_advanced(current_batch,delta,LAMBDA_FACTOR,-1);
	// 测试是reorder的问题，还是下面重分配事务号的问题
	// std::vector<Message*> reorder_batch = current_batch;

	// 重新分配事务号，以保证事务顺序确定
	for (size_t i = 0; i < reorder_batch.size(); i++) {
		Message *m = reorder_batch[i];
		txnid_t txn_id = g_node_id + g_node_cnt * next_txn_id;
		next_txn_id++;
		m->txn_id = txn_id;
	}	

	// Now push reordered messages into the per-node fill_queue.
	for (auto & msg : reorder_batch) {
		std::set<uint64_t> participants;
		#if WORKLOAD == YCSB
			participants = YCSBQuery::participants(msg,_wl);
		#elif WORKLOAD == TPCC
			participants = TPCCQuery::participants(msg,_wl);
		#elif WORKLOAD == PPS
			participants = PPSQuery::participants(msg,_wl);
		#elif WORKLOAD == DA
			participants = DAQuery::participants(msg,_wl);
		#endif
		for(auto participant = participants.begin(); participant != participants.end(); participant++) {
			while (!fill_queue[*participant].push(msg) && !simulation->is_done()) {}
		}
	}

	// We've dispatched the current batch, clear the collector so it can be
	// reused for the next epoch.
	current_batch.clear();
	#endif
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

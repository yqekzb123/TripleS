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
#include "sdocc_sequencer.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
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
#include "water_mark.h"

// Assumes 1 thread does sequencer work
void SDOCCSequencer::process_ack(Message * msg, uint64_t thd_id) {
	qlite_ll * en = wl_head;
	uint64_t batch_id = msg->get_batch_id();

	// 先检查是哪个batch的ack，找到对应的等待列表
	while(en != NULL && en->epoch != batch_id) {
		en = en->next;
	}
	assert(en);
	// 找到对应的等待列表，检查这个ack是哪个事务的，并且更新等待列表
	qlite * wait_list = en->list;
	assert(wait_list != NULL);
	assert(en->txns_left > 0);

	uint64_t id = msg->get_txn_id() / g_node_cnt;
	uint64_t prof_stat = get_sys_clock();
	assert(wait_list[id].server_ack_cnt > 0);

	// 对于SDOCC来说，每个事务只需要本地服务器ACK就行了，所以每收到一个ACK就把这个事务的ACK计数减1，看看这个事务是不是已经收到了所有ACK了
	uint32_t query_acks_left = ATOM_SUB_FETCH(wait_list[id].server_ack_cnt, 1);

	if (wait_list[id].skew_startts == 0) {
		wait_list[id].skew_startts = get_sys_clock();
	}
	// 如果这个事务已经收到了所有ACK了，就把这个事务从等待列表里移除，并且看看这个batch里还有没有其他事务，如果没有了就把这个batch也移除掉
	if (query_acks_left == 0) {
		en->txns_left--;
		ATOM_FETCH_ADD(total_txns_finished,1);
		INC_STATS(thd_id,seq_txn_cnt,1);
		// free msg, queries
		#if WORKLOAD == YCSB
			YCSBClientQueryMessage* cl_msg = (YCSBClientQueryMessage*)wait_list[id].msg;
			for(uint64_t i = 0; i < cl_msg->requests.size(); i++) {
				DEBUG_M("SDOCCSequencer::process_ack() ycsb_request free\n");
				mem_allocator.free(cl_msg->requests[i],sizeof(ycsb_request));
			}
		#elif WORKLOAD == TPCC
			TPCCClientQueryMessage* cl_msg = (TPCCClientQueryMessage*)wait_list[id].msg;
			if(cl_msg->txn_type == TPCC_NEW_ORDER) {
				for(uint64_t i = 0; i < cl_msg->items.size(); i++) {
						DEBUG_M("SDOCCSequencer::process_ack() items free\n");
						mem_allocator.free(cl_msg->items[i],sizeof(Item_no));
				}
			}
		#endif
		// 下面都是统计信息的更新，主要是事务从开始到现在的各种时间统计
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
			// 对于SDOCC来说，事务只需要本地服务器ACK就行了，所以如果收到的ACK的return_node_id不是本地服务器的话就是不对的，直接assert失败
			assert(false);
			INC_STATS(0,lat_short_network_time,msg->lat_network_time);
		}
		INC_STATS(0,lat_short_batch_time,wait_list[id].total_batch_time);

		PRINT_LATENCY("lat_l_seq %ld %ld %d %f %f %f\n", msg->get_txn_id(), msg->get_batch_id(),
					wait_list[id].abort_cnt, (double)timespan / BILLION,
					(double)skew_timespan / BILLION,
					(double)wait_list[id].total_batch_time / BILLION);
		cl_msg->release();

		ClientResponseMessage *rsp_msg =
				(ClientResponseMessage *)Message::create_message(msg->get_txn_id(), CL_RSP);
				rsp_msg->client_startts = wait_list[id].client_startts;
				msg_queue.enqueue(thd_id,rsp_msg,wait_list[id].client_id);

		INC_STATS(thd_id,seq_complete_cnt,1);

		DEBUG_SEQ("FINISHED txn=[%ld,%ld] in BATCH %ld, left txn %d\n", msg->get_batch_id(),msg->get_txn_id(),en->epoch,en->txns_left);
	}

	// 如果这个batch里的所有事务都已经收到了ACK了，就把这个batch从等待列表里移除掉，并且给这个batch里所有事务的客户端发送响应消息
	if (en->txns_left == 0) {
		DEBUG_SEQ("FINISHED BATCH %ld\n",en->epoch);
		LIST_REMOVE_HT(en,wl_head,wl_tail);
		mem_allocator.free(en->list,sizeof(qlite) * en->max_size);
		mem_allocator.free(en,sizeof(qlite_ll));
	}
	INC_STATS(thd_id,seq_ack_time,get_sys_clock() - prof_stat);
}

// !这块代码不确定，后续重试可能不能走这一段
void SDOCCSequencer::process_abort(Message *msg, uint64_t thd_id) {
	qlite_ll * en = wl_head;
	uint64_t batch_id = msg->get_batch_id();
	// uint64_t batch_id = msg->get_batch_id();
	while(en != NULL && en->epoch != batch_id) {
		en = en->next;
	}
	assert(en);
	qlite * wait_list = en->list;
	assert(wait_list != NULL);
	assert(en->txns_left > 0);

	uint64_t id = msg->get_txn_id() / g_node_cnt;
	// recover "return node id"
	msg->return_node_id = wait_list[id].client_id;


	uint64_t prof_stat = get_sys_clock();
	assert(wait_list[id].server_ack_cnt > 0);

	en->txns_left--;
	if (en->txns_left == 0) {
		DEBUG("FINISHED BATCH %ld\n",en->epoch);
		LIST_REMOVE_HT(en,wl_head,wl_tail);
		mem_allocator.free(en->list, sizeof(qlite) * en->max_size);
		mem_allocator.free(en, sizeof(qlite_ll));
	}
	INC_STATS(thd_id, seq_ack_time, get_sys_clock() - prof_stat);
	
	// set it to a new client query
	msg->rtype = CL_QRY;
	process_txn(msg, thd_id, 0, 0, 0, 0);
}

void SDOCCSequencer::process_txn(Message *msg, uint64_t thd_id, uint64_t early_start,
														uint64_t last_start, uint64_t wait_time, uint32_t abort_cnt) {

	uint64_t starttime = get_sys_clock();
	DEBUG("SEQ Processing msg\n");
	qlite_ll * en = wl_tail;

	// 检查是不是该换batch了
	if(!en || en->epoch != simulation->get_seq_epoch()+1) {
		DEBUG("SEQ new wait list for epoch %ld\n",simulation->get_seq_epoch()+1);
		// First txn of new wait list
		en = (qlite_ll *) mem_allocator.alloc(sizeof(qlite_ll));
		en->epoch = simulation->get_seq_epoch()+1;
		en->max_size = 1000;
		en->size = 0;
		en->txns_left = 0;
		en->list = (qlite *) mem_allocator.alloc(sizeof(qlite) * en->max_size);
		LIST_PUT_TAIL(wl_head,wl_tail,en)
	}
	if(en->size == en->max_size) {
		en->max_size *= 2;
		en->list = (qlite *) mem_allocator.realloc(en->list,sizeof(qlite) * en->max_size);
	}

	txnid_t txn_id = g_node_id + g_node_cnt * next_txn_id;
	next_txn_id++;
	uint64_t id = txn_id / g_node_cnt;

	msg->batch_id = en->epoch;
	msg->txn_id = txn_id;
	assert(txn_id != UINT64_MAX);

#if LONG_TXN_WORKLOAD
	if (id >= en->max_size) {
		en->max_size *= 2;
		en->list = (qlite *) mem_allocator.realloc(en->list,sizeof(qlite) * en->max_size);
	}
#endif

	// 构造当前的事务信息，记录在等待列表里
	assert(ISCLIENTN(msg->get_return_id()));
	en->list[id].client_id = msg->get_return_id();
	en->list[id].msg = msg;
	en->list[id].client_startts = ((ClientQueryMessage*)msg)->client_startts;
	en->list[id].total_batch_time = wait_time;
	en->list[id].abort_cnt = abort_cnt;
	en->list[id].skew_startts = 0;
	en->list[id].server_ack_cnt = 1; // SDOCC只需要本地服务器ACK
	en->list[id].msg = msg;
	en->size++;
	en->txns_left++;
	// Note: Modifying msg!
	msg->return_node_id = g_node_id;
	msg->lat_network_time = 0;
	msg->lat_other_time = 0;
	en->list[id].seq_startts = get_sys_clock();

	if (early_start == 0) {
		en->list[id].seq_first_startts = en->list[id].seq_startts;
	} else {
		en->list[id].seq_first_startts = early_start;
	}
	assert(en->size == en->txns_left);
	assert(en->size <= ((uint64_t)g_inflight_max * g_node_cnt));

	DEBUG_SEQ("INSERT txn=[%ld,%ld] in BATCH %ld, left txn %d\n", msg->get_batch_id(),msg->get_txn_id(), en->epoch,en->txns_left);
	
	#ifdef DEBUG_SEQUENCER
	check_participants(msg, _wl);
	#endif
	// 往fill_queue里塞事务，只需要考虑本地即可
	DEBUG("SEQ adding (%ld,%ld) to fill queue\n", msg->get_batch_id(),msg->get_txn_id());
	while (!fill_queue[g_node_id].push(msg) && !simulation->is_done()) {
	}

#if LOGGING
	char * data = (char *)malloc(sizeof(char) * 10);
	logger.writeToBuffer(thd_id, data, sizeof(data));
#endif

	INC_STATS(thd_id,seq_process_cnt,1);
	INC_STATS(thd_id,seq_process_time,get_sys_clock() - starttime);
	ATOM_ADD(total_txns_received,1);
}

// 对于SDOCC来说，收集到事务，只需要给本地发，不需要发给远程，远程操作由下面执行器来管理
void SDOCCSequencer::send_next_batch(uint64_t thd_id) {
	uint64_t prof_stat = get_sys_clock();
	qlite_ll * en = wl_tail;
#if LOGGING
	logger.enqueueRecord(logger.createRecord(thd_id, L_C_FLUSH, 0, 0));
#endif
	bool empty = true;
	if(en && en->epoch == simulation->get_seq_epoch()) {
		DEBUG_SEQ("SEND NEXT BATCH %ld [%ld,%ld] %ld\n", thd_id, simulation->get_seq_epoch(), en->epoch,
					en->size);
			empty = false;
			en->batch_send_time = prof_stat;
	}

	Message * msg;
	int j = g_node_id;
	while(fill_queue[j].pop(msg)) {
		msg->return_node_id = g_node_id;
		// 把事务塞到check_water_mark里面
		uint64_t key = get_calvin_key(msg->batch_id, msg->return_node_id, msg->txn_id);
        // min_commit_read_sid = std::max(min_commit_read_sid, key);
        watermark_node_entry* entry = (watermark_node_entry*)mem_allocator.align_alloc(sizeof(watermark_node_entry));
        entry->key = key;

        ListNode<watermark_node_entry*>* ld = check_water_mark->insert(entry, thd_id);
        DEBUG_SCH("check_water_mark save %ld for txn %ld,%ld.\n", key, msg->batch_id, msg->txn_id);
        // !发送事务到工作队列
        ((ClientQueryMessage*)msg)->list_node_pointer = ld;

		work_queue.enqueue(thd_id,msg,false);
	}
	if(!empty) {
		DEBUG_SEQ("Seq RDONE %ld\n",simulation->get_seq_epoch())
	}
	// 对于SDOCC来说，应该不需要发RDONE消息。
	#if 0
		msg = Message::create_message(RDONE);
		msg->batch_id = simulation->get_seq_epoch();
		if(j == g_node_id) {
			work_queue.sched_enqueue(thd_id,msg);
		} else {
			msg_queue.enqueue(thd_id,msg,j);
		}
	#endif
	

	if(last_time_batch > 0) {
		INC_STATS(thd_id,seq_batch_time,get_sys_clock() - last_time_batch);
	}
	last_time_batch = get_sys_clock();

	INC_STATS(thd_id,seq_batch_cnt,1);
	if(!empty) {
		INC_STATS(thd_id,seq_full_batch_cnt,1);
	}
	INC_STATS(thd_id,seq_prep_time,get_sys_clock() - prof_stat);
	next_txn_id = 0;
}

// 检查当前事务的参与者里是否有当前服务器，如果没有的话说明这个事务不应该由这个服务器来处理，可能是之前的事务被重试了，或者是之前的事务被错误地发到了这个服务器上了
void SDOCCSequencer::check_participants(Message * msg, Workload * wl) {
	// 帮我写一段，检查g_node_id是否在participants里的
	#if WORKLOAD == YCSB
		std::set<uint64_t> participants = YCSBQuery::participants(msg,_wl);
	#elif WORKLOAD == TPCC
		std::set<uint64_t> participants = TPCCQuery::participants(msg,_wl);
	#endif
	uint32_t server_ack_cnt = participants.size();
	assert(server_ack_cnt > 0);
	bool found = false;
	for (auto participant : participants) {
		if (participant == g_node_id) {
			found = true;
			break;
		}	
	}
	assert(found);
}
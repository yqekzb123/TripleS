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

#if CC_ALG == SDOCC
void SDOCCSequencer::init(Workload *wl) {
    next_txn_id = 0;
    batch_id = 0;
    last_batch_time = 0;
    txns_left = 0;
    _wl = wl;
    pipeline_batches.clear();
    pipeline_current_batch = nullptr;
    pipeline_next_batch_id = 0;
    pipeline_txns_left = 0;
    assert((uint32_t)g_inflight_max > g_aria_batch_size);
}

// Assumes 1 thread does sequencer work
void SDOCCSequencer::process_ack(Message * msg, uint64_t thd_id) {
	uint64_t starttime = get_sys_clock();
    uint64_t txn_id = msg->txn_id;
    uint64_t b_id = msg->batch_id;
    DEBUG_SCH("PIPELINE PROCESS ACK txn_id: %ld, b_id: %ld, rc: %d\n", txn_id, b_id, ((AckMessage *)msg)->rc);
    // 先根据b_id找到对应的PBatch

    PBatch * b = nullptr;
    int bi;
    for (bi = 0; bi < pipeline_batches.size(); ++bi) {
        PBatch * bb = pipeline_batches[bi];
        if (bb->id == b_id) {
            b = bb;
            break;
        }
    }
    if (!b) {
        assert(false);
        DEBUG_SCH("PIPELINE PROCESS ACK ERROR: batch id %ld not found for txn_id %ld\n", b_id, txn_id);
        return;
    }

	for (size_t i = 0; i < b->txns.size(); ++i) {
        if (b->txns[i]->msg->txn_id == txn_id) {
            assert(b->txns[i]->msg->batch_id == b_id);
            if (((AckMessage *)msg)->rc == RCOK) {
                INC_STATS(thd_id, seq_txn_cnt, 1);
            #if WORKLOAD == YCSB
                YCSBClientQueryMessage * cl_msg = (YCSBClientQueryMessage *)b->txns[i]->msg;
            #elif WORKLOAD == TPCC
                TPCCClientQueryMessage * cl_msg = (TPCCClientQueryMessage*)b->txns[i]->msg;
            #endif
                uint64_t curr_clock = get_sys_clock();
                uint64_t long_timespan = curr_clock - b->txns[i]->seq_first_startts;
                uint64_t short_timespan = curr_clock - b->txns[i]->seq_startts;
                if (warmup_done) {
                    INC_STATS_ARR(0, first_start_commit_latency, long_timespan);
                    INC_STATS_ARR(0, last_start_commit_latency, short_timespan);
                    INC_STATS_ARR(0, start_abort_commit_latency, short_timespan);
                }
                if (b->txns[i]->abort_cnt > 0) 
                    INC_STATS(0, unique_txn_abort_cnt, 1);
                INC_STATS(0, lat_l_loc_msg_queue_time, curr_clock - last_batch_time);
                INC_STATS(0, lat_short_work_queue_time, msg->lat_work_queue_time);
                INC_STATS(0, lat_short_msg_queue_time, msg->lat_msg_queue_time);
                INC_STATS(0, lat_short_cc_block_time, msg->lat_cc_block_time);
                INC_STATS(0, lat_short_cc_time, msg->lat_cc_time);
                INC_STATS(0, lat_short_process_time, msg->lat_process_time);
                if (msg->return_node_id != g_node_id) 
                    INC_STATS(0, lat_short_network_time, msg->lat_network_time);
                cl_msg->release();
                ClientResponseMessage * rsp_msg = (ClientResponseMessage *)Message::create_message(msg->get_txn_id(), CL_RSP);
                rsp_msg->client_startts = b->txns[i]->client_startts;
                msg_queue.enqueue(thd_id, rsp_msg, b->txns[i]->client_id);
                mem_allocator.free(b->txns[i], sizeof(sdocc_txn));
                b->txns.erase(b->txns.begin() + i);
                INC_STATS(thd_id, seq_complete_cnt, 1);
            } else {
                b->txns[i]->abort_cnt++; b->txns[i]->seq_startts = get_sys_clock();
            }
            if (b->txns_left > 0) b->txns_left--; 
            if (pipeline_txns_left > 0) pipeline_txns_left--;
            txns_left = pipeline_txns_left;
            if (b->txns_left == 0) { 
                delete b; 
                pipeline_batches.erase(pipeline_batches.begin() + bi); 
            }
            INC_STATS(thd_id, seq_ack_time, get_sys_clock() - starttime);
            return;
        }
    }
}

void SDOCCSequencer::send_next_batch(uint64_t thd_id) {
    uint64_t prof_stat = get_sys_clock();
    if (pipeline_batches.empty()) return;
    PBatch * b = nullptr;
    for (auto &bb : pipeline_batches) {
        if (!bb->sent) { 
            b = bb; 
            break; 
        }
    }
    if (!b) return;
    DEBUG_SCH("PIPELINE SEND NEXT BATCH %ld %ld %ld\n", thd_id, b->id, b->txns.size());
    for (uint64_t i = 0; i < b->txns.size(); i++) {
        // !目前设定为在发送事务的时候，提高 min_commit_read_sid
		// uint64_t key = get_calvin_key(msg->batch_id, msg->return_node_id, msg->txn_id);
        // watermark_node_entry* entry = (watermark_node_entry*)mem_allocator.align_alloc(sizeof(watermark_node_entry));
        // entry->key = key;
        // ListNode<watermark_node_entry*>* ld = check_water_mark->insert(entry, thd_id);
        // DEBUG_SCH("check_water_mark save %ld for txn %ld,%ld.\n", key, msg->batch_id, msg->txn_id);
        // ((ClientQueryMessage*)msg)->list_node_pointer = ld;

        work_queue.sdocc_enqueue(thd_id, b->txns[i]->msg, false);
        // DEBUG_SCH("PIPELINE SEND NEXT TXN %ld to queue in phase ARIA_READ, txn[%ld,%ld], key: %lu\n", thd_id, b->txns[i]->msg->batch_id, b->txns[i]->msg->txn_id, key);
    }
    INC_STATS(thd_id, seq_batch_cnt, 1);
    if (b->txns.size() == g_aria_batch_size) INC_STATS(thd_id, seq_full_batch_cnt, 1);
    b->sent = true;
    batch_id = b->id + 1;
    INC_STATS(thd_id, seq_prep_time, get_sys_clock() - prof_stat);
    INC_STATS(thd_id, seq_batch_time, get_sys_clock() - last_batch_time);
    last_batch_time = prof_stat;
}

void SDOCCSequencer::put_one_txn_to_batch(uint64_t _thd_id) {
    Message * msg = work_queue.txn_dequeue(_thd_id);
    if (!msg) return;
    int rtype = msg->get_rtype(); 
    assert(rtype == CL_QRY);
    if (!pipeline_current_batch)
        pipeline_current_batch = new PBatch(pipeline_next_batch_id);
    sdocc_txn * en = (sdocc_txn *) mem_allocator.alloc(sizeof(sdocc_txn));
    msg->batch_id = pipeline_current_batch->id;
    msg->txn_id = g_node_id + g_node_cnt * next_txn_id; next_txn_id++; 
    assert(msg->txn_id != UINT64_MAX);
    assert(ISCLIENTN(msg->get_return_id()));
    en->client_id = msg->get_return_id(); 
    en->client_startts = ((ClientQueryMessage *)msg)->client_startts;
    en->total_batch_time = 0; 
    en->abort_cnt = 0; 
    msg->return_node_id = g_node_id; 
    msg->lat_network_time = 0; 
    msg->lat_other_time = 0;
    en->msg = msg; 
    en->seq_startts = get_sys_clock(); 
    en->seq_first_startts = en->seq_startts;
    pipeline_current_batch->txns.push_back(en);
    if (pipeline_current_batch->txns.size() >= g_aria_batch_size) {
        pipeline_current_batch->txns_left = pipeline_current_batch->txns.size();
        pipeline_batches.push_back(pipeline_current_batch);
        pipeline_txns_left += pipeline_current_batch->txns_left;
        pipeline_current_batch = nullptr; 
        pipeline_next_batch_id++;
    }
    txns_left = pipeline_txns_left;
}

#endif
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

#if CC_ALG == SDOCC// || CC_ALG == SILO

#if SIMPLE_SDOCC_BATCH
void SDOCCSequencer::init(Workload *wl) {
    batch_id = 0;
    next_txn_id = 0;
    _wl = wl;

    pipeline_txn_count = 0;
    assert((uint32_t)g_inflight_max > g_aria_batch_size);
}

void SDOCCSequencer::put_one_txn_to_batch(uint64_t _thd_id, Message * msg) {
    if (!msg) return;
    int rtype = msg->get_rtype(); 
    assert(rtype == CL_QRY);

    // 这里就是给事务分配batch_id和txn_id了，保证同一batch内的事务id递增
    msg->batch_id = batch_id;
    msg->txn_id = g_node_id + g_node_cnt * next_txn_id; 
    next_txn_id++;
    assert(msg->txn_id != UINT64_MAX);
    assert(ISCLIENTN(msg->get_return_id()));

    ATOM_ADD(pipeline_txn_count, 1);

    if (pipeline_txn_count >= g_aria_batch_size) {
        // 先把当前batch的事务发出去
        pipeline_txn_count = 0;
        batch_id++;
        DEBUG_SEQ("PIPELINE ADVANCE SEQ EPOCH, new batch id: %ld \n", batch_id);
    } 

    // 直接把事务发出去
    uint64_t key = get_calvin_key(msg->batch_id, msg->return_node_id, msg->txn_id);
    watermark_node_entry* entry = (watermark_node_entry*)mem_allocator.align_alloc(sizeof(watermark_node_entry));
    entry->key = key;
    ListNode<watermark_node_entry*>* ld = check_water_mark->insert(entry, _thd_id);
    // DEBUG_SCH("check_water_mark save %ld for txn %ld,%ld.\n", key, msg->batch_id, msg->txn_id);
    ((ClientQueryMessage*)msg)->list_node_pointer = ld;

    work_queue.sdocc_enqueue(_thd_id, msg, false);
    DEBUG_SEQ("PIPELINE PUT ONE TXN TO BATCH, txn_id: %ld, batch_id: %ld\n", msg->txn_id, msg->batch_id);
}
#else
void SDOCCSequencer::init(Workload *wl) {
    next_txn_id = 0;
    last_batch_time = 0;
    _wl = wl;

    pipeline_batches.clear();
    pipeline_current_batch = nullptr;
    pipeline_next_batch_id = 0;
    pipeline_txns_left = 0;
    assert((uint32_t)g_inflight_max > g_aria_batch_size);

    pipeline_current_batch = new PBatch(pipeline_next_batch_id);
    pipeline_batches.push_back(pipeline_current_batch);

    // pthread_mutex_init(&batch_lock, nullptr);
}

// Assumes 1 thread does sequencer work
void SDOCCSequencer::process_ack(Message * msg, uint64_t thd_id) {
	uint64_t starttime = get_sys_clock();
    uint64_t txn_id = msg->txn_id;
    uint64_t b_id = msg->batch_id;
    DEBUG_SEQ("PIPELINE PROCESS ACK txn_id: %ld, b_id: %ld, rc: %d\n", txn_id, b_id, ((AckMessage *)msg)->rc);
    // 先根据b_id找到对应的PBatch

    PBatch * b = nullptr;
    int bi;
    // pthread_mutex_lock(&batch_lock);
    for (bi = 0; bi < pipeline_batches.size(); ++bi) {
        PBatch * bb = pipeline_batches[bi];
        if (bb->id == b_id) {
            b = bb;
            break;
        }
    }
    // pthread_mutex_unlock(&batch_lock);
    if (!b) {
        assert(false);
        DEBUG_SEQ("PIPELINE PROCESS ACK ERROR: batch id %ld not found for txn_id %ld\n", b_id, txn_id);
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
            if (pipeline_txns_left > 0) {
                ATOM_SUB(pipeline_txns_left, 1);
            }
            b->txn_complete();
            if (b->batch_complete()) {
                assert(b != pipeline_current_batch);
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
    if (!pipeline_current_batch) return; // current batch not finished yet
    PBatch * b = nullptr;
    // for (auto &bb : pipeline_batches) {
    //     if (!bb->sent) { 
    //         b = bb; 
    //         break; 
    //     }
    // }
    // if (!b) return;
    b = pipeline_current_batch;
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
        // DEBUG_SCH("PIPELINE SEND NEXT TXN %ld to queue, txn[%ld,%ld], key: %lu\n", thd_id, b->txns[i]->msg->batch_id, b->txns[i]->msg->txn_id, key);
    }
    INC_STATS(thd_id, seq_batch_cnt, 1);
    if (b->txns.size() == g_aria_batch_size) INC_STATS(thd_id, seq_full_batch_cnt, 1);
    b->sent = true;
    INC_STATS(thd_id, seq_prep_time, get_sys_clock() - prof_stat);
    INC_STATS(thd_id, seq_batch_time, get_sys_clock() - last_batch_time);
    last_batch_time = prof_stat;
}

void SDOCCSequencer::put_one_txn_to_batch(uint64_t _thd_id, Message * msg) {
    if (!msg) return;
    int rtype = msg->get_rtype(); 
    assert(rtype == CL_QRY);
    assert(pipeline_current_batch);

    sdocc_txn * en = (sdocc_txn *) mem_allocator.alloc(sizeof(sdocc_txn));
    msg->batch_id = pipeline_current_batch->id;
    msg->txn_id = g_node_id + g_node_cnt * next_txn_id; next_txn_id++; 
    assert(msg->txn_id != UINT64_MAX);
    assert(ISCLIENTN(msg->get_return_id()));
    en->client_id = msg->get_return_id(); 
    en->client_startts = ((ClientQueryMessage *)msg)->client_startts;
    en->total_batch_time = 0; 
    en->abort_cnt = 0; 
    // msg->return_node_id = g_node_id; 
    msg->lat_network_time = 0; 
    msg->lat_other_time = 0;
    en->msg = msg; 
    en->seq_startts = get_sys_clock(); 
    en->seq_first_startts = en->seq_startts;
    pipeline_current_batch->txns.push_back(en);
    assert(pipeline_current_batch->txns.size() <= g_aria_batch_size);

    ATOM_ADD(pipeline_txns_left, 1);
    pipeline_current_batch->txn_start();

    DEBUG_SEQ("PIPELINE PUT ONE TXN TO BATCH, txn_id: %ld, batch_id: %ld\n", msg->txn_id, msg->batch_id);
    if (is_batch_ready()) {
        advance_seq_epoch();
    }
    assert(pipeline_current_batch->txns.size() < g_aria_batch_size);
    {
        // 直接把事务发出去
        work_queue.sdocc_enqueue(_thd_id, msg, false);
        // work_queue.enqueue(_thd_id, msg, false);
    }
}

bool SDOCCSequencer::is_batch_ready() {
    assert(pipeline_current_batch);
    bool ready = pipeline_current_batch->txns_cnt >= g_aria_batch_size;
	return ready;
}

void SDOCCSequencer::advance_seq_epoch() {
    assert(pipeline_current_batch);
    // 开始新的batch
    pipeline_next_batch_id++;
    DEBUG_SEQ("PIPELINE ADVANCE SEQ EPOCH, new batch id: %ld now batch count:%ld\n", pipeline_next_batch_id, pipeline_batches.size());
    pipeline_current_batch = new PBatch(pipeline_next_batch_id); 
    // pthread_mutex_lock(&batch_lock);
    pipeline_batches.push_back(pipeline_current_batch);
    // pthread_mutex_unlock(&batch_lock);
}

#endif
#endif
#include "aria_sequencer.h"
#include "work_queue.h"
#include "message.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "msg_queue.h"

#if CC_ALG == ARIA

void AriaSequencer::init(Workload *wl) {
    next_txn_id = 0;
    batch_id = 0;
    last_batch_time = 0;
    txns_left = 0;
    _wl = wl;
    assert((uint32_t)g_inflight_max > g_aria_batch_size);
}

void AriaSequencer::send_next_batch(uint64_t thd_id) {
    uint64_t prof_stat = get_sys_clock();
    assert(aria_batch.size() != 0);
    DEBUG("SEND NEXT BATCH %ld %ld %ld\n", thd_id, batch_id, aria_batch.size());
    for (uint64_t i = 0; i < aria_batch.size(); i++) {
        work_queue.work_enqueue(thd_id, aria_batch[i]->msg, false, ARIA_READ);
        // printf("thd_id: %ld add txn: %ld to queue in phase %d\n", thd_id, aria_batch[i]->msg->txn_id, simulation->aria_phase);
    }

    INC_STATS(thd_id, seq_batch_cnt, 1);
    if (aria_batch.size() == g_aria_batch_size) {
        INC_STATS(thd_id, seq_full_batch_cnt, 1);
    }
    batch_id++;
    //use seq_prep_time to store the send time
    INC_STATS(thd_id, seq_prep_time, get_sys_clock() - prof_stat);
    INC_STATS(thd_id, seq_batch_time, get_sys_clock() - last_batch_time);
    last_batch_time = prof_stat;
}

void AriaSequencer::fill_batch(uint64_t _thd_id) {
    Message * msg;
    uint64_t idle_starttime = 0;
    while (aria_batch.size() < g_aria_batch_size) {
        msg = work_queue.txn_dequeue(_thd_id);

        if (!msg) {
            if (idle_starttime == 0) idle_starttime = get_sys_clock();
                continue;
        }
        if(idle_starttime > 0) {
            INC_STATS(_thd_id,seq_idle_time,get_sys_clock() - idle_starttime);
            idle_starttime = 0;
        }
        auto rtype = msg->get_rtype();
        assert(rtype == CL_QRY);
        process_txn(msg, _thd_id);
        assert(aria_batch.size() <= g_aria_batch_size);
    }
    txns_left = aria_batch.size();
}

void AriaSequencer::process_txn(Message* msg, uint64_t thd_id) {
    uint64_t starttime = get_sys_clock();
    aria_txn * en = (aria_txn *) mem_allocator.alloc(sizeof(aria_txn));
    msg->batch_id = batch_id;
    msg->txn_id = g_node_id + g_node_cnt * next_txn_id;
    next_txn_id++;
    assert(msg->txn_id != UINT64_MAX);

    //We use this information to send the message back to the client quickly, it does not hurt the execution logic.
    // #if WORKLOAD == YCSB
	// 	std::set<uint64_t> participants = YCSBQuery::participants(msg,_wl);
    // #elif WORKLOAD == TPCC
	// 	std::set<uint64_t> participants = TPCCQuery::participants(msg,_wl);
    // #endif

    // en->server_ack_cnt = participants.size();
    // assert(en->server_ack_cnt > 0);
    assert(ISCLIENTN(msg->get_return_id()));
    en->client_id = msg->get_return_id();
    en->client_startts = ((ClientQueryMessage *)msg)->client_startts;
    en->total_batch_time = 0;
    en->abort_cnt = 0;
    // en->skew_startts = 0;
    msg->return_node_id = g_node_id;
    msg->lat_network_time = 0;
    msg->lat_other_time = 0;
    en->msg = msg;
    en->seq_startts = get_sys_clock();
    en->seq_first_startts = en->seq_startts;

    aria_batch.push_back(en);
    INC_STATS(thd_id,seq_process_cnt,1);
	INC_STATS(thd_id,seq_process_time,get_sys_clock() - starttime);
}

//TODO: 传进来的msg好像没有释放
void AriaSequencer::process_ack(Message * msg, uint64_t thd_id) {
    // Find the corresponding aria_txn in the aria_batch with the message
    uint64_t starttime = get_sys_clock();
    uint64_t txn_id = msg->txn_id;
    uint64_t batch_id = msg->batch_id;
    DEBUG_SCH("process ack txn_id: %ld, rc: %d txns_left %ld\n", txn_id, ((AckMessage *)msg)->rc, txns_left);
    for (uint64_t i = 0; i < aria_batch.size(); i++) {
        if (aria_batch[i]->msg->txn_id == txn_id && aria_batch[i]->msg->batch_id == batch_id) {
            if (((AckMessage *)msg)->rc == RCOK) {
                INC_STATS(thd_id, seq_txn_cnt, 1);

            #if WORKLOAD == YCSB
                YCSBClientQueryMessage * cl_msg = (YCSBClientQueryMessage *)aria_batch[i]->msg;
                // for(uint64_t i = 0; i < cl_msg->requests.size(); i++) {
				// 	DEBUG_M("Sequencer::process_ack() ycsb_request free\n");
				// 	mem_allocator.free(cl_msg->requests[i],sizeof(ycsb_request));
                // }
            #elif WORKLOAD == TPCC
                TPCCClientQueryMessage * cl_msg = (TPCCClientQueryMessage*)aria_batch[i]->msg;
                // if(cl_msg->txn_type == TPCC_NEW_ORDER) {
				// 	for(uint64_t i = 0; i < cl_msg->items.size(); i++) {
				// 			DEBUG_M("Sequencer::process_ack() items free\n");
				// 			mem_allocator.free(cl_msg->items[i],sizeof(Item_no));
				// 	}
			    // }
            #endif

                uint64_t curr_clock = get_sys_clock();
                uint64_t long_timespan = curr_clock - aria_batch[i]->seq_first_startts;
                uint64_t short_timespan = curr_clock - aria_batch[i]->seq_startts;
                // uint64_t skew_timespan = get_sys_clock() - aria_batch[i]->skew_startts;
                if (warmup_done) {
                    INC_STATS_ARR(0, first_start_commit_latency, long_timespan);
                    INC_STATS_ARR(0, last_start_commit_latency, short_timespan);
                    INC_STATS_ARR(0, start_abort_commit_latency, short_timespan);
                }
                if (aria_batch[i]->abort_cnt > 0) {
                    INC_STATS(0, unique_txn_abort_cnt, 1);
                }
                INC_STATS(0, lat_l_loc_msg_queue_time, curr_clock - last_batch_time);
                // INC_STATS(0, lat_l_loc_process_time, skew_timespan);
                INC_STATS(0, lat_short_work_queue_time, msg->lat_work_queue_time);
                INC_STATS(0, lat_short_msg_queue_time, msg->lat_msg_queue_time);
                INC_STATS(0, lat_short_cc_block_time, msg->lat_cc_block_time);
                INC_STATS(0, lat_short_cc_time, msg->lat_cc_time);
                INC_STATS(0, lat_short_process_time, msg->lat_process_time);

                if (msg->return_node_id != g_node_id) {
                    INC_STATS(0, lat_short_network_time, msg->lat_network_time);
                }
                cl_msg->release();

                ClientResponseMessage * rsp_msg = (ClientResponseMessage *)Message::create_message(msg->get_txn_id(), CL_RSP);
                rsp_msg->client_startts = aria_batch[i]->client_startts;
                msg_queue.enqueue(thd_id, rsp_msg, aria_batch[i]->client_id);

                // Remove the aria_txn from the pool
                mem_allocator.free(aria_batch[i], sizeof(aria_txn));
                aria_batch.erase(aria_batch.begin() + i);
                // printf("txn: %ld remove from batch\n", txn_id);
                INC_STATS(thd_id,seq_complete_cnt,1);
                assert(aria_batch.size() < g_aria_batch_size);

                // Dequeue new message to process if there is any
                Message * msg = work_queue.txn_dequeue(thd_id);
                if (msg) {
                    process_txn(msg, thd_id);
                }
            } else {
                aria_batch[i]->abort_cnt++;
                aria_batch[i]->seq_startts = get_sys_clock();
            }

            txns_left--;
            if (txns_left == 0) {
                DEBUG_SCH("thd_id: %ld, all ack received for this batch, move to next phase %d\n", thd_id, simulation->aria_phase);
                // This is the last ack for this batch. wait work thread finish all transactions and go to next phase.
                while (simulation->batch_process_count != 0 && !simulation->is_done()) {}
                simulation->next_aria_phase();
                DEBUG_SCH("thd_id: %ld, phase: %d\n", thd_id, simulation->aria_phase);
                assert(simulation->aria_phase == ARIA_COLLECT);
            }

            INC_STATS(thd_id, seq_ack_time, get_sys_clock() - starttime);

            break;
        }
    }
}
#endif // CC_ALG == ARIA

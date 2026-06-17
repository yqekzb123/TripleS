#include "caracal_sequencer.h"
#include "work_queue.h"
#include "message.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "msg_queue.h"
#include "global.h"
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

#if CC_ALG == CARACAL
void CaracalSequencer::init(Workload *wl) {
    next_txn_id = 0;
    batch_id = 0;
    last_batch_time = 0;
    txns_left = 0;
    _wl = wl;
    assert((uint32_t)g_inflight_max > g_caracal_batch_size);

    fill_queue = new boost::lockfree::queue<Message*, boost::lockfree::capacity<65526> > [g_node_cnt];
}

void CaracalSequencer::send_next_batch(uint64_t thd_id) {
    uint64_t prof_stat = get_sys_clock();
    assert(caracal_batch.size() != 0);
    printf("SEND NEXT BATCH %ld %ld %ld\n", thd_id, batch_id, caracal_batch.size());
    // DEBUG_SEQ("SEND NEXT BATCH %ld %ld %ld\n", thd_id, batch_id, caracal_batch.size());
    Message * msg;
    for(uint64_t j = 0; j < g_node_cnt; j++) {
		while(fill_queue[j].pop(msg)) {
			if(j == g_node_id) {
				work_queue.work_enqueue(thd_id, msg, false, CARACAL_INIT);
                // printf("thd_id: %ld add txn: %ld to queue in phase %d\n", thd_id, caracal_batch[i]->msg->txn_id, simulation->caracal_phase);
			} else {
				msg_queue.enqueue(thd_id,msg,j);
			}
            // 只要有一个消息被成功发送了，就算这个ACK的数量加1
            // 同一个事务如果有多个参与节点，那么这个事务的ACK数量会被多次计算，但这并不影响逻辑正确性，因为我们只关心ACK数量达到一个阈值（即caracal_batch中事务的总参与节点数量）时才进行下一步处理
            // total_ack_count++;
		}
	}

    INC_STATS(thd_id, seq_batch_cnt, 1);
    if (caracal_batch.size() == g_caracal_batch_size) {
        INC_STATS(thd_id, seq_full_batch_cnt, 1);
    }
    // batch_id++;
    //use seq_prep_time to store the send time
    INC_STATS(thd_id, seq_prep_time, get_sys_clock() - prof_stat);
    INC_STATS(thd_id, seq_batch_time, get_sys_clock() - last_batch_time);
    last_batch_time = prof_stat;
}

void CaracalSequencer::fill_batch(uint64_t _thd_id) {
    Message * msg;
    uint64_t idle_starttime = 0;
    total_ack_count = 0;
    while (caracal_batch.size() < g_caracal_batch_size) {
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
        assert(caracal_batch.size() <= g_caracal_batch_size);
    }
    txns_left = caracal_batch.size();
    batch_id++;
     // DEBUG_SEQ("FILL BATCH %ld %ld\n", _thd_id, batch_id);
}

void CaracalSequencer::process_txn(Message* msg, uint64_t thd_id) {
    uint64_t starttime = get_sys_clock();
    caracal_txn * en = (caracal_txn *) mem_allocator.alloc(sizeof(caracal_txn));
    msg->batch_id = batch_id;
    msg->txn_id = g_node_id + g_node_cnt * next_txn_id;
    // msg->caracal_phase = CARACAL_INIT;
    next_txn_id++;
    assert(msg->txn_id != UINT64_MAX);

    //We use this information to send the message back to the client quickly, it does not hurt the execution logic.
    #if WORKLOAD == YCSB
		std::set<uint64_t> participants = YCSBQuery::participants(msg,_wl);
    #elif WORKLOAD == TPCC
		std::set<uint64_t> participants = TPCCQuery::participants(msg,_wl);
    #endif

    en->server_ack_cnt = participants.size();
    assert(en->server_ack_cnt > 0);
    total_ack_count += en->server_ack_cnt;

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

    caracal_batch.push_back(en);
    for(auto participant = participants.begin(); participant != participants.end(); participant++) {
		// DEBUG_SEQ("SEQ adding (%ld,%ld) to fill queue (recon: %d)\n", msg->get_txn_id(),
			// msg->get_batch_id(), ((PPSClientQueryMessage *)msg)->recon);
		// DEBUG_SEQ("SEQ adding (%ld,%ld) to fill queue\n", msg->get_batch_id(), msg->get_txn_id());
		while (!fill_queue[*participant].push(msg) && !simulation->is_done()) {
		}
	}

    INC_STATS(thd_id,seq_process_cnt,1);
	INC_STATS(thd_id,seq_process_time,get_sys_clock() - starttime);
}

//TODO: 传进来的msg好像没有释放

void CaracalSequencer::process_ack(Message * msg, uint64_t thd_id) {
    // Find the corresponding caracal_txn in the caracal_batch with the message
    uint64_t starttime = get_sys_clock();
    uint64_t txn_id = msg->txn_id;
    uint64_t batch_id = ((AckMessage*)msg)->batch_id;

    if (txns_left < 5) {
        std::string remain_txn_info = get_remain_txn_info();
        DEBUG_SEQ("process ack %ld,%ld, rc: %d txns_left %ld, remains %s\n", batch_id,txn_id, ((AckMessage *)msg)->rc, txns_left, remain_txn_info.c_str());
    } else {
        DEBUG_SEQ("process ack %ld,%ld, rc: %d txns_left %ld\n", batch_id,txn_id, ((AckMessage *)msg)->rc, txns_left);
    }

    assert(batch_id == simulation->current_batch_id);
    for (uint64_t i = 0; i < caracal_batch.size(); i++) {
        if (caracal_batch[i]->msg->txn_id == txn_id && caracal_batch[i]->msg->batch_id == batch_id) {
            uint32_t query_acks_left = ATOM_SUB_FETCH(caracal_batch[i]->server_ack_cnt, 1);
            DEBUG_SEQ("Ack received for %ld,%ld from node %ld, rc: %d, query_acks_left: %d\n", batch_id,txn_id,msg->return_node_id, ((AckMessage *)msg)->rc, query_acks_left);
            if (query_acks_left == 0) {
                DEBUG_SEQ("Ack received for %ld,%ld, rc: %d, all acks received, process the result\n", batch_id,txn_id, ((AckMessage *)msg)->rc);
                INC_STATS(thd_id, seq_txn_cnt, 1);

            #if WORKLOAD == YCSB
                YCSBClientQueryMessage * cl_msg = (YCSBClientQueryMessage *)caracal_batch[i]->msg;
            #elif WORKLOAD == TPCC
                TPCCClientQueryMessage * cl_msg = (TPCCClientQueryMessage*)caracal_batch[i]->msg;
                // if(cl_msg->txn_type == TPCC_NEW_ORDER) {
				// 	for(uint64_t i = 0; i < cl_msg->items.size(); i++) {
				// 			DEBUG_M("Sequencer::process_ack() items free\n");
				// 			mem_allocator.free(cl_msg->items[i],sizeof(Item_no));
				// 	}
			    // }
            #endif

                uint64_t curr_clock = get_sys_clock();
                uint64_t long_timespan = curr_clock - caracal_batch[i]->seq_first_startts;
                uint64_t short_timespan = curr_clock - caracal_batch[i]->seq_startts;
                // uint64_t skew_timespan = get_sys_clock() - caracal_batch[i]->skew_startts;
                if (warmup_done) {
                    INC_STATS_ARR(0, first_start_commit_latency, long_timespan);
                    INC_STATS_ARR(0, last_start_commit_latency, short_timespan);
                    INC_STATS_ARR(0, start_abort_commit_latency, short_timespan);
                }
                if (caracal_batch[i]->abort_cnt > 0) {
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
                
                rsp_msg->client_startts = caracal_batch[i]->client_startts;
                msg_queue.enqueue(thd_id, rsp_msg, caracal_batch[i]->client_id);
                DEBUG_SEQ("Ack processed for %ld,%ld, send response to client_id: %d\n", batch_id, txn_id, caracal_batch[i]->client_id);

                // Remove the caracal_txn from the pool
                mem_allocator.free(caracal_batch[i], sizeof(caracal_txn));
                caracal_batch.erase(caracal_batch.begin() + i);
                // printf("txn: %ld remove from batch\n", txn_id);
                INC_STATS(thd_id,seq_complete_cnt,1);
                assert(caracal_batch.size() < g_caracal_batch_size);

                // Dequeue new message to process if there is any
                // Message * msg = work_queue.txn_dequeue(thd_id);
                // if (msg) {
                //     process_txn(msg, thd_id);
                // }
            } else {
                break;
            }

            txns_left--;
            if (txns_left == 0) {
                DEBUG_SEQ("thd_id: %ld, all ack received for this batch, move to next phase %d\n", thd_id, simulation->caracal_phase);
                simulation->get_all_txn_finish.store(true);
                // This is the last ack for this batch. wait work thread finish all transactions and go to next phase.
                // while (simulation->caracal_phase != CARACAL_COMMIT && !simulation->is_done()) {}
                // if (simulation->is_done()) {
                //     DEBUG_SEQ("thd_id: %ld, simulation is done, return\n", thd_id);
                //     return;
                // }
                // simulation->next_caracal_phase();
                // DEBUG_SEQ("thd_id: %ld, phase: %d\n", thd_id, simulation->caracal_phase);
                // assert(simulation->caracal_phase == CARACAL_COLLECT);
            }

            INC_STATS(thd_id, seq_ack_time, get_sys_clock() - starttime);

            break;
        }
    }
}
#endif // CC_ALG == ARIA

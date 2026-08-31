#include "global.h"
#include "thread.h"
#include "caracal_thread.h"
#include "caracal_sequencer.h"
#include "work_queue.h"
#include "message.h"
#include "msg_queue.h"
#include "caracal.h"

#if CC_ALG == CARACAL

void CaracalSequencerThread::setup() {}

RC CaracalSequencerThread::run() {
    tsetup();
    printf("Running Sequencer %ld\n",_thd_id);

    Message * msg;
    uint64_t idle_starttime = 0;

    while (!simulation->is_done())
    {
        //TODO: 好像不需要ARIA_INIT
        if (simulation->caracal_phase.load() == CARACAL_COLLECT && 
            !simulation->send_txn_finish.load()) {
            if (!caracal_seq.fill_batch(_thd_id)) break;
            simulation->current_batch_id = caracal_seq.get_batch_id()-1;
            caracal_seq.send_next_batch(_thd_id);

            simulation->send_txn_finish.store(true);
            // simulation->next_caracal_phase();
            // DEBUG_SEQ("thd_id: %ld, phase: %d\n", _thd_id, simulation->caracal_phase);
            // assert(simulation->caracal_phase == CARACAL_INIT);
        }

        msg = work_queue.sequencer_dequeue(_thd_id);
        if (!msg) {
            if (idle_starttime == 0) {
                idle_starttime = get_sys_clock();
            }
            continue;
        }
        if (idle_starttime > 0) {
            INC_STATS(_thd_id, seq_idle_time, get_sys_clock() - idle_starttime);
            idle_starttime = 0;
        }

        int rtype = msg->get_rtype();
        if (rtype == CARACAL_DONE) {
            caracal_seq.process_ack(msg, _thd_id);
        } else {
            assert(false);
        }
    }

    printf("FINISH %ld:%ld\n", _node_id, _thd_id);
    fflush(stdout);
    
    return FINISH; 
}

void CaracalControlThread::setup() {}

RC CaracalControlThread::process_caracal_txn_ack(Message * msg) {
  AckMessage * ack = (AckMessage *)msg;

  assert(ack->batch_id >= simulation->current_batch_id);
  assert(ack->caracal_phase >= simulation->caracal_phase.load());
  
  if (ack->batch_id != simulation->current_batch_id || 
      ack->caracal_phase != simulation->caracal_phase.load()) {
      // 说明不是当前阶段的，先assert阶段比当前大
      work_queue.phase_ack_enqueue(get_thd_id(), msg);
    //   DEBUG_SCH("CaracalControlThread %ld received future CARACAL_TXN_ACK for node %ld txn %ld,%ld phase %ld, current batch %ld phase %d, re-enqueue it\n", get_thd_id(), ack->get_return_id(), ack->batch_id, ack->txn_id, ack->caracal_phase, simulation->current_batch_id, simulation->caracal_phase);
      return RCOK;
  }
  
  ATOM_ADD_FETCH(simulation->batch_process_count, 1);
  ATOM_ADD_FETCH(simulation->batch_remote_process_count, 1);

  DEBUG_SCH("CaracalControlThread %ld received CARACAL_TXN_ACK from node %ld txn %ld,%ld phase %ld, now batch_id %ld batch_process_count %ld/%ld, batch_local_process_count %ld, batch_remote_process_count %ld, batch_remote_send_count %ld\n", get_thd_id(), ack->get_return_id(), ack->batch_id, ack->txn_id, ack->caracal_phase, simulation->current_batch_id, simulation->batch_process_count, caracal_seq.get_total_ack_count(), simulation->batch_local_process_count, simulation->batch_remote_process_count, simulation->batch_remote_send_count);

  msg->release();
  delete msg;
  return RCOK;
}

RC CaracalControlThread::process_caracal_phase_ack(Message * msg) {
  AckMessage * ack = (AckMessage *)msg;
  // 考虑几种情况吧，消息落后于当前阶段了，这个明显不对

  int index = 0;
  if (ack->caracal_phase == CARACAL_INIT) {
    index = 0;
    // assert(simulation->caracal_phase == CARACAL_APPEND || simulation->caracal_phase == CARACAL_APPEND_SYNC ||
        //    simulation->caracal_phase == CARACAL_INIT || simulation->caracal_phase == CARACAL_COLLECT);
  } else if (ack->caracal_phase == CARACAL_APPEND) {
    // assert(false);
    index = 1;
    // assert(simulation->caracal_phase == CARACAL_EXECUTION || simulation->caracal_phase == CARACAL_EXECUTION_SYNC);
  } else if (ack->caracal_phase == CARACAL_EXECUTION) {
    index = 2;
    // assert(simulation->caracal_phase == CARACAL_EXECUTION || simulation->caracal_phase == CARACAL_EXECUTION_SYNC);
  } else {
    assert(false);
  }
  simulation->caracal_barrier[index].set_barrier(ack->get_return_id());
  std::string str = simulation->caracal_barrier[0].get_barrier_str("0") + simulation->caracal_barrier[1].get_barrier_str("1") + simulation->caracal_barrier[2].get_barrier_str("2");
  DEBUG_SCH("%s\n", str.c_str());
  msg->release();
  delete msg;
  return RCOK;
}

void CaracalControlThread::send_phase_sync_message(CARACAL_PHASE phase) {
    // 这个函数主要是用来检查当前阶段的完成情况的
    for (uint64_t i = 0; i < g_node_cnt; i++) {
        if (i == g_node_id) continue;
        Message* msg = Message::create_message(CARACAL_PHASE_ACK); 
        ((AckMessage*)msg)->batch_id = simulation->current_batch_id;
        ((AckMessage*)msg)->caracal_phase = phase;
        msg_queue.enqueue(_thd_id, msg, i);
        DEBUG_SCH("Worker %ld sent CARACAL_PHASE_ACK to node %ld for batch %ld phase %d\n", get_thd_id(), i, simulation->current_batch_id, phase);
    }
}

RC CaracalControlThread::check_phase_end() {
    switch (simulation->caracal_phase.load())
    {
    case CARACAL_COLLECT:
        if (simulation->send_txn_finish.load()) {
            simulation->next_caracal_phase();
            // simulation->send_txn_finish.store(false);

            caracal_man.set_phase_undone_for_all();
        }
        break;
    case CARACAL_INIT:
        /* code */
        if (simulation->batch_process_count >= g_caracal_batch_size &&
            simulation->batch_process_count >= caracal_seq.get_total_ack_count()) {

            if (g_mpr != 0) send_phase_sync_message(CARACAL_INIT);
            simulation->next_caracal_phase();
            simulation->batch_process_count = 0;
            simulation->batch_local_process_count = 0;
            simulation->batch_remote_process_count = 0;
            simulation->batch_remote_send_count = 0;
        }
        break;
    case CARACAL_INIT_SYNC:
        if (g_mpr == 0) simulation->next_caracal_phase();
        else {
            if (simulation->caracal_barrier[0].barrier_count == g_node_cnt - 1) {
                simulation->next_caracal_phase();
            }
        }
        break;
    case CARACAL_APPEND:
        if (simulation->finish_append_cnt.load() == g_thread_cnt) {
            if (g_mpr != 0) send_phase_sync_message(CARACAL_APPEND);
            simulation->next_caracal_phase();
            simulation->finish_append_cnt.store(0);

            simulation->send_txn_finish.store(false);
        }
        break;
    case CARACAL_APPEND_SYNC:
        if (g_mpr == 0) simulation->next_caracal_phase();
        else {
            if (simulation->caracal_barrier[1].barrier_count == g_node_cnt - 1) {
                simulation->next_caracal_phase();
            }
        }
        break;
    case CARACAL_EXECUTION:
        if (simulation->get_all_txn_finish.load()) {
            if (g_mpr != 0) send_phase_sync_message(CARACAL_EXECUTION);
            simulation->get_all_txn_finish.store(false);
            simulation->next_caracal_phase();
        }
        break;
    case CARACAL_EXECUTION_SYNC:
         if (g_mpr == 0) simulation->next_caracal_phase();
        else {
            if (simulation->caracal_barrier[2].barrier_count == g_node_cnt - 1) {
                simulation->next_caracal_phase();
            }
        }
        break;
    default:
        break;
    }
    return RCOK;
}


RC CaracalControlThread::run() {
    tsetup();
    printf("Running CaracalControlThread %ld\n",_thd_id);

    while (!simulation->is_done()) {
        AckMessage* ack = (AckMessage*) work_queue.phase_ack_dequeue(_thd_id);
        if (ack) {
            int rtype = ack->get_rtype();
            switch (rtype)
            {
            case CARACAL_TXN_ACK:
                process_caracal_txn_ack(ack);
                break;
            case CARACAL_PHASE_ACK:
                process_caracal_phase_ack(ack);
                break;
            default:
                break;
            }
        }
        check_phase_end();

    }
    return FINISH;
}
#endif

#include "global.h"
#include "thread.h"
#include "caracal_thread.h"
#include "caracal_sequencer.h"
#include "work_queue.h"
#include "message.h"

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
        if (simulation->caracal_phase == CARACAL_COLLECT) {
            caracal_seq.fill_batch(_thd_id);
            caracal_seq.send_next_batch(_thd_id);
            simulation->current_batch_id = caracal_seq.get_batch_id()-1;
            simulation->next_caracal_phase();
            DEBUG_SEQ("thd_id: %ld, phase: %d\n", _thd_id, simulation->caracal_phase);
            assert(simulation->caracal_phase == CARACAL_INIT);
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
#endif

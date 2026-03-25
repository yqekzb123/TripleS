#include "global.h"
#include "thread.h"
#include "aria_thread.h"
#include "aria_sequencer.h"
#include "work_queue.h"
#include "message.h"

#if CC_ALG == ARIA

void AriaSequencerThread::setup() {}

RC AriaSequencerThread::run() {
    tsetup();
    printf("Running Sequencer %ld\n",_thd_id);

    Message * msg;
    uint64_t idle_starttime = 0;

    while (!simulation->is_done())
    {
        //TODO: 好像不需要ARIA_INIT
        if (simulation->aria_phase == ARIA_INIT) {
            aria_seq.fill_batch(_thd_id);
            simulation->next_aria_phase();
            // printf("thd_id: %ld, phase: %d\n", _thd_id, simulation->aria_phase);
            assert(simulation->aria_phase == ARIA_COLLECT);
        }

        if (simulation->aria_phase == ARIA_COLLECT) {
            aria_seq.fill_batch(_thd_id);
            aria_seq.send_next_batch(_thd_id);
            simulation->next_aria_phase();
            // printf("thd_id: %ld, phase: %d\n", _thd_id, simulation->aria_phase);
            assert(simulation->aria_phase == ARIA_READ);
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
        if (rtype == ARIA_ACK) {
            aria_seq.process_ack(msg, _thd_id);
        } else {
            assert(false);
        }
    }

    printf("FINISH %ld:%ld\n", _node_id, _thd_id);
    fflush(stdout);
    
    return FINISH; 
}
#endif

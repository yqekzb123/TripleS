#ifndef _CARACALTHREAD_H_
#define _CARACALTHREAD_H_

#include "global.h"
#include "thread.h"
#include "message.h"
#include "work_queue.h"

class CaracalSequencerThread : public Thread {
public:
    RC run();
    void setup();
};

class CaracalControlThread : public Thread {
public:
    RC run();
    void setup();
    RC process_caracal_txn_ack(Message * msg);
    RC process_caracal_phase_ack(Message * msg);
    RC check_phase_end();
    void send_phase_sync_message(CARACAL_PHASE phase);
};
#endif

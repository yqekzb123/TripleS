#ifndef _CARACALTHREAD_H_
#define _CARACALTHREAD_H_

#include "global.h"

class CaracalSequencerThread : public Thread {
public:
    RC run();
    void setup();
};

#endif

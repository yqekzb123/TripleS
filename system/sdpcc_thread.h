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

#ifndef _SDPCCTHREAD_H_
#define _SDPCCTHREAD_H_

#include "global.h"
#include <vector>
class Workload;

class SDPCCLockThread : public Thread {
public:
    RC run();
    void setup();
    void handle_tmp_txn(uint64_t current_minSid, uint64_t &old_minSid);
private:
    struct PendingTxn {
        TxnManager *txn;
        uint64_t wait_start;
    };
    TxnManager * m_txn;

    std::vector<PendingTxn> tmp_txn_list;
};

class SDPCCSequencerThread : public Thread {
public:
    RC run();
    void setup();
private:
    bool is_batch_ready();
	uint64_t last_batchtime;
};

#endif

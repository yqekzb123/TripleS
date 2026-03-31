/*
    Copyright 2026 Shandong University

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

#ifndef _SDOCC_SEQUENCER_H_
#define _SDOCC_SEQUENCER_H_

#include "global.h"
#include "query.h"
#include "sequencer.h"
#include <unordered_map>
#include <boost/lockfree/queue.hpp>

class Workload;
class BaseQuery;
class Message;

class SDOCCSequencer : public Sequencer {
 public:
	void process_ack(Message * msg, uint64_t thd_id);
	void process_txn(Message* msg, uint64_t thd_id, uint64_t early_start, uint64_t last_start,
									 uint64_t wait_time, uint32_t abort_cnt);
	void process_abort(Message *msg, uint64_t thd_id);
	void send_next_batch(uint64_t thd_id);

 private:
    void check_participants(Message * msg, Workload * wl);
};
#endif

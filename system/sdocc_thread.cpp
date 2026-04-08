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
#include "global.h"
#include "manager.h"
#include "thread.h"
#include "sdocc_thread.h"
#include "txn.h"
#include "wl.h"
#include "query.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "mem_alloc.h"
#include "transport.h"
#include "math.h"
#include "helper.h"
#include "msg_thread.h"
#include "msg_queue.h"
#include "sdocc_sequencer.h"
#include "logger.h"
#include "message.h"
#include "work_queue.h"

#if CC_ALG == SDOCC// || CC_ALG == SILO
void SDOCCSequencerThread::setup() {}

RC SDOCCSequencerThread::run() {
	tsetup();

	Message * msg;
	uint64_t idle_starttime = 0;
	uint64_t prof_starttime = 0;

	while(!simulation->is_done()) {
		// if (sdocc_seq_man.is_batch_ready()) {
		// 	sdocc_seq_man.send_next_batch(_thd_id);
		// 	INC_STATS(_thd_id, seq_batch_time, get_sys_clock() - prof_starttime);
		// 	prof_starttime = get_sys_clock();
		// 	sdocc_seq_man.advance_seq_epoch();
		// }
		msg = work_queue.txn_dequeue(_thd_id);
		// msg = work_queue.sdocc_sequencer_dequeue(_thd_id);
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
		// work_queue.sdocc_enqueue(_thd_id, msg, false);
		int rtype = msg->get_rtype();
		// if (rtype == PIP_ACK) {
		// 	sdocc_seq_man.process_ack(msg, _thd_id);
		// } else 
		if (rtype == CL_QRY) {
			sdocc_seq_man.put_one_txn_to_batch(_thd_id, msg);
		} else {
			assert(false);
		}
	}
	printf("FINISH %ld:%ld\n",_node_id,_thd_id);
	fflush(stdout);
	return FINISH;

}

#endif
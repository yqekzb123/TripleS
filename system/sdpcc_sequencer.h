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

#ifndef _SDPCC_SEQUENCER_H_
#define _SDPCC_SEQUENCER_H_

#include "global.h"
#include "query.h"
#include <unordered_map>
#include <boost/lockfree/queue.hpp>
#include "sequencer.h"

class SDPCCSequencer {
 public:
	void init(Workload * wl);
	void process_ack(Message * msg, uint64_t thd_id);
	void process_txn(Message* msg, uint64_t thd_id, uint64_t early_start, uint64_t last_start,
									 uint64_t wait_time, uint32_t abort_cnt);
	void process_abort(Message *msg, uint64_t thd_id);
	void send_next_batch(uint64_t thd_id);
	bool is_batch_ready() {
		bool ready = txn_size >= ARIA_BATCH_SIZE;
		return ready;
	}
	void reset_batch() {
		txn_size = 0;
	}	
	void add_txn() {
		txn_size++;
	}
 protected:
	void reset_participating_nodes(bool * part_nodes);

	boost::lockfree::queue<Message*, boost::lockfree::capacity<65526> > * fill_queue;
	uint64_t txn_size=0;

#if WORKLOAD == YCSB
	YCSBQuery* node_queries;
#elif WORKLOAD == TPCC
	TPCCQuery* node_queries;
#elif WORKLOAD == PPS
	PPSQuery* node_queries;
#endif
	volatile uint64_t total_txns_finished;
	volatile uint64_t total_txns_received;
	volatile uint32_t rsp_cnt;
	uint64_t last_time_batch;
	qlite_ll * wl_head;		// list of txns in batch being executed
	qlite_ll * wl_tail;		// list of txns in batch being executed
	volatile uint32_t next_txn_id;
	Workload * _wl;
};

#endif

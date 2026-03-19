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

#include "global.h"
#include "helper.h"
#include "manager.h"
#include "thread.h"
#include "io_thread.h"
#include "query.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "mem_alloc.h"
#include "transport.h"
#include "math.h"
#include "msg_thread.h"
#include "msg_queue.h"
#include "message.h"
#include "client_txn.h"
#include "work_queue.h"
#include "txn.h"
#include "ycsb.h"

void InputThread::setup() {

	std::vector<Message*> * msgs;
	while(!simulation->is_setup_done()) {
		msgs = tport_man.recv_msg(get_thd_id());
		if (msgs == NULL) continue;
		while(!msgs->empty()) {
			Message * msg = msgs->front();
			if(msg->rtype == INIT_DONE) {
				printf("Received INIT_DONE from node %ld\n",msg->return_node_id);
				fflush(stdout);
				simulation->process_setup_msg();
			} else {
				assert(ISSERVER || ISREPLICA);
				//printf("Received Msg %d from node %ld\n",msg->rtype,msg->return_node_id);
#if CC_ALG == CALVIN || CC_ALG == HDCC || CC_ALG == SNAPPER
			if(msg->rtype == CALVIN_ACK ||(msg->rtype == CL_QRY && ISCLIENTN(msg->get_return_id())) ||
				(msg->rtype == CL_QRY_O && ISCLIENTN(msg->get_return_id()))) {
				work_queue.sequencer_enqueue(get_thd_id(),msg);
				msgs->erase(msgs->begin());
				continue;
			}
			if( msg->rtype == RDONE || msg->rtype == CL_QRY || msg->rtype == CL_QRY_O) {
				assert(ISSERVERN(msg->get_return_id()));
				work_queue.sched_enqueue(get_thd_id(),msg);
				msgs->erase(msgs->begin());
				continue;
			}
#endif
				work_queue.enqueue(get_thd_id(),msg,false);
			}
			msgs->erase(msgs->begin());
		}
		delete msgs;
	}
	// if (!ISCLIENT) {
	// 	txn_man = (YCSBTxnManager *)
	// 		mem_allocator.align_alloc( sizeof(YCSBTxnManager));
	// 	new(txn_man) YCSBTxnManager();
	// 	// txn_man = (TxnManager*) malloc(sizeof(TxnManager));
	// 	uint64_t thd_id = get_thd_id();
	// 	txn_man->init(thd_id, NULL);
	// }
}

RC InputThread::run() {
	tsetup();
	printf("Running InputThread %ld\n",_thd_id);

	if(ISCLIENT) {
		client_recv_loop();
	} else {
		server_recv_loop();
	}

	return FINISH;

}

RC InputThread::client_recv_loop() {
	int rsp_cnts[g_servers_per_client];
	memset(rsp_cnts, 0, g_servers_per_client * sizeof(int));

	run_starttime = get_sys_clock();
	uint64_t return_node_offset;
	uint64_t inf;

	std::vector<Message*> * msgs;

	while (!simulation->is_done()) {
		heartbeat();
		uint64_t starttime = get_sys_clock();
		msgs = tport_man.recv_msg(get_thd_id());
		INC_STATS(_thd_id,mtx[28], get_sys_clock() - starttime);
		starttime = get_sys_clock();
		//while((m_query = work_queue.get_next_query(get_thd_id())) != NULL) {
		//Message * msg = work_queue.dequeue();
		if (msgs == NULL) continue;
		while(!msgs->empty()) {
			Message * msg = msgs->front();
			assert(msg->rtype == CL_RSP);
		#if CC_ALG == BOCC || CC_ALG == FOCC
			return_node_offset = msg->return_node_id;
		#else
			return_node_offset = msg->return_node_id - g_server_start_node;
		#endif
			assert(return_node_offset < g_servers_per_client);
			rsp_cnts[return_node_offset]++;
			INC_STATS(get_thd_id(),txn_cnt,1);
			uint64_t timespan = get_sys_clock() - ((ClientResponseMessage*)msg)->client_startts;
			INC_STATS(get_thd_id(),txn_run_time, timespan);
			if (warmup_done) {
				INC_STATS_ARR(get_thd_id(),client_client_latency, timespan);
			}
			//INC_STATS_ARR(get_thd_id(),all_lat,timespan);
			inf = client_man.dec_inflight(return_node_offset);
			DEBUG("Recv %ld from %ld, %ld -- %f\n", ((ClientResponseMessage *)msg)->txn_id,
						msg->return_node_id, inf, float(timespan) / BILLION);
			assert(inf >=0);
			// delete message here
			msgs->erase(msgs->begin());
		}
		delete msgs;
		INC_STATS(_thd_id,mtx[29], get_sys_clock() - starttime);

	}

	printf("FINISH %ld:%ld\n",_node_id,_thd_id);
	fflush(stdout);
	return FINISH;
}

RC InputThread::server_recv_loop() {

	myrand rdm;
	rdm.init(get_thd_id());
	RC rc = RCOK;
	assert (rc == RCOK);
	uint64_t starttime;

	std::vector<Message*> * msgs;
	while (!simulation->is_done()) {
		heartbeat();
		starttime = get_sys_clock();

		msgs = tport_man.recv_msg(get_thd_id());

		INC_STATS(_thd_id,mtx[28], get_sys_clock() - starttime);
		starttime = get_sys_clock();

		if (msgs == NULL) continue;
		while(!msgs->empty()) {
			Message * msg = msgs->front();
			if(msg->rtype == INIT_DONE) {
				msgs->erase(msgs->begin());
				continue;
			}
#if CC_ALG == CALVIN||CC_ALG==HDCC || CC_ALG == SNAPPER
			if(msg->rtype==CONF_STAT){
				assert(CC_ALG==HDCC);
				g_conflict_queue.push((ConflictStaticsMessage*)msg);
				msgs->erase(msgs->begin());
				continue;
			}
			if(msg->rtype == CALVIN_ACK ||(msg->rtype == CL_QRY && ISCLIENTN(msg->get_return_id())) ||
			(msg->rtype == CL_QRY_O && ISCLIENTN(msg->get_return_id()))) {
			#if LONG_TXN_WORKLOAD && LONG_TXN_SPLIT
				#if WORKLOAD == YCSB
					if (msg->rtype == CL_QRY && ((YCSBClientQueryMessage*)msg)->requests.size() == g_req_per_query) {
						split_long_transaction(msg);
					}
				#endif
			#endif
			#if LONG_TXN_WORKLOAD && (LONG_TXN_SORT || LONG_TXN_SPLIT)
				if (msg->rtype == CL_QRY) {
					work_queue.order_enqueue(get_thd_id(),msg);
				} else {
					work_queue.sequencer_enqueue(get_thd_id(),msg);
				}
			#else
				work_queue.sequencer_enqueue(get_thd_id(),msg);
			#endif
				msgs->erase(msgs->begin());
				continue;
			}
			if(msg->rtype == RDONE || msg->rtype == CL_QRY || msg->rtype == CL_QRY_O) {
				assert(ISSERVERN(msg->get_return_id()));
				work_queue.sched_enqueue(get_thd_id(),msg);
				msgs->erase(msgs->begin());
				continue;
			}
#endif
			work_queue.enqueue(get_thd_id(),msg,false);
			msgs->erase(msgs->begin());
		}
		delete msgs;
		INC_STATS(_thd_id,mtx[29], get_sys_clock() - starttime);

	}
	printf("FINISH %ld:%ld\n",_node_id,_thd_id);
	fflush(stdout);
	return FINISH;
}


// 先分析事务内操作的依赖，横向拆分子事务；
// 随后对操作数仍然很多的子事务进行纵向拆分，原则上，先按访问节点拆分，随后按节点分析一批事务的读写集，并均匀划分临时分区。实现时用HASH来快速代替。
#if WORKLOAD == YCSB
void InputThread::split_long_transaction(Message * msg) {
	YCSBClientQueryMessage * ycsb_msg = (YCSBClientQueryMessage *) msg;
	// 1. 横向切分：分为读请求和写请求
	std::vector<ycsb_request*> read_reqs;
	std::vector<ycsb_request*> write_reqs;
	for (uint64_t i = 0; i < ycsb_msg->requests.size(); i++) {
		ycsb_request * req = ycsb_msg->requests[i];
		if (req->acctype == RD) {
			read_reqs.push_back(req);
		} else if (req->acctype == WR) {
			write_reqs.push_back(req);
		}
	}

	// 2. 纵向切分：分别对读和写请求按照g_req_per_short_query拆分
	std::vector<std::vector<ycsb_request*>> sub_reqs;
	std::vector<int> sub_types; // 0:读, 1:写
	// 读请求拆分
	uint64_t read_steps = (read_reqs.size() + g_req_per_short_query - 1) / g_req_per_short_query;
	for (uint64_t i = 0; i < read_steps; i++) {
		std::vector<ycsb_request*> sub;
		uint64_t start = i * g_req_per_short_query;
		uint64_t end = std::min(start + g_req_per_short_query, (uint64_t)read_reqs.size());
		for (uint64_t j = start; j < end; j++) {
			sub.push_back(read_reqs[j]);
		}
		sub_reqs.push_back(sub);
		sub_types.push_back(0); // 读
	}
	// 写请求拆分
	uint64_t write_steps = (write_reqs.size() + g_req_per_short_query - 1) / g_req_per_short_query;
	for (uint64_t i = 0; i < write_steps; i++) {
		std::vector<ycsb_request*> sub;
		uint64_t start = i * g_req_per_short_query;
		uint64_t end = std::min(start + g_req_per_short_query, (uint64_t)write_reqs.size());
		for (uint64_t j = start; j < end; j++) {
			sub.push_back(write_reqs[j]);
		}
		sub_reqs.push_back(sub);
		#if OPEN_YCSB_DEPENDENCY
		// 打开写依赖于全部的读
		sub_types.push_back(1); // 写
		#else
		sub_types.push_back(0); // 写
		#endif
	}

	// 3. 更新ycsb_msg->sub_reqs和steps，并设置依赖关系
	ycsb_msg->sub_reqs = sub_reqs;
	// steps: 数字表示，子事务在整个事务里被执行的顺序，1代表第一波执行，2代表第二波执行，依此类推
	// 这里的实现是：所有读子事务都是第一波执行，所有写子事务都是第二波执行
	// 这样做的好处是，读子事务可以并行执行，写子事务也可以并行执行
	ycsb_msg->steps = std::vector<uint64_t>(sub_reqs.size(), 1);
	// 先激活所有读子事务
	for (size_t i = 0; i < sub_types.size(); i++) {
		if (sub_types[i] == 1) {
			ycsb_msg->steps[i] = 2;
		}
	}
	// 写子事务依赖于所有读子事务，后续调度时需判断读子事务全部完成后再激活写子事务
}
#elif WORKLOAD == TPCC
void InputThread::split_long_transaction(Message * msg) {
}
#else
void InputThread::split_long_transaction(Message * msg) {
}
#endif

void OutputThread::setup() {
	DEBUG_M("OutputThread::setup MessageThread alloc\n");
	messager = (MessageThread *) mem_allocator.alloc(sizeof(MessageThread));
	messager->init(_thd_id);
	while (!simulation->is_setup_done()) {
		messager->run();
	}
}

RC OutputThread::run() {

	tsetup();
	printf("Running OutputThread %ld\n",_thd_id);

	while (!simulation->is_done()) {
		heartbeat();
		messager->run();
	}

	printf("FINISH %ld:%ld\n",_node_id,_thd_id);
	fflush(stdout);
	return FINISH;
}



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

#include "client_query.h"
#include "mem_alloc.h"
#include "wl.h"
#include "table.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "pps_query.h"
#include <boost/algorithm/string.hpp>

/*************************************************/
//     class Query_queue
/*************************************************/

typedef struct
{
	void* context;
	int thd_id;
}FUNC_ARGS;

void
Client_query_queue::init(Workload * h_wl) {
	_wl = h_wl;


#if SERVER_GENERATE_QUERIES
	if (ISCLIENT) return;
	size = g_thread_cnt;
#else
  	size = g_servers_per_client;
#endif

#if DYNAMIC_FLAG
	std::vector<std::string> dy_write_str, dy_skew_str;
	boost::split(dy_write_str, DYNAMIC_WRITE, boost::is_any_of("|"), boost::token_compress_on);
	boost::split(dy_skew_str, DYNAMIC_SKEW, boost::is_any_of("|"), boost::token_compress_on);
	for(auto str : dy_write_str){
		dy_write.push_back(atof(str.c_str()));
	}
	for(auto str : dy_skew_str){
		dy_skew.push_back(atof(str.c_str()));
	}
	g_dy_Nbatch = std::min(dy_write.size() * dy_skew.size(), (WARMUP_TIMER + DONE_TIMER)/BILLION/SWITCH_INTERVAL);
	g_dy_batch_id = 0;
	queries.resize(g_dy_Nbatch);
	query_cnt = new uint64_t **[g_dy_Nbatch];
	for(uint32_t batch_id = 0; batch_id < g_dy_Nbatch; batch_id++){
		queries[batch_id].resize(size);
		query_cnt[batch_id] = new uint64_t*[size];
		for(uint32_t server_id = 0; server_id < size; server_id++){
			queries[batch_id][server_id].resize(g_max_txn_per_part + 4);
			query_cnt[batch_id][server_id] = new uint64_t[1];
		}
	}
#else
	query_cnt = new uint64_t * [size];
	for ( UInt32 id = 0; id < size; id ++) {
		std::vector<BaseQuery*> new_queries(g_max_txn_per_part+4,NULL);
		queries.push_back(new_queries);
		query_cnt[id] = (uint64_t*)mem_allocator.align_alloc(sizeof(uint64_t));
	}
	next_tid = 0;
#endif

	pthread_t * p_thds = new pthread_t[g_init_parallelism - 1];
	for (uint64_t i = 0; i < g_init_parallelism - 1; i++) {
		FUNC_ARGS *arg=(FUNC_ARGS*)mem_allocator.align_alloc(sizeof(FUNC_ARGS));
		arg->context=this;
		arg->thd_id=i;
		pthread_create(&p_thds[i], NULL, initQueriesHelper, (void*)arg );
	}
	FUNC_ARGS *arg=(FUNC_ARGS*)mem_allocator.align_alloc(sizeof(FUNC_ARGS));
	arg->context=this;
	arg->thd_id=g_init_parallelism - 1;

	initQueriesHelper(arg);

	for (uint32_t i = 0; i < g_init_parallelism - 1; i++) {
		pthread_join(p_thds[i], NULL);
	}

}

void *
Client_query_queue::initQueriesHelper(void * args) {
  ((Client_query_queue*)((FUNC_ARGS*)args)->context)->initQueriesParallel(((FUNC_ARGS*)args)->thd_id);

  return NULL;
}

void
Client_query_queue::initQueriesParallel(uint64_t thd_id) {
	UInt32 tid = ATOM_FETCH_ADD(next_tid, 1);
  	uint64_t request_cnt;
	request_cnt = g_max_txn_per_part + 4;

	uint32_t final_request;

	if (tid == g_init_parallelism-1) {
		final_request = request_cnt;
	} else {
		final_request = request_cnt / g_init_parallelism * (tid+1);
	}

#if WORKLOAD == YCSB
	YCSBQueryGenerator * gen = new YCSBQueryGenerator;
	gen->init();
#elif WORKLOAD == TPCC
	TPCCQueryGenerator * gen = new TPCCQueryGenerator;
	gen->init();
#elif WORKLOAD == PPS
	PPSQueryGenerator * gen = new PPSQueryGenerator;
#endif

	#if DYNAMIC_FLAG
		for(uint32_t batch_id = 0; batch_id < g_dy_Nbatch; batch_id++){
			for(uint32_t server_id = 0; server_id < g_servers_per_client; server_id++){
				for(uint32_t query_id = request_cnt / g_init_parallelism * tid; query_id < final_request; query_id++){
					queries[batch_id][server_id][query_id] = gen->create_query(_wl, server_id + g_server_start_node, batch_id);
				#if DETERMINISTIC_ABORT_MODE
					setDeterministicAbort(queries[server_id][query_id]);
				#endif
				}
			}
		}
	#else
		for ( UInt32 server_id = 0; server_id < g_servers_per_client; server_id ++) {
			for (UInt32 query_id = request_cnt / g_init_parallelism * tid; query_id < final_request;
				query_id++) {
			queries[server_id][query_id] = gen->create_query(_wl,server_id+g_server_start_node);
		#if DETERMINISTIC_ABORT_MODE
			setDeterministicAbort(queries[server_id][query_id]);
		#endif
			}
		}
	#endif


}

bool Client_query_queue::done() { return false; }

void Client_query_queue::setDeterministicAbort(BaseQuery *query) {
	double r = (double)(rand() % 10000) / 10000;
	query->isDeterministicAbort = (r < g_deterministic_abort_ratio);
}

BaseQuery *
Client_query_queue::get_next_query(uint64_t server_id,uint64_t thread_id) {
	#if DYNAMIC_FLAG
		assert(server_id < size);
		uint64_t query_id = __sync_fetch_and_add(query_cnt[g_dy_batch_id][server_id], 1);//return query_cnt[g_dy_batch_id][server_id]，then query_cnt[g_dy_batch_id][server_id]++
		if(query_id > g_max_txn_per_part) {
			__sync_bool_compare_and_swap(query_cnt[g_dy_batch_id][server_id],query_id+1,0);//if query_cnt[g_dy_batch_id][server_id]==query_id+1, then set query_cnt[g_dy_batch_id][server_id] to 0
			query_id = __sync_fetch_and_add(query_cnt[g_dy_batch_id][server_id], 1);
		}
		BaseQuery * query = queries[g_dy_batch_id][server_id][query_id];
		return query;
	#else
	assert(server_id < size);
	uint64_t query_id = __sync_fetch_and_add(query_cnt[server_id], 1);//return query_cnt[server_id]，then query_cnt[server_id]++
	if(query_id > g_max_txn_per_part) {
		__sync_bool_compare_and_swap(query_cnt[server_id],query_id+1,0);//if query_cnt[server_id]==query_id+1, then set query_cnt[server_id] to 0
		query_id = __sync_fetch_and_add(query_cnt[server_id], 1);
	}
	BaseQuery * query = queries[server_id][query_id];
	return query;
	#endif

}

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
#include "mem_alloc.h"
#include "time.h"

bool itemid_t::operator==(const itemid_t &other) const {
	return (type == other.type && location == other.location);
}

bool itemid_t::operator!=(const itemid_t &other) const { return !(*this == other); }

void itemid_t::operator=(const itemid_t &other){
	this->valid = other.valid;
	this->type = other.type;
	this->location = other.location;
	assert(*this == other);
	assert(this->valid);
}

void itemid_t::init() {
	valid = false;
	location = 0;
	next = NULL;
}

int get_thdid_from_txnid(uint64_t txnid) { return txnid % g_thread_cnt; }

uint64_t get_part_id(void *addr) { return ((uint64_t)addr / PAGE_SIZE) % g_part_cnt; }

uint64_t key_to_part(uint64_t key) {
	return key % g_part_cnt;
	// this function is called by hdcc.cpp only, another referene is actualy impossible
	// use above statment instead of below statements is acceptable for our aim
	// if (g_part_alloc)
	// 	return key % g_part_cnt;
	// else 
	// 	return 0;
}

/*
	key_to_shard(uint64_t key)
	node_num indicates which node the record lies in
	shard_number_in_node indicates the natural shard number in the node,
	for example, we have 9 records numberd from 0-8, 
	g_node_cnt=2, g_data_shard_size=3
	then we should have 4 shards like this
	NODE	|			SHARD_CONTENT
	---------------------------------------
	node 0	|			0,2,4		6,8
	node 1	|			1,3,5		7
	Globally, shard(0,2,4) is 0th shard, shard(1,3,5) is 1st shard, 
			  shard(6,8) is 2nd shard and shard(7) is 3rd shard
	Locally, the shard_number_in_node of shard(0,2,4) is 0, of shard(6,8) is 1 in node zero
			 the shard_number_in_node of shard(1,3,5) is 0, of shard(7) is 1 in node one

	NOTE: how to determine the total number of shards?
	total_number_of_shards=key_to_shard(max_key)+g_node_cnt
	for the previous example, max_key is 8, key_to_shard(8)=2, g_node_cnt=2
	so total_number_of_shards=2+2=4

	NOTE: last two steps cannot be merged into one step, that is
	key/(g_node_cnt*g_data_shard_size)*g_node_cnt DOES NOT EQUAL TO key/(g_data_shard_size)
*/
uint64_t key_to_shard(uint64_t key) {
	int node_num=key%g_part_cnt;
	int shard_number_in_node=key/(g_node_cnt*g_data_shard_size);
	return shard_number_in_node*g_node_cnt+node_num;
}

uint64_t merge_idx_key(UInt64 key_cnt, UInt64 * keys) {
	UInt64 len = 64 / key_cnt;
	UInt64 key = 0;
	for (UInt32 i = 0; i < len; i++) {
		assert(keys[i] < (1UL << len));
		key = (key << len) | keys[i];
	}
	return key;
}

uint64_t merge_idx_key(uint64_t key1, uint64_t key2) {
	assert(key1 < (1UL << 32) && key2 < (1UL << 32));
	return key1 << 32 | key2;
}

uint64_t merge_idx_key(uint64_t key1, uint64_t key2, uint64_t key3) {
	assert(key1 < (1 << 21) && key2 < (1 << 21) && key3 < (1 << 21));
	return key1 << 42 | key2 << 21 | key3;
}

uint64_t get_calvin_key(uint64_t batch_id, uint64_t return_id, uint64_t txn_id) {
	uint64_t key = (batch_id << 32) + (return_id << 24) + txn_id + 1;
	return key;
}

std::vector<uint64_t> split_calvin_key(uint64_t calvin_key) {
	std::vector<uint64_t> parts(3);
	parts[0] = (calvin_key >> 32); // batch_id
	parts[1] = (calvin_key >> 24) & 0xFF; // return_id
	parts[2] = (calvin_key & 0xFFFFFF) - 1; // txn_id
	return parts;
}

void init_globals() {
  g_max_read_req = g_node_cnt * g_inflight_max;
  g_max_pre_req = g_node_cnt * g_inflight_max;
}

void init_client_globals() {
  if(g_node_cnt > g_client_node_cnt) {
    g_servers_per_client = g_node_cnt / g_client_node_cnt;
    g_clients_per_server = 1;
  } else {
    g_servers_per_client = 1;
    g_clients_per_server = g_client_node_cnt / g_node_cnt;
  }
#if CC_ALG == BOCC || CC_ALG == FOCC || ONE_NODE_RECIEVE == 1
  g_server_start_node = 0; 
#else
  uint32_t client_node_id = g_node_id - g_node_cnt;
  g_server_start_node = (client_node_id * g_servers_per_client) % g_node_cnt; 
#endif
  if (g_node_cnt >= g_client_node_cnt && g_node_cnt % g_client_node_cnt != 0 &&
      g_node_id == (g_node_cnt + g_client_node_cnt - 1)) {
      // Have last client pick up any leftover servers if the number of
      // servers cannot be evenly divided between client nodes
      // fix the remainder to be equally distributed among clients
      g_servers_per_client += g_node_cnt % g_client_node_cnt;
  }
  printf("Node %u: servicing %u total nodes starting with node %u\n", g_node_id,
         g_servers_per_client, g_server_start_node);
}

/****************************************************/
// Global Clock!
/****************************************************/

uint64_t get_wall_clock() {
	timespec * tp = new timespec;
  clock_gettime(CLOCK_REALTIME, tp);
  uint64_t ret = tp->tv_sec * 1000000000 + tp->tv_nsec;
  delete tp;
  return ret;
}

uint64_t get_server_clock() {
#if defined(__i386__)
    uint64_t ret;
    __asm__ __volatile__("rdtsc" : "=A" (ret));
#elif defined(__x86_64__)
    unsigned hi, lo;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
    uint64_t ret = ( (uint64_t)lo)|( ((uint64_t)hi)<<32 );
	ret = (uint64_t) ((double)ret / CPU_FREQ);
#else 
	timespec * tp = new timespec;
    clock_gettime(CLOCK_REALTIME, tp);
    uint64_t ret = tp->tv_sec * 1000000000 + tp->tv_nsec;
		delete tp;
#endif
    return ret;
}

uint64_t get_sys_clock() {
  if (TIME_ENABLE) return get_server_clock();
	return 0;
}

void myrand::init(uint64_t seed) { this->seed = seed; }

uint64_t myrand::next() {
	seed = (seed * 1103515247UL + 12345UL) % (1UL<<63);
	return (seed / 65537) % RAND_MAX;
}


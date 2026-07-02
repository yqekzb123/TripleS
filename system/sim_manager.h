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

#ifndef _SIMMAN_H_
#define _SIMMAN_H_

#include "global.h"
#include <string>
#include <atomic>

// Aria
enum ARIA_PHASE {
  ARIA_INIT = -1,
  ARIA_COLLECT = 0,
  ARIA_READ,
  ARIA_RESERVATION,
  ARIA_CHECK,
  ARIA_COMMIT
};

enum SDOCC_PHASE {
  SDOCC_INIT = 0,
  SDOCC_EXECUTION,
  SDOCC_CHECK,
  SDOCC_COMMIT
};

// 用于整体阶段的，给simulation用的
// 注意，对于事务来说，只会有 INIT和EXECUTION两个阶段，剩下都是给系统用的，包括SYNC和APPEND
enum CARACAL_PHASE {
  CARACAL_COLLECT = 0,
  CARACAL_INIT,        // 第一个大阶段，初始化; ! 注意，事务只会
  CARACAL_INIT_SYNC,   // 这个阶段主要是等远程的锁都拿好了，拿到结果了
  CARACAL_APPEND,      //
  CARACAL_APPEND_SYNC,   // 这个阶段主要是等远程的读都完成了，拿到结果了
  CARACAL_EXECUTION,     // 第二个大阶段，这个阶段下应该得拆分成，读、同步、写
  CARACAL_EXECUTION_SYNC  //所谓的结束阶段
};

enum CARACAL_TXN_PHASE {
  CARACAL_TXN_ANALYSIS = 0,
  CARACAL_TXN_RD,
  CARACAL_TXN_SYNC,
  CARACAL_TXN_COLLECT,
  CARACAL_TXN_WR,
  CARACAL_TXN_WR_SYNC,
  CARACAL_TXN_DONE
};

// 服了，整一个ARIA_BARRIER类，专门用来处理ARIA的barrier，放在SimManager里，感觉SimManager里东西太多了
// 这个barrier维护一个循环数组吧，因为phase和batch只会提前一轮，所以只需要维护当前轮和下一轮的barrier就行了，数组大小为2，每轮切换的时候重置对应的barrier
template <typename PHASE>
class AriaBarrier {
public:
// 这一段是标记
  uint64_t batch_id;
  PHASE phase;
  uint64_t g_node_cnt;
// 下面这一段是检查
  uint64_t barrier_count;
  bool * barriers;
  int current_barrier_index;
  void init(uint64_t g_node_cnt, bool * barriers) {
    this->g_node_cnt = g_node_cnt;
    #if CC_ALG == ARIA
    batch_id = 1;
    #elif CC_ALG == CARACAL
    batch_id = 0;
    #else
    batch_id = 0;
    #endif
    // phase = ARIA_COLLECT;
    barrier_count = 0;
    this->barriers = barriers;
    memset(this->barriers, 0, sizeof(bool) * g_node_cnt);
    current_barrier_index = 0;
  }
  void init_batch(uint64_t batch_id){ 
    this->batch_id = batch_id;
  }
  void reset_barrier() {
    // batch_id = 0;
    barrier_count = 0;
    memset(barriers, 0, sizeof(bool) * g_node_cnt);
  }
  bool set_barrier(uint64_t node_id) {
    if (barriers[node_id]) {
      assert(false);
      return false;
    } else {
      barriers[node_id] = true;
      barrier_count++;
      return true;
    }
  }
  bool check_barrier(uint64_t node_id) {
    return barriers[node_id];
  }
  std::string get_barrier_str(std::string prefix) {
    // 帮我把整个barrier的状态打印出来吧，看看哪些节点到达了屏障，哪些没有
    std::string str = prefix + " batch_id: " + std::to_string(batch_id) + " phase: " + std::to_string(phase) + " barrier_count: " + std::to_string(barrier_count) + " barriers: ";
    for (uint64_t i = 0; i < g_node_cnt; i++) {
      str += std::to_string(i) + ":" + (barriers[i] ? "1" : "0") + " ";
    }
    str += "\n";
    return str;
    // printf("%s\n", str.c_str());
  }
};

class SimManager {
public:
	volatile bool sim_init_done;
	volatile bool warmup;
  volatile uint64_t warmup_end_time;
	bool start_set;
	volatile bool sim_done;
  uint64_t run_starttime;
  uint64_t rsp_cnt;
  uint64_t seq_epoch;
  uint64_t worker_epoch;
  uint64_t last_worker_epoch_time;
  uint64_t last_seq_epoch_time;
  int64_t epoch_txn_cnt;
  uint64_t txn_cnt;
  uint64_t inflight_cnt;
  uint64_t last_da_query_time;
  ARIA_PHASE aria_phase;
  uint64_t current_batch_id;

  uint64_t batch_process_count;
  uint64_t batch_local_process_count;
  uint64_t batch_remote_process_count;
  uint64_t batch_remote_send_count;

  // aria_barrier[0]固定用来reservation，
  // aria_barrier[1]固定用来check，两个barrier交替使用
  AriaBarrier<ARIA_PHASE> aria_barrier[2];
  uint64_t aria_barrier_index;
  // uint64_t barrier_count;
  // bool * barriers;

  std::atomic<CARACAL_PHASE> caracal_phase;
  // volatile CARACAL_PHASE caracal_phase;
  AriaBarrier<CARACAL_PHASE> caracal_barrier[3];
  uint64_t caracal_barrier_index;
  std::atomic<uint64_t> finish_append_cnt;
  std::atomic<bool> send_txn_finish;
  std::atomic<bool> get_all_txn_finish;

  void init();
  bool is_setup_done();
  bool is_done();
  bool is_warmup_done();
  void set_setup_done();
  void set_done();
  bool timeout();
  void set_starttime(uint64_t starttime);
  void process_setup_msg();
  void inc_txn_cnt();
  void inc_inflight_cnt();
  void dec_inflight_cnt();
  uint64_t get_worker_epoch();
  void next_worker_epoch();
  uint64_t get_seq_epoch();
  void advance_seq_epoch();
  void inc_epoch_txn_cnt();
  void decr_epoch_txn_cnt();
  double seconds_from_start(uint64_t time);
  void next_aria_phase();
  void next_caracal_phase();
};

#endif

#include "global.h"
#include "message.h"
#include "reorder.h"
#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <string>
#include <limits>
#include <cstddef>
#include <cstdint>
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "cc_selector.h"
#include "work_queue.h"
#include <list>
#include <deque>
#include <cmath>
#include <functional>

void SlidingWindowReorder::extract_msg_keys(Message *m, std::vector<uint64_t> &out_read_keys, std::vector<uint64_t> &out_write_keys) {
	out_read_keys.clear();
	out_write_keys.clear();
#if WORKLOAD == YCSB
	YCSBClientQueryMessage *y = (YCSBClientQueryMessage *)m;
	if (!y) return;
	out_read_keys.reserve(y->requests.size());
	out_write_keys.reserve(y->requests.size());
	for (size_t i = 0; i < y->requests.size(); ++i) {
		if (y->requests[i]->acctype == RD || y->requests[i]->acctype == SCAN) {
			out_read_keys.push_back(y->requests[i]->key);
		} else {
			out_write_keys.push_back(y->requests[i]->key);
		}
	}
#elif WORKLOAD == TPCC
	TPCCClientQueryMessage *t = (TPCCClientQueryMessage *)m;
	if (!t) return;
	// TODO: add TPCC key extraction logic if needed
#endif
}

void SlidingWindowReorder::compute_bloom_params(size_t n, double p, size_t &m_bits, size_t &k_hashes) {
	if (n == 0) n = 1;
	double ln2 = std::log(2.0);
	double m = -((double)n) * std::log(p) / (ln2 * ln2);
	size_t mb = (size_t)std::ceil(m);
	size_t k = (size_t)std::max(1.0, std::round(((double)mb / (double)n) * ln2));
	m_bits = mb;
	k_hashes = k;
}

void SlidingWindowReorder::build_bloom_for_msg(Message *m, BloomFilter &bf_read, BloomFilter &bf_write) {
	std::vector<uint64_t> read_keys, write_keys;
	extract_msg_keys(m, read_keys, write_keys);
	// build read bloom
	{
		size_t n = read_keys.size();
		const double pfp = 0.01;
		size_t m_bits = 0, k_hashes = 0;
		compute_bloom_params(n, pfp, m_bits, k_hashes);
		if (m_bits < 64) m_bits = 64;
		if (k_hashes < 1) k_hashes = 1;
		bf_read.init(m_bits, k_hashes);
		for (auto k : read_keys) bf_read.add_key(k);
	}
	// build write bloom
	{
		size_t n = write_keys.size();
		const double pfp = 0.01;
		size_t m_bits = 0, k_hashes = 0;
		compute_bloom_params(n, pfp, m_bits, k_hashes);
		if (m_bits < 64) m_bits = 64;
		if (k_hashes < 1) k_hashes = 1;
		bf_write.init(m_bits, k_hashes);
		for (auto k : write_keys) bf_write.add_key(k);
	}
}

bool SlidingWindowReorder::msg_conflicts_with_sent(Message *m) {
	if (delta == 0) return false;
	bool any_valid = false;
	for (size_t i = 0; i < (size_t)delta; ++i) if (sent_buffer[i].valid) { any_valid = true; break; }
	if (!any_valid) return false;
	std::vector<uint64_t> read_keys, write_keys;
	extract_msg_keys(m, read_keys, write_keys);
	if (read_keys.empty() && write_keys.empty()) return false;
	// !If this message depends on other messages that haven't been enqueued to the
	// sequencer yet, consider it conflicting (can't send yet).
	ClientQueryMessage * cm = (ClientQueryMessage*) m;
	if (!cm->depends_on_messages.empty()) {
		for (Message *dep : cm->depends_on_messages) {
			// treat as conflict until dependency has been enqueued to Sequencer
			ClientQueryMessage * dep_cm = (ClientQueryMessage*) dep;
			if (!dep_cm->enqueued.load()) {
				return true;
			}
		}
	}
	// For each sent entry, check three conflict types:
	//  - write-write: candidate.write vs sent.write
	//  - write-read : candidate.write vs sent.read
	//  - read-write : candidate.read  vs sent.write
	for (size_t i = 0; i < (size_t)delta; ++i) {
		if (!sent_buffer[i].valid) continue;
		const BloomFilter &sent_read = sent_buffer[i].bf_read;
		const BloomFilter &sent_write = sent_buffer[i].bf_write;
		// candidate.write vs sent.write (WW) and candidate.write vs sent.read (WR)
		for (auto k : write_keys) {
			if (sent_write.maybe_contains_key(k)) return true;
			if (sent_read.maybe_contains_key(k)) return true;
		}
		// candidate.read vs sent.write (RW)
		for (auto k : read_keys) {
			if (sent_write.maybe_contains_key(k)) return true;
		}
	}
	return false;
}

void SlidingWindowReorder::send_and_register(Message *m) {
	work_queue.sequencer_enqueue(thd_id, m);
	// mark this Message as having been enqueued to the sequencer and notify dependents
	ClientQueryMessage * cm = (ClientQueryMessage*) m;
	cm->enqueued.store(true);
	// notify dependents (decrement their enqueue_left)
	for (Message *dep : cm->dependents_ptrs) {
		ClientQueryMessage *dcm = (ClientQueryMessage*) dep;
		dcm->enqueue_left.fetch_sub(1);
	}
	DEBUG_ORDER("ReorderThread %ld send msg %p-%ld, type %d, return_node_id %ld, parent_marker %ld, origin_return_node_id %ld, sub_reqs %ld\n", thd_id, m,m->get_txn_id(), m ? m->get_rtype() : -1, m ? m->get_return_id() : -1, m ? ((ClientQueryMessage*)m)->parent_marker : -1, m ? ((ClientQueryMessage*)m)->origin_return_node_id : -1, ((ClientQueryMessage*)m)->sub_reqs_size);

	#if LONG_TXN_SORT
	// 只有打开重排序的时候，才需要记录已发送消息
	if (delta > 0) {
		SentEntry &e = sent_buffer[sent_head];
		build_bloom_for_msg(m, e.bf_read, e.bf_write);
		e.txn_id = m->get_txn_id();
		e.valid = true;
		sent_head = (sent_head + 1) % (size_t)delta;
	}
	#endif
	// update last move time to now (ns)
	last_move_time_ns = get_sys_clock();
}

void SlidingWindowReorder::trigger_window_move() {
	// Iterative window move to avoid recursion.
	std::deque<Message*> proc_q;

	// initial extraction and shift
	{
		std::list<Message*> L = std::move(delay_queues[(size_t)delta]);
		delay_queues[(size_t)delta].clear();
		for (Message *m : L) proc_q.push_back(m);
		// shift left
		for (int i = delta; i >= 1; --i) {
			delay_queues[(size_t)i] = std::move(delay_queues[(size_t)i-1]);
		}
		delay_queues[0].clear();
	}

	while (!proc_q.empty()) {
		Message *m = proc_q.front(); proc_q.pop_front();

		int cnt = ((ClientQueryMessage*)m)->delay_counts;

		if (!msg_conflicts_with_sent(m) || cnt >= max_delay) {
			// send (even if conflict but past max_delay)
			send_and_register(m);
			// After sending, perform another window shift: extract delay_queues[delta], shift left,
			// and append its contents to proc_q for processing.
			std::list<Message*> L2 = std::move(delay_queues[(size_t)delta]);
			delay_queues[(size_t)delta].clear();
			for (Message *m2 : L2) proc_q.push_back(m2);
			for (int i = delta; i >= 1; --i) {
				delay_queues[(size_t)i] = std::move(delay_queues[(size_t)i-1]);
			}
			delay_queues[0].clear();
		} else {
			// increment delay count and put back to delay_queues[0]
			((ClientQueryMessage*)m)->delay_counts ++;
			delay_queues[0].push_back(m);
		}
	}
}

// API used from run(): process a new message (returns after handling)
void SlidingWindowReorder::process_new_msg(Message *m) {
	// If this is a long-txn parent that contains multiple steps, split it here
	// into sub-messages so reorder can operate at sub-transaction granularity.
	// We only split when steps has more than one entry (i.e. it's actually splitable).
	m->txn_id = temp_txn_id++; // 目前只是临时的事务号
	// 打印msg的信息
	DEBUG_ORDER("ReorderThread %ld get msg %p-%ld, type %d, return_node_id %ld\n", thd_id, m, m->txn_id , m ? m->get_rtype() : -1, m ? m->get_return_id() : -1);
	if (LONG_TXN_WORKLOAD && LONG_TXN_SPLIT) {
		#if WORKLOAD == YCSB
		YCSBClientQueryMessage *cl = (YCSBClientQueryMessage*) m;
		if (cl && cl->steps.size() > 1) {
			// allocate a parent marker for this split group
			uint64_t parent_marker = next_parent_marker++;
			// First pass: create child messages per step
			std::vector<YCSBClientQueryMessage*> created_by_step;
			created_by_step.resize(cl->steps.size(), nullptr);
			uint64_t total_sub_reqs = 0;
			for (size_t i = 0; i < cl->steps.size(); ++i) {
				if (cl->sub_reqs[i].size() == 0) continue;
				YCSBClientQueryMessage * new_msg = (YCSBClientQueryMessage *)Message::create_message(CL_QRY);
				// inherit batch and return node
				new_msg->batch_id = cl->batch_id;
				new_msg->rtype = CL_QRY;
				new_msg->isDone = false;
				new_msg->deps_left.store(0);
				// 记录原始的大事务信息
				new_msg->origin_return_node_id = cl->return_node_id;
				// single-step
				new_msg->steps = std::vector<uint64_t>(1, cl->steps[i]);
				new_msg->original_txn_id = INVALID_ID; // will be set later by Sequencer
				new_msg->original_batch_id = INVALID_ID; // will be set later by Sequencer
				new_msg->return_node_id = g_node_id;
				new_msg->lat_network_time = 0;
				new_msg->lat_other_time = 0;
				// init requests array and copy pointers
				new_msg->requests.init(cl->sub_reqs[i].size());
				new_msg->requests.init(g_req_per_query);
				for (size_t j = 0; j < cl->sub_reqs[i].size(); ++j) {
					new_msg->requests.add(cl->sub_reqs[i][j]);
				}
				created_by_step[i] = new_msg;
				// mark grouping parent marker so Sequencer can map siblings to first child txn_id
				new_msg->parent_marker = parent_marker;
				new_msg->parent_msg = m;
				total_sub_reqs++;
			}
			// Second pass: link intra-parent dependencies (steps==2 depend on steps==1)
			for (size_t i = 0; i < cl->steps.size(); ++i) {
				YCSBClientQueryMessage * cur = created_by_step[i];
				if (!cur) continue;
				if (cl->steps[i] == 2) {
					int depcount = 0;
					for (size_t k = 0; k < cl->steps.size(); ++k) {
						if (cl->steps[k] == 1 && created_by_step[k] != nullptr) {
							depcount++;
							// record pointer dependency; also add cur to dependency's dependents_ptrs
							cur->depends_on_messages.push_back(created_by_step[k]);
							{
								pthread_mutex_lock(&created_by_step[k]->dependents_lock);
								created_by_step[k]->dependents_ptrs.push_back(cur);
								pthread_mutex_unlock(&created_by_step[k]->dependents_lock);
							}
						}
					}
					cur->deps_left.store(depcount);
					cur->enqueue_left.store(depcount);
				}
			}
			// Third pass: hand each created child into reorder pipeline
			for (size_t i = 0; i < created_by_step.size(); ++i) {
				if (!created_by_step[i]) continue;
				// initialize delay counter on message itself
				created_by_step[i]->delay_counts = 0;
				created_by_step[i]->sub_reqs_size = total_sub_reqs;
				// process each child as a separate message through reorder
				process_new_msg((Message*)created_by_step[i]);
			}
			// release parent message: reorder consumed it by splitting
			m->release();
			return;
		} 
		#endif
	}

	// 处理冲突和重排序
	#if LONG_TXN_SORT
	if (!msg_conflicts_with_sent(m)) {
		send_and_register(m);
		trigger_window_move();
	} else {
		// use message-internal delay_counts field
		((ClientQueryMessage*)m)->delay_counts = 0;
		delay_queues[0].push_back(m);
	}
	#else
	send_and_register(m);
	#endif
}

void SlidingWindowReorder::maybe_timeout_move(uint64_t now_ns, uint64_t timeout_ns) {
	if (timeout_ns == 0) return;
	if (last_move_time_ns == 0) {
		last_move_time_ns = now_ns;
		return;
	}
	if (now_ns >= last_move_time_ns && (now_ns - last_move_time_ns) >= timeout_ns) {
		// perform one window move
		trigger_window_move();
		last_move_time_ns = now_ns;
	}
}

/* 滑动窗口重排序算法例子
- **初始化**：`delay_queues = [空, 空, 空]`（索引0,1,2），`sent_buffer = []`。
- **T1到达**：无冲突，发送，更新 `sent_buffer = [T1]`，触发窗口移动：
  - 出队 `delay_queues[2]`（空），移动队列后 `delay_queues = [空, 空, 空]`。
- **T2到达**：与T1冲突，放入 `delay_queues[0]`，现在 `delay_queues = [T2, 空, 空]`。
- **T3到达**：与T1冲突，放入 `delay_queues[0]`，现在 `delay_queues = [T2+T3, 空, 空]`。
- **T4到达**：无冲突，发送，更新 `sent_buffer = [T4, T1]`，触发窗口移动：
  - 出队 `delay_queues[2]`（空），移动队列后 `delay_queues = [空, T2+T3, 空]`。
- **T5到达**：无冲突，发送，更新 `sent_buffer = [T5, T4]`，触发窗口移动：
  - 出队 `delay_queues[2]`（空），移动队列后 `delay_queues = [空, 空, T2+T3]`。
- **下一个事务发送**：触发窗口移动：
  - 出队 `delay_queues[2]`（包含T2和T3），移动队列后 `delay_queues = [空, 空, 空]`。
  - 检查T2和T3与 `sent_buffer = [T5, T4]` 的冲突：
    - 如果无冲突，发送T2和T3，并递归触发窗口移动。
    - 如果有冲突，将T2和T3放入 `delay_queues[0]`。
*/


void ReorderThread::setup() {
	// do nothing
}

RC ReorderThread::run() {
	#if LONG_TXN_WORKLOAD && (LONG_TXN_SORT || LONG_TXN_SPLIT)
	tsetup();
	Message * msg;
	uint64_t idle_starttime = 0;
	uint64_t prof_starttime = 0;
	#if LONG_TXN_SCHEDULE
	int delta = g_scheduler_thread_cnt > g_thread_cnt ? g_scheduler_thread_cnt : g_thread_cnt;
	#else
	int delta = 1;
	#endif
	int max_delay = LONG_SORT_MAX_DELAY; // 可以调整最大延迟次数

	SlidingWindowReorder swr(delta, _thd_id, max_delay);
	// int id = 0; //记录来的事务编号
	uint64_t slide_timeout_ns = 1000000ULL; // 1ms default timeout for forcing a window move
	// Main loop: process incoming ordered messages and perform reordering
	while(!simulation->is_done()) {
		uint64_t now_ns = get_sys_clock();
		// possibly trigger a window move due to timeout
		swr.maybe_timeout_move(now_ns, slide_timeout_ns);

		msg = work_queue.order_dequeue(_thd_id);

		INC_STATS(_thd_id,mtx[31],get_sys_clock() - prof_starttime);
		prof_starttime = get_sys_clock();

		if(!msg) {
			if (idle_starttime == 0) idle_starttime = get_sys_clock();
			continue;
		}

		if(idle_starttime > 0) {
			INC_STATS(_thd_id,order_idle_time,get_sys_clock() - idle_starttime);
			idle_starttime = 0;
		}

		// Delegate new message processing to sliding-window helper
		swr.process_new_msg(msg);

		// INC_STATS(_thd_id,mtx[32],get_sys_clock() - prof_starttime);
		// prof_starttime = get_sys_clock();
	}
	printf("FINISH %ld:%ld\n",_node_id,_thd_id);
	fflush(stdout);
	#endif
	return FINISH;
}

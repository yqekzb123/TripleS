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
#if CC_ALG == HDCC || CC_ALG == SNAPPER || LONG_TXN_SCHEDULE
#include "cc_selector.h"
#endif

/*
	reorder.cpp 说明（中文注释）

	这个文件实现了一组用于事务/子事务重排序的工具函数，目的是
	将高冲突事务在序列中尽量拉开，从而在确定性并发控制中减少调度/执
	行阶段的锁竞争。

	主要接口：
	- Reorder::calc_conflict_matrix(txn_list)
			计算事务对之间的冲突得分矩阵（ConflictScore），包含写写(ww)、写读(wr)、读写(rw)计数。
			矩阵的冲突计数会按 shard 热度 pstats 加权（如果可用）。

	- Reorder::schedule_transactions(txn_list, conflict_matrix, max_batch_size)
			基于冲突矩阵的简单贪心按批调度（用于快速把非冲突事务放在同一批次），
			该函数用于生成多个批次，每批尽可能包含互不冲突的事务。

	- Reorder::schedule_transactions_advanced(txn_list, delta, lambda, max_batch_size)
			更复杂的贪心距离加权调度（实现了用户提供的数学模型）：
				* 对每个候选事务，计算其与序列中最近 delta 个已放事务的加权冲突分数，
					每项按距离 k 乘以 1/k 衰减，然后选择分数最小的事务放到序列末尾；
				* 冲突类型有权重（WW=2.0, WR=1.5, RW=1.0, RR=0.0），并用 pstats 热度加权；
				* 使用 lambda 来做热度的指数衰减更新（pstats <- lambda * pstats + (1-lambda) * accessed）；
				* 根据计算得到的冲突率对 delta 做简单自适应调整（>0.3 增加，<0.1 减少）。

	备注：实现中对 YCSB workload 使用了 `YCSBClientQueryMessage->requests` 提取读写键集合。
	如果需要支持其他 workload，需要在相应分支补充提取键的逻辑。
*/


std::vector<std::vector<ConflictScore>> Reorder::calc_conflict_matrix(const std::vector<Message*>& txn_list) {
	size_t n = txn_list.size();
	// extern uint64_t *pstats; // 热度统计数组，需在外部定义
	// 分别提取每个事务的读集和写集
	uint64_t *pstats = nullptr;
	// cc_selector.pstats;
	std::vector<std::unordered_set<uint64_t>> readsets(n), writesets(n);
	std::vector<std::unordered_set<uint64_t>> readshards(n), writeshards(n);
	for (size_t i = 0; i < n; ++i) {
		#if WORKLOAD == YCSB
		YCSBClientQueryMessage* ycsb_msg = (YCSBClientQueryMessage*)txn_list[i];
		for (size_t j = 0; j < ycsb_msg->requests.size(); ++j) {
			uint64_t key = ycsb_msg->requests[j]->key;
			uint64_t shard = key_to_shard(key);
			if (ycsb_msg->requests[j]->acctype == RD) {
				readsets[i].insert(key);
				readshards[i].insert(shard);
			} else if (ycsb_msg->requests[j]->acctype == WR) {
				writesets[i].insert(key);
				writeshards[i].insert(shard);
			}
		}
		#endif
	}
	// 计算冲突矩阵，冲突因子乘以热度
	std::vector<std::vector<ConflictScore>> conflict_matrix(n, std::vector<ConflictScore>(n));
	for (size_t i = 0; i < n; ++i) {
		for (size_t j = i + 1; j < n; ++j) {
			
			for (auto key : writesets[i]) {
				// 写写
				if (writesets[j].count(key)) {
					uint64_t shard = key_to_shard(key);
					uint64_t hot = pstats ? pstats[shard] : 1;
					conflict_matrix[i][j].ww += hot;
					conflict_matrix[j][i].ww += hot;
				}
				// 写读
				if (readsets[j].count(key)) {
					uint64_t shard = key_to_shard(key);
					uint64_t hot = pstats ? pstats[shard] : 1;
					conflict_matrix[i][j].wr += hot;
					conflict_matrix[j][i].rw += hot;
				}
			}
			// 读写
			for (auto key : readsets[i]) {
				if (writesets[j].count(key)) {
					uint64_t shard = key_to_shard(key);
					uint64_t hot = pstats ? pstats[shard] : 1;
					conflict_matrix[i][j].rw += hot;
					conflict_matrix[j][i].wr += hot;
				}
			}
		}
	}
	return conflict_matrix;
}

std::vector<Message*> Reorder::schedule_transactions(const std::vector<Message*>& txn_list, const std::vector<std::vector<ConflictScore>>& conflict_matrix, int max_batch_size) {
	size_t n = txn_list.size();
	std::vector<bool> scheduled(n, false); // 标记每个事务是否已被调度
	std::vector<Message*> scheduled_txns;  // 调度后的事务顺序
	scheduled_txns.reserve(n);

	// 按批次调度，每批最多max_batch_size个事务
	for (size_t batch_start = 0; batch_start < n; ) {
		std::vector<size_t> batch_indices; // 当前批次选中的事务下标
		batch_indices.reserve(max_batch_size);
		std::unordered_set<uint64_t> batch_readshards, batch_writeshards; // 当前批次涉及的读/写分区

		// 遍历所有未调度的事务，尝试加入当前批次
		for (size_t i = 0; i < n; ++i) {
			if (scheduled[i]) continue; // 跳过已调度
			// 检查与当前批次中已选事务是否有冲突（写写、写读、读写）
			bool conflict = false;
			for (size_t idx : batch_indices) {
				if (conflict_matrix[i][idx].ww > 0 || conflict_matrix[i][idx].wr > 0 || conflict_matrix[i][idx].rw > 0) {
					// 有冲突则不能加入本批次
					conflict = true;
					break;
				}
			}
			if (!conflict) {
				// 没有冲突，加入当前批次
				batch_indices.push_back(i);
				scheduled[i] = true;
				// 更新批次的读写分区集合（可用于后续更细粒度的冲突检测或统计）
				#if WORKLOAD == YCSB
				YCSBClientQueryMessage* ycsb_msg = (YCSBClientQueryMessage*)txn_list[i];
				for (size_t j = 0; j < ycsb_msg->requests.size(); ++j) {
					uint64_t key = ycsb_msg->requests[j]->key;
					uint64_t shard = key_to_shard(key);
					if (ycsb_msg->requests[j]->acctype == RD) {
						batch_readshards.insert(shard);
					} else if (ycsb_msg->requests[j]->acctype == WR) {
						batch_writeshards.insert(shard);
					}
				}
				#endif
			}
			// 达到批次最大容量，提前结束本批次
			if (batch_indices.size() >= (size_t)max_batch_size) break;
		}

		// 将当前批次的事务加入调度结果，顺序不变
		for (size_t idx : batch_indices) {
			scheduled_txns.push_back(txn_list[idx]);
		}
		// 更新批次起点，继续调度剩余事务
		batch_start += batch_indices.size();
	}
	// 返回重排序后的事务列表
	return scheduled_txns;
}

// Advanced scheduler based on the user's mathematical model:
// - distance-weighted greedy selection within a sliding window delta
// - weighted conflict types (ww=2.0, wr=1.5, rw=1.0, rr=0.1)
// - hotness-weighted conflict via cc_selector.pstats
// - hotness exponential decay (lambda) updated from accesses
// - adaptive delta adjustment based on observed conflict rate
std::vector<Message*> Reorder::schedule_transactions_advanced(const std::vector<Message*>& txn_list, int delta, double lambda, int max_batch_size) {
	size_t N = txn_list.size();
	std::vector<Message*> output;
	output.reserve(N);

	// batch中无事务
	if (N == 0) return output;

	DEBUG_SEQ("[Reorder] advanced schedule start=%d, %ld txns need to reorder\n", delta, N);
	// 记录调度开始时间，用于度量 reorder 耗时
	uint64_t prof_start = get_sys_clock();

	// 说明：这里预定义了不同冲突类型的权重，后面的冲突计算会基于这些权重并乘以 shard 热度。
	const double w_ww = 2.0;
	const double w_wr = 1.5;
	const double w_rw = 1.5;
	

	// 热度统计数组，需在外部定义
	uint64_t *pstats = nullptr;
	// 暂时先不用冲突统计
#if CC_ALG == HDCC || CC_ALG == SNAPPER
	pstats = cc_selector.pstats;
#endif

	// Precompute pairwise conflict matrix to avoid repeated set traversals
	// This trades one O(N^2 * M) precomputation for much faster inner-loop lookups.

	auto conflict_matrix = Reorder::calc_conflict_matrix(txn_list);

	std::vector<bool> chosen(N, false);
	// chosen 标记数组：表示原始列表中某个事务是否已被放入输出序列

	std::unordered_map<uint64_t, uint64_t> key_access_count;
	// key_access_count 用于记录在新序列中被选中的事务所访问的 key 的频次，
	// 以便后续按 shard 聚合并更新 pstats（热度统计）。

	// !Greedy build: at each step pick the candidate c (not chosen) minimizing
	// score(c) = sum_{k=1..min(delta,|ordered|)} w(c, ordered[-k]) * 1/k
	// output_indices 存放已放入输出序列的事务在原始 txn_list 中的下标，
	// 便于在计算冲突时快速定位原始读写集合。
	std::vector<size_t> output_indices; 
	output_indices.reserve(N);

	std::unordered_map<uint64_t, size_t> msg_to_idx;
	// 构建事务 ID 到其在 txn_list 中下标的映射，便于快速查找依赖关系
    for (size_t i = 0; i < N; ++i) {
        // 注意：get_txn_id() 假定在 Message 接口可用并返回唯一的事务 id
        msg_to_idx[ txn_list[i]->get_txn_id() ] = i;
    }

	for (size_t placed = 0; placed < N; ++placed) {
		// quick debug: report progress per 64 placements
		// if ((placed & 0x3F) == 0) {
		// 	DEBUG_SEQ("[Reorder] placed=%zu N=%zu output_indices.size=%zu\n", placed, N, output_indices.size());
		// }
		double best_score = std::numeric_limits<double>::infinity();
		int best_idx = -1;

		// 说明：此处为主循环的第一步。我们要在所有尚未被选中的事务中
		// 找到一个得分（score）最小的候选 best_idx。得分是候选与已放入序列
		// 的最近 delta 个事务的冲突加权和，且每个冲突按距离衰减 1/k。
		// output_indices 保存已放事务在原数组中的下标，便于快速比较。
		// 遍历所有候选事务（未被选中的），计算其 score
		for (size_t i = 0; i < N; ++i) {
			if (chosen[i]) continue; // 被选过的跳过
			double score = 0.0; // 候选 i 的累积分数
			int k = 1; // 与已放事务的距离计数（1 表示最近的已放事务）
			// 从最近放入的事务向前遍历，直到超过 delta 或没有更多已放事务
			for (int back = (int)output_indices.size() - 1; back >= 0 && k <= delta; --back, ++k) {
				size_t idx_j = output_indices[back]; // 已放事务在原数组的下标
				if (idx_j >= N) {
					// DEBUG_SEQ("[Reorder] corrupt output_indices[%d]=%zu >= N=%zu\n", back, idx_j, N);
					assert(idx_j < N);
				}
				// 计算 i 与 idx_j 之间的冲突权重 w
				// Use precomputed conflict matrix to get pairwise conflict counts in O(1)
				auto &cs = conflict_matrix[i][idx_j];
				double w = w_ww * (double)cs.ww + w_wr * (double)cs.wr + w_rw * (double)cs.rw;
				score += w / (double)k;
				// early prune: if score already worse than best, stop accumulating
				if (score > best_score) break;
			}
			// 如果当前候选的分数更优，则记录为 best
			if (score < best_score) {
				best_score = score;
				best_idx = (int)i;
			}
		}
		// 临时按原序列倒序输出，关闭重排序功能
		best_idx = (int)(N - 1 - placed);
		// 临时按照原序列顺序输出，关闭重排序功能
		// best_idx = (int)placed;
		if (best_idx == -1) {
			assert(false);
			break; // nothing left (shouldn't happen)
		}

		// choose best_idx
		chosen[best_idx] = true;
		output.push_back(txn_list[best_idx]);
		output_indices.push_back((size_t)best_idx);
		assert(output.size() == output_indices.size());
		assert(output.size() <= N);

		// 更新全局访问计数：记录被选中子事务所访问到的 key，用于后续热度(pstats)
		// 的指数衰减更新。注意这里仅做频次统计，不修改 pstats 本身。
		// for (auto key : writesets[best_idx]) key_access_count[key]++;
		// for (auto key : readsets[best_idx]) key_access_count[key]++;
	}

	// 请帮我打印reorder后的事务顺序，用原本的事务号来output_indices

	// Build a comma-separated list of txn ids in the reordered output and emit via DEBUG_SEQ
	{
		std::string out_ids;
		for (size_t i = 0; i < output.size(); ++i) {
			if (i) out_ids.append(",");
			char buf[64];
			snprintf(buf, sizeof(buf), "%lu", (unsigned long)output[i]->get_txn_id());
			out_ids.append(buf);
		}
		uint64_t prof_elapsed = get_sys_clock() - prof_start;
		DEBUG_SEQ("[Reorder] advanced schedule done=%d, elapsed_ns=%llu, reordered_txn_ids=%s\n", delta, (unsigned long long)prof_elapsed, out_ids.c_str());
	}

	// // Update hotness array pstats with exponential decay: pstats = lambda*pstats + (1-lambda)*accessed
	// if (pstats) {
	// 	// 把 key 频次映射到分区（shard）集合，然后对这些分区应用指数衰减更新：
	// 	// pstats[s] = lambda * pstats[s] + (1-lambda) * accessed
	// 	// 这里只对被访问到的分区做更新，避免遍历所有分区带来的额外开销。
	// 	std::unordered_set<uint64_t> accessed_shards;
	// 	for (auto &kv : key_access_count) {
	// 		accessed_shards.insert(key_to_shard(kv.first));
	// 	}
	// 	// 对每个被访问到的 shard 应用热度更新
	// 	for (auto s : accessed_shards) {
	// 		double old = (double)pstats[s];
	// 		double inc = 1.0; // 本次观测到访问，记作 1
	// 		double updated = lambda * old + (1.0 - lambda) * inc;
	// 		pstats[s] = (uint64_t)updated;
	// 	}
	// }

	// // 统计序列中的冲突强度以便自适应调整 delta：
	// // 说明：这里计算的是已排好序列中，在每个元素之后最多 delta 范围内的冲突权重的平均值，
	// // 用来估计当前排序的冲突密度，从而决定是否扩大或缩小窗口 delta。
	// // 该统计为离线近似，仅用于调整调度参数，不会直接影响已生成的 output 序列。
	// // 遍历已排好的序列，对每个 i 考察其在后续 delta 范围内的冲突加权和，
	// // 计算总体冲突权重的平均值作为 conflict_rate 的近似。
	// // 该值用于简单地增/减 delta：冲突率高则扩展窗口，冲突率低则收缩。
	// double conflict_weight_sum = 0.0;
	// double possible_pairs = 0.0;
	// for (size_t i = 0; i < output_indices.size(); ++i) {
	// 	for (size_t j = i+1; j < output_indices.size() && j <= i + (size_t)delta; ++j) {
	// 		// compute pair conflict score (using same weights)
	// 		size_t a = output_indices[i];
	// 		size_t b = output_indices[j];
	// 		double w = 0.0;
	// 		for (auto key : writesets[a]) if (writesets[b].count(key)) w += w_ww;
	// 		for (auto key : writesets[a]) if (readsets[b].count(key)) w += w_wr;
	// 		for (auto key : readsets[a]) if (writesets[b].count(key)) w += w_rw;
	// 		for (auto key : readsets[a]) if (readsets[b].count(key)) w += w_rr;
	// 		conflict_weight_sum += w;
	// 		possible_pairs += 1.0;
	// 	}
	// }
	// double conflict_rate = possible_pairs > 0.0 ? conflict_weight_sum / possible_pairs : 0.0;

	// // 自适应调整 delta：这是一个非常简单的策略，根据冲突率阈值调整窗口大小
	// if (conflict_rate > 0.3) delta += 2;
	// else if (conflict_rate < 0.1 && delta > 1) delta -= 1;
	// // TODO: 虽然调整，但是delta还没返回给调用者
	
	return output;
}
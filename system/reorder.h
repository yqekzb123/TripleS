#ifndef _REORDER_H_
#define _REORDER_H_
#include "global.h"
#include "message.h"
#include <unordered_set>
#include <vector>

#if CC_ALG == HDCC || CC_ALG == SNAPPER || LONG_TXN_SORT
#include "cc_selector.h"
#endif
// 来一个define，分辨要不要细化冲突类型
#define CONFLICT_DETAIL true

// 计算事务之间的冲突因子，返回冲突矩阵
struct ConflictScore {
	// 最基础的冲突，写写，写读，读写都算
	// int conflict;
	// 下面是更细化的冲突类型
	int ww; // 写写
	int wr; // 写读
	int rw; // 读写

	ConflictScore() : ww(0), wr(0), rw(0) {}
};

class Reorder {
public:
    // 计算事务之间的冲突矩阵
    static std::vector<std::vector<ConflictScore>> calc_conflict_matrix(const std::vector<Message*>& txn_list);
    // 基于冲突矩阵进行调度
    static std::vector<Message*> schedule_transactions(const std::vector<Message*>& txn_list, const std::vector<std::vector<ConflictScore>>& conflict_matrix, int max_batch_size);

    
	// !更高级的贪心距离加权调度：使用冲突权重、热度衰减、自适应冲突窗口delta
	// 参数：txn_list - 原始子事务列表
	//        delta - 冲突窗口大小（δ）
	//        lambda - 热度衰减因子（0..1），用于更新热点统计
	//        max_batch_size - 可选的批次上限（如果<=0则忽略批次限制）
	static std::vector<Message*> schedule_transactions_advanced(const std::vector<Message*>& txn_list, int delta, double lambda, int max_batch_size = -1);
};
#endif

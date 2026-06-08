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

#ifndef _CARACAL_H_
#define _CARACAL_H_

#include "row.h"
#include "semaphore.h"
#include <vector>
#include <unordered_set>

// For simplicity, the txn hisotry for OCC is oganized as follows:
// 1. history is never deleted.
// 2. hisotry forms a single directional list.
//		history head -> hist_1 -> hist_2 -> hist_3 -> ... -> hist_n
//    The head is always the latest and the tail the youngest.
// 	  When history is traversed, always go from head -> tail order.

class TxnManager;
class row_t;

struct RowPtrHash {
    std::size_t operator()(row_t* row) const noexcept {
        if (row == nullptr) return 0;
        // hash函数改一下，要把table也考虑进来
        return std::hash<uint64_t>{}(row->get_primary_key()) ^ std::hash<void*>{}(row->get_table());
        // return std::hash<uint64_t>{}(row->get_primary_key());
    }
};

struct RowPtrEqual {
    bool operator()(row_t* lhs, row_t* rhs) const noexcept {
        if (lhs == rhs) return true;
        if (lhs == nullptr || rhs == nullptr) return false;
        return lhs->get_primary_key() == rhs->get_primary_key() && 
                lhs->get_table() == rhs->get_table();
    }
};

class CaracalThreadContent{
public:
	CaracalThreadContent(){}
    // ! 记录当前线程是否完成了当前阶段
    bool phase_done = false;
	// ! 这里得记录这一个epoch内，当前线程跑哪些事务
    std::vector<TxnManager*> txn_man_list;
    // !用于init phase, batch append机制，用于记录还有哪些row只是append到了临时队列上
    std::unordered_set<row_t*,RowPtrHash,RowPtrEqual> tmp_row_list;

    std::unordered_set<row_t*,RowPtrHash,RowPtrEqual> access_row_list;
};

// 用来存各个线程上的一些信息
class Caracal {
public:
	void init(int thread_cnt) {
		this->thread_cnt = thread_cnt;
		caracal_thread_list = new CaracalThreadContent[thread_cnt];
	}
	void free() {
		delete[] caracal_thread_list;
	}
	void insert_txn(uint64_t thd_id, TxnManager* txn) {
		caracal_thread_list[thd_id % thread_cnt].txn_man_list.push_back(txn);
	}
	void insert_temp_row(uint64_t thd_id, row_t* row) {
		caracal_thread_list[thd_id % thread_cnt].tmp_row_list.insert(row);
	}
    void insert_access_row(uint64_t thd_id, row_t* row) {
        caracal_thread_list[thd_id % thread_cnt].access_row_list.insert(row);
    }
    CaracalThreadContent* get_thread_content(uint64_t thd_id) {
        return &caracal_thread_list[thd_id % thread_cnt];
    }
    bool is_phase_done(uint64_t thd_id) {
        return caracal_thread_list[thd_id % thread_cnt].phase_done;
    }
    void set_phase_done(uint64_t thd_id) {
        caracal_thread_list[thd_id % thread_cnt].phase_done = true;
        DEBUG_WRK("Thread %ld set phase_done to true\n", thd_id);
    }

    void set_phase_undone_for_all() {
        DEBUG_WRK("System set all phase_done to false\n");
        for (int i = 0; i < thread_cnt; i++) {
            caracal_thread_list[i].phase_done = false;
        }
    }

	int thread_cnt;
    CaracalThreadContent* caracal_thread_list;	
};

#endif

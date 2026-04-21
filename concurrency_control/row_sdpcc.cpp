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

#include "helper.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row.h"
#include "txn.h"
#include "row_sdpcc.h"

void Row_sdpcc::init(row_t * row) {
    _row = row;
    owners_head = NULL;
    owners_tail = NULL;
    waiters_head = NULL;
    waiters_tail = NULL;
    owner_cnt = 0;
    waiter_cnt = 0;

    latch = new pthread_mutex_t;
    pthread_mutex_init(latch, NULL);

    lock_type = LOCK_NONE;
    own_starttime = 0;
}

RC Row_sdpcc::lock_get(lock_t type, TxnManager * txn) {
    RC rc;
    uint64_t starttime = get_sys_clock();
    uint64_t lock_get_start_time = starttime;
    pthread_mutex_lock( latch );
    INC_STATS(txn->get_thd_id(), mtx[17], get_sys_clock() - starttime);
    INC_STATS(txn->get_thd_id(), trans_access_lock_wait_time, get_sys_clock() - lock_get_start_time);
    if (owner_cnt > 0) {
      INC_STATS(txn->get_thd_id(),twopl_already_owned_cnt,1);
    }

    DEBUG_LK("lock (%ld,%ld): try to lock on row: %ld\n", txn->get_batch_id(),txn->get_txn_id(), _row->get_primary_key());
    LockEntry * entry = get_entry();
    entry->start_ts = get_sys_clock();
    entry->txn = txn;
    entry->type = type;

    uint64_t trace_cnt = 0;
    uint64_t trace_owners_cnt = 0;
    uint64_t trace_waiters_cnt = 0;

    if (owners_head == NULL) {
        LIST_PUT_TAIL(owners_head, owners_tail, entry);
        rc = lock_succeeded(txn, type);
    } else {
        bool isConflict = conflict_check(type);
        // 按batch_id, txn_id排序
        auto txn_cmp = [](TxnManager* a, TxnManager* b) -> bool {
            if (a->get_batch_id() != b->get_batch_id())
                return a->get_batch_id() < b->get_batch_id();
            else if (a->return_id != b->return_id) 
                return a->return_id < b->return_id;
            else return a->get_txn_id() < b->get_txn_id();
        };

        // 判断owners_tail和txn的顺序
        if (txn_cmp(owners_tail->txn, txn)) {
            // ! txn比owners_tail新，也就是说，当前事务应该塞到owners_list尾部或者waiters_list中
            // 找到waiters_list中第一个比txn新的位置
            #if 0
            // 如果从头部开始找
            // ! 下面这一段O(n)复杂度!!!
            LockEntry* pos = waiters_head;
            while (pos && txn_cmp(pos->txn, txn)) {
                pos = pos->next;
                trace_cnt++;
                trace_waiters_cnt++;
            }
            if (!isConflict && (waiters_head == NULL || pos == waiters_head)) {
                // the txn is not conflict with owners_list, AND waiters_list is empty or the txn is older than waiters_head
                // put into owners_list
                LIST_PUT_TAIL(owners_head, owners_tail, entry);
                rc = lock_succeeded(txn, type);
            } else {
                // 如果txn和owners_list冲突，或者waiters_list不为空并且txn比waiters_head新，那么就放到waiters_list中
                if (waiters_head == NULL || pos == NULL) {
                    // 如果waiters_list为空，或者txn比waiters_list中所有事务都新，那么就直接放到waiters_list尾部
                    LIST_PUT_TAIL(waiters_head, waiters_tail, entry);
                } else {
                    // 否则插入到中间位置
                    LIST_INSERT_BEFORE(pos, entry, waiters_head);
                    // LIST_INSERT_AFTER(pos, entry, waiters_tail);
                }
                // ATOM_CAS(txn->lock_ready,true,false);
                rc = lock_failed(txn);
            }
            #else 
            // 5 要插进下面的1-10，按照顺序
            // 1 2 3 4 X 6 7 8 9 10
            // 如果从尾部开始
            LockEntry* pos = waiters_tail;
            while (pos && txn_cmp(txn, pos->txn)) {
                pos = pos->prev;
                trace_cnt++;
                trace_waiters_cnt++;
            }
            // 注意: 这里是从尾部向前遍历，循环退出时 pos 的语义是
            //   pos == NULL    -> txn 比所有 waiters 都老（应该插在 waiters head 或直接进 owners，视冲突而定）
            //   pos == waiters_tail -> txn 比所有 waiters 都新（应该 append 到 waiters tail）
            //   否则 pos 指向应该插入在其之后的位置
            // 如果 txn 不和 owners_list 冲突，并且 waiters_list 为空或者 txn 比 waiters_list 中最老的还老（pos == NULL），
            // 那么就直接放到 owners_list 尾部
            if (!isConflict && (waiters_head == NULL || pos == NULL)) {
                // the txn is not conflict with owners_list, AND waiters_list is empty or the txn is older than waiters_head
                // put into owners_list
                LIST_PUT_TAIL(owners_head, owners_tail, entry);
                rc = lock_succeeded(txn, type);
            } else {
                // 如果txn和owners_list冲突，或者waiters_list不为空并且txn比waiters_head新，那么就放到waiters_list中
                if (waiters_head == NULL) {
                    // waiters 为空，直接放入 tail（等价于 head）
                    LIST_PUT_TAIL(waiters_head, waiters_tail, entry);
                } else if (pos == NULL) {
                    // txn 比所有 waiters 都老，但与 owners 冲突 -> 成为最老的 waiter（放到 head）
                    LIST_PUT_HEAD(waiters_head, waiters_tail, entry);
                } else if (pos == waiters_tail) {
                    // txn 比所有 waiters 都新 -> append 到 tail
                    LIST_PUT_TAIL(waiters_head, waiters_tail, entry);
                } else {
                    // 中间插入 -> 插在 pos 之后
                    LIST_INSERT_AFTER(pos, entry, waiters_tail);
                }
                // ATOM_CAS(txn->lock_ready,true,false);
                rc = lock_failed(txn);
            }
            #endif
        } else {    
            // ! txn比owners_tail老，需要抢锁了。。。。。。。。
            // 找到owners_list中第一个比txn新的位置
            // ! md，这里也是O(n)复杂度的
            LockEntry* pos = owners_head;
            while (pos && txn_cmp(pos->txn, txn)) {
                pos = pos->next;
                trace_cnt++;
                trace_owners_cnt++;
            }

            if (!isConflict) {
                // 如果全是lock_sh，并且txn比owners_list中所有事务都新，那么就直接放到owners_list中pos之前
                LIST_INSERT_BEFORE(pos, entry, owners_head);
                rc = lock_succeeded(txn, type);
            } else {
                // 抢占逻辑，按batch_id/txn_id判断
                /* there are 4 cases when conflict happens
                    owner    ---->   txn
                    LOCK_SH         LOCK_EX     (txn is older than all owners, then all owners become waiters, txn itself holds the lock)
                    LOCK_SH         LOCK_EX     (txn is older than partial owners, then txn itself and partial owners become waiters)
                    LOCK_EX         LOCK_SH     (there is only one owner and original owner become waiter, txn itself holds the lock)
                    LOCK_EX         LOCK_EX     (there is only one owner and original owner become waiter, txn itself holds the lock)
                */

                /*  truncate owners_list, put these txns into waiters_list, structured like below
                    owners_head  --- pos --- original_owners_tail
                    waiters_head --- waiters_tail
                */

                // deprive lock from pos to owners_tail
                // ! 要从pos到owners_tail都抢占锁，不能只抢占pos，因为可能存在多个owner都是比txn新的，并且和txn冲突的情况
                // ! 这一段也是O(n)复杂度的，感觉这个抢锁逻辑效率不太高啊
                deprive_lock(pos);

                if (pos != owners_head) {
                    // case 2, txn iteself should become waiter, add it
                    LockEntry* original_owners_tail = owners_tail;
                    assert(lock_type == LOCK_SH && type == LOCK_EX && txn_cmp(owners_head->txn, txn));
                    // set new owners tail
                    owners_tail = pos->prev;
                    owners_tail->next = NULL;

                    // link entry and pos
                    pos->prev = entry;
                    entry->next = pos;

                    // put into waiter list
                    move_to_waiter(entry, original_owners_tail);
                    // ATOM_CAS(txn->lock_ready,true,false);
                    rc = lock_failed(txn);
                } else {    // case 1, 3, 4, txn itself become the only owner for now
                    move_to_waiter(owners_head, owners_tail);
                    owners_head = owners_tail = entry;
                    rc = lock_succeeded(txn, type);
                }
            }
        }
    }

    uint64_t curr_time = get_sys_clock();
    uint64_t timespan = curr_time - starttime;
    if (rc == WAIT && txn->twopl_wait_start == 0) {
        txn->twopl_wait_start = curr_time;
    }
    txn->txn_stats.cc_time += timespan;
    txn->txn_stats.cc_time_short += timespan;
    INC_STATS(txn->get_thd_id(),twopl_getlock_time,timespan);
    INC_STATS(txn->get_thd_id(),twopl_getlock_cnt,1);

    INC_STATS(txn->get_thd_id(),twopl_lock_trace_cnt, trace_cnt);
    INC_STATS(txn->get_thd_id(),twopl_lock_trace_owners_cnt, trace_owners_cnt);
    INC_STATS(txn->get_thd_id(),twopl_lock_trace_waiters_cnt, trace_waiters_cnt);

    pthread_mutex_unlock(latch);
	return rc;
}


RC Row_sdpcc::lock_release(TxnManager * txn) {

#if WORKLOAD == PPS
    if (txn->isRecon()) {
        return RCOK;
    }
#endif

    uint64_t starttime = get_sys_clock();
    pthread_mutex_lock( latch );
    INC_STATS(txn->get_thd_id(),mtx[18],get_sys_clock() - starttime);

    DEBUG_LK("unlock (%ld,%ld): owners %d, own type %d, key %ld %lx\n", 
        txn->get_batch_id(),txn->get_txn_id(), owner_cnt, lock_type, _row->get_primary_key(), (uint64_t)_row);

    // Try to find the entry in the owners
    // TMP_DEBUG("txn: %ld release lock on row: %ld\n", txn->get_txn_id(), _row->get_primary_key());
    LockEntry * en = owners_head;
    while (en && en->txn != txn) {
        en = en->next;
    }

    assert(en);
    LIST_REMOVE_HT(en, owners_head, owners_tail);
    return_entry(en);
    owner_cnt --;
    if (owner_cnt == 0) {
        INC_STATS(txn->get_thd_id(),twopl_owned_cnt,1);
        uint64_t endtime = get_sys_clock();
        INC_STATS(txn->get_thd_id(),twopl_owned_time,endtime - own_starttime);
        if (lock_type == LOCK_SH) {
            INC_STATS(txn->get_thd_id(),twopl_sh_owned_time,endtime - own_starttime);
            INC_STATS(txn->get_thd_id(),twopl_sh_owned_cnt,1);
        } else {
            INC_STATS(txn->get_thd_id(),twopl_ex_owned_time,endtime - own_starttime);
            INC_STATS(txn->get_thd_id(),twopl_ex_owned_cnt,1);
        }
        lock_type = LOCK_NONE;
    }

    if (owner_cnt == 0) ASSERT(lock_type == LOCK_NONE);

    LockEntry * entry;
    // If any waiter can join the owners, just do it!
    while (waiters_head && !conflict_check(waiters_head->type)) {
        LIST_GET_HEAD(waiters_head, waiters_tail, entry);
#if DEBUG_TIMELINE
        printf("LOCK %ld %ld\n",entry->txn->get_txn_id(),get_sys_clock());
#endif
        DEBUG_LK("2lock (%ld,%ld): owners %d, own type %d, req type %d, key %ld %lx\n",
            entry->txn->get_batch_id(), entry->txn->get_txn_id(),  owner_cnt, lock_type, entry->type,
            _row->get_primary_key(), (uint64_t)_row);
        uint64_t timespan = get_sys_clock() - entry->txn->twopl_wait_start;
        entry->txn->twopl_wait_start = 0;

        INC_STATS(txn->get_thd_id(), twopl_wait_time, timespan);

        LIST_PUT_TAIL(owners_head, owners_tail, entry);
        owner_cnt++;


        /*
            imagine a scenario, thread 0 fetches txn 0 to lock, thread 1 fetches txn 1 to lock
            thread 1 txn 1 acquires all locks sucessfully, then calls decr_lr() == 0, 
            but before executing ATOM_CAS(lock_ready, false, true), thread 0 txn 0 finds it conflicts with txn 1 during locking process
            so it deprives lock from txn 1, and calls incr_lr() of txn 1, executes ATOM_CAS(txn->lock_ready, true, false)
            then thread 1 continues to do ATOM_CAS(lock_ready, false, true)
            now you encounter a weird situation, txn 1's lock ready is true, but it is not executable
            and after txn 0 releases its lock, makes txn 1 lock's owner, it encounters another weird situation,
            which is, below statement fails, ASSERT(entry->txn->lock_ready == false) DOES NOT hold
            So, we give up using lock_ready, instead, we use lock_ready_cnt and restart_cnt to check if a txn is executable
            for more information, refer to txn.h
        */
        // ASSERT(entry->txn->lock_ready == false);
        // entry->txn->decr_lr();
        if(entry->txn->decr_lr() == 0) {
            if(ATOM_CAS(entry->txn->lock_ready,false,true)) {
                entry->txn->txn_stats.cc_block_time += timespan;
                entry->txn->txn_stats.cc_block_time_short += timespan;
                txn_table.restart_txn(txn->get_thd_id(), entry->txn->get_txn_id(), entry->txn->get_batch_id());
                DEBUG_SCH("[SDPCC_LOCK] %ld txn %ld,%ld re-enqueue txn %ld,%ld\n", txn->get_thd_id(), txn->get_batch_id(), txn->get_txn_id(), entry->txn->get_batch_id(), entry->txn->get_txn_id());
            }
        }
        if (lock_type == LOCK_NONE) {
            own_starttime = get_sys_clock();
        }
        lock_type = entry->type;
    }

    uint64_t timespan = get_sys_clock() - starttime;
    txn->txn_stats.cc_time += timespan;
    txn->txn_stats.cc_time_short += timespan;
    INC_STATS(txn->get_thd_id(), twopl_release_time, timespan);
    INC_STATS(txn->get_thd_id(), twopl_release_cnt, 1);

    pthread_mutex_unlock(latch);
        
    return RCOK;
}

inline bool Row_sdpcc::conflict_check(lock_t type) {
    if (lock_type == LOCK_NONE) {
        return false;
    }
    return lock_type == LOCK_EX || type == LOCK_EX;
}

LockEntry * Row_sdpcc::get_entry() {
  LockEntry *entry = (LockEntry *)mem_allocator.alloc(sizeof(LockEntry));
    entry->type = LOCK_NONE;
    entry->txn = NULL;
    entry->prev = NULL;
    entry->next = NULL;
    return entry;
}
void Row_sdpcc::return_entry(LockEntry * entry) {
    mem_allocator.free(entry, sizeof(LockEntry));
}

LockEntry* Row_sdpcc::truncate_list(LockEntry* head, uint64_t txn_id) {
    LockEntry *en = head;
    while (en && en->txn->get_txn_id() < txn_id) {
        en = en->next;
    }
    return en;
}

RC Row_sdpcc::lock_succeeded(TxnManager *txn, lock_t type) {
    if(owner_cnt > 0) {
        assert(type == LOCK_SH);
        INC_STATS(txn->get_thd_id(), twopl_sh_bypass_cnt, 1);
    }
    owner_cnt++;
    if (lock_type == LOCK_NONE) {
        own_starttime = get_sys_clock();
    }
    lock_type = type;
    return RCOK;
}

RC Row_sdpcc::lock_failed(TxnManager *txn) {
    ATOM_CAS(txn->lock_ready, true, false);
    txn->incr_lr();
    return WAIT;
}

void Row_sdpcc::deprive_lock(LockEntry *start) {
    LockEntry *en = start;
    // !歪日，这个抢锁，居然是O(n)复杂度的，感觉效率不太高啊
    while (en != NULL) {
        owner_cnt--;
        en->txn->incr_lr();
        ATOM_CAS(en->txn->lock_ready, true, false);
        en = en->next;
    }
}

void Row_sdpcc::move_to_waiter(LockEntry *start, LockEntry *end) {
    if (waiters_head == NULL) {
        waiters_head = start;
        waiters_tail = end;
    } else {
        end->next = waiters_head;
        waiters_head->prev = end;
        waiters_head = start;
    }
};

bool Row_sdpcc::has_write_lock() {
    return lock_type == LOCK_EX;
}


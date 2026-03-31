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
#include "row_lock.h"

#if LONG_TXN_SCHEDULE

void Row_lock::init(row_t * row) {
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

RC Row_lock::lock_get(lock_t type, TxnManager * txn) {
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
        if (txn_cmp(owners_tail->txn, txn)) {   // txn比owners_tail新
            // 找到waiters_list中第一个比txn新的位置
            LockEntry* pos = waiters_head;
            while (pos && txn_cmp(pos->txn, txn)) pos = pos->next;

            if (!isConflict && (waiters_head == NULL || pos == waiters_head)) {
                // the txn is not conflict with owners_list, AND waiters_list is empty or the txn is older than waiters_head
                // put into owners_list
                LIST_PUT_TAIL(owners_head, owners_tail, entry);
                rc = lock_succeeded(txn, type);
            } else {
                // the txn is conflict with owners_list OR the txn should be placed into waiters_list
                if (waiters_head == NULL || pos == NULL) {
                    // waiters_list is empty or the txn is newer than waiters_tail
                    LIST_PUT_TAIL(waiters_head, waiters_tail, entry);
                } else {
                    // the txn is older than waiters_head or the txn should be placed in the middle
                    LIST_INSERT_BEFORE(pos, entry, waiters_head);
                }
                rc = lock_failed(txn);
            }
        } else {    
            // txn比owners_tail老
            // 找到owners_list中第一个比txn新的位置
            LockEntry* pos = owners_head;
            while (pos && txn_cmp(pos->txn, txn)) pos = pos->next;
            if (!isConflict) {
                // lock_type and type are both LOCK_SH, put into correct position
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

    pthread_mutex_unlock(latch);
	return rc;
}


RC Row_lock::lock_release(TxnManager * txn) {

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
        DEBUG("2lock (%ld,%ld): owners %d, own type %d, req type %d, key %ld %lx\n",
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
        entry->txn->decr_lr();
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

inline bool Row_lock::conflict_check(lock_t type) {
    if (lock_type == LOCK_NONE) {
        return false;
    }
    return lock_type == LOCK_EX || type == LOCK_EX;
}

LockEntry * Row_lock::get_entry() {
  LockEntry *entry = (LockEntry *)mem_allocator.alloc(sizeof(LockEntry));
    entry->type = LOCK_NONE;
    entry->txn = NULL;
    entry->prev = NULL;
    entry->next = NULL;
    return entry;
}
void Row_lock::return_entry(LockEntry * entry) {
    mem_allocator.free(entry, sizeof(LockEntry));
}

LockEntry* Row_lock::truncate_list(LockEntry* head, uint64_t txn_id) {
    LockEntry *en = head;
    while (en && en->txn->get_txn_id() < txn_id) {
        en = en->next;
    }
    return en;
}

RC Row_lock::lock_succeeded(TxnManager *txn, lock_t type) {
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

RC Row_lock::lock_failed(TxnManager *txn) {
    // ATOM_CAS(txn->lock_ready, true, false);
    txn->incr_lr();
    return WAIT;
}

void Row_lock::deprive_lock(LockEntry *start) {
    LockEntry *en = start;
    while (en != NULL) {
        owner_cnt--;
        en->txn->incr_lr();
        // ATOM_CAS(en->txn->lock_ready, true, false);
        en = en->next;
    }
}

void Row_lock::move_to_waiter(LockEntry *start, LockEntry *end) {
    if (waiters_head == NULL) {
        waiters_head = start;
        waiters_tail = end;
    } else {
        end->next = waiters_head;
        waiters_head->prev = end;
        waiters_head = start;
    }
};
#else
void Row_lock::init(row_t * row) {
    _row = row;
    owners_size = 1;//1031;
    owners = NULL;
    owners = (LockEntry**) mem_allocator.alloc(sizeof(LockEntry*)*owners_size);
  for (uint64_t i = 0; i < owners_size; i++) owners[i] = NULL;
    waiters_head = NULL;
    waiters_tail = NULL;
    owner_cnt = 0;
    waiter_cnt = 0;
    max_owner_ts = 0;

    latch = new pthread_mutex_t;
    pthread_mutex_init(latch, NULL);

    lock_type = LOCK_NONE;
    blatch = false;
    own_starttime = 0;

}

RC Row_lock::lock_get(lock_t type, TxnManager * txn) {
	uint64_t *txnids = NULL;
	int txncnt = 0;
	return lock_get(type, txn, txnids, txncnt);
}

RC Row_lock::lock_get(lock_t type, TxnManager * txn, uint64_t* &txnids, int &txncnt) {
    assert (CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == CALVIN);
    RC rc;
    uint64_t starttime = get_sys_clock();
    uint64_t lock_get_start_time = starttime;
    if (g_central_man) {
        glob_manager.lock_row(_row);
    } else {
        uint64_t mtx_wait_starttime = get_sys_clock();
        pthread_mutex_lock( latch );
        INC_STATS(txn->get_thd_id(),mtx[17],get_sys_clock() - mtx_wait_starttime);
    }
    INC_STATS(txn->get_thd_id(), trans_access_lock_wait_time, get_sys_clock() - lock_get_start_time);
    if(owner_cnt > 0) {
      INC_STATS(txn->get_thd_id(),twopl_already_owned_cnt,1);
    }
	bool conflict = conflict_lock(lock_type, type);
#if TWOPL_LITE
	  conflict = owner_cnt > 0;
#endif
	if (CC_ALG == WAIT_DIE && !conflict) {
		if (waiters_head && txn->get_timestamp() < waiters_head->txn->get_timestamp()) {
			conflict = true;
		}
	}
    if (CC_ALG == CALVIN && !conflict) {
    if (waiters_head) conflict = true;
    }

    if (conflict) {
    //printf("conflict! rid%ld txnid%ld ",_row->get_primary_key(),txn->get_txn_id());
        // Cannot be added to the owner list.
        if (CC_ALG == NO_WAIT) {
            rc = Abort;
      DEBUG("abort %ld,%ld %ld %lx\n", txn->get_batch_id(), txn->get_txn_id(), 
            _row->get_primary_key(), (uint64_t)_row);
      //printf("abort %ld %ld %lx\n",txn->get_txn_id(),_row->get_primary_key(),(uint64_t)_row);
            goto final;
        } else if (CC_ALG == WAIT_DIE) {
            ///////////////////////////////////////////////////////////
            //  - T is the txn currently running
            //  IF T.ts > min ts of owners
            //      T can wait
            //  ELSE
            //      T should abort
            //////////////////////////////////////////////////////////

      //bool canwait = txn->get_timestamp() > max_owner_ts;
            bool canwait = true;
            LockEntry * en;
            for(uint64_t i = 0; i < owners_size; i++) {
              en = owners[i];
              while (en != NULL) {
                assert(txn->get_txn_id() != en->txn->get_txn_id());
                assert(txn->get_timestamp() != en->txn->get_timestamp());
                if (txn->get_timestamp() > en->txn->get_timestamp()) {
            // printf("abort %ld %ld -- %ld --
            // %f\n",txn->get_txn_id(),en->txn->get_txn_id(),_row->get_primary_key(),(float)(txn->get_timestamp()
            // - en->txn->get_timestamp()) / BILLION);
                  INC_STATS(txn->get_thd_id(), twopl_diff_time,
                      (txn->get_timestamp() - en->txn->get_timestamp()));
                  canwait = false;
                  break;
                }
                en = en->next;
              }
        if (!canwait) break;
            }
            if (canwait) {
                // insert txn to the right position
                // the waiter list is always in timestamp order
                LockEntry * entry = get_entry();
                entry->start_ts = get_sys_clock();
                        entry->txn = txn;
                        entry->type = type;
                entry->start_ts = get_sys_clock();
                        entry->txn = txn;
                        entry->type = type;
                LockEntry * en;
                //txn->lock_ready = false;
                ATOM_CAS(txn->lock_ready,1,0);
                txn->incr_lr();
                en = waiters_head;
                while (en != NULL && txn->get_timestamp() < en->txn->get_timestamp()) {
                    en = en->next;
                }
                if (en) {
                    LIST_INSERT_BEFORE(en, entry,waiters_head);
                } else {
                    LIST_PUT_TAIL(waiters_head, waiters_tail, entry);
                }

                waiter_cnt ++;
        DEBUG("lk_wait (%ld,%ld): owners %d, own type %d, req type %d, key %ld %lx\n",
              txn->get_batch_id(), txn->get_txn_id(), owner_cnt, lock_type, type,
              _row->get_primary_key(), (uint64_t)_row);
                //txn->twopl_wait_start = get_sys_clock();
                rc = WAIT;
                //txn->wait_starttime = get_sys_clock();
            } else {
        DEBUG("abort (%ld,%ld): owners %d, own type %d, req type %d, key %ld %lx\n",
              txn->get_batch_id(), txn->get_txn_id(), owner_cnt, lock_type, type,
              _row->get_primary_key(), (uint64_t)_row);
              rc = Abort;
            }
        } else if (CC_ALG == CALVIN){
            LockEntry * entry = get_entry();
            entry->start_ts = get_sys_clock();
            entry->txn = txn;
            entry->type = type;
      DEBUG("lk_wait (%ld,%ld): owners %d, own type %d, req type %d, key %ld %lx\n",
            txn->get_batch_id(), txn->get_txn_id(), owner_cnt, lock_type, type,
            _row->get_primary_key(), (uint64_t)_row);
            LIST_PUT_TAIL(waiters_head, waiters_tail, entry);
            waiter_cnt ++;
            /*
            if (txn->twopl_wait_start == 0) {
                txn->twopl_wait_start = get_sys_clock();
            }
            */
            //txn->lock_ready = false;
            ATOM_CAS(txn->lock_ready,true,false);
            txn->incr_lr();
            rc = WAIT;
            //txn->wait_starttime = get_sys_clock();
        }
    } else {
    DEBUG("1lock (%ld,%ld): owners %d, own type %d, req type %d, key %ld %lx\n", 
          txn->get_batch_id(), txn->get_txn_id(), owner_cnt, lock_type, type, _row->get_primary_key(), (uint64_t)_row);
#if DEBUG_TIMELINE
        printf("LOCK %ld %ld\n",entry->txn->get_txn_id(),entry->start_ts);
#endif
#if CC_ALG != NO_WAIT
        LockEntry * entry = get_entry();
        entry->type = type;
        entry->start_ts = get_sys_clock();
        entry->txn = txn;
        STACK_PUSH(owners[hash(txn->get_txn_id())], entry);
#endif
        if(owner_cnt > 0) {
          assert(type == LOCK_SH);
          INC_STATS(txn->get_thd_id(),twopl_sh_bypass_cnt,1);
        }
        if(txn->get_timestamp() > max_owner_ts) {
          max_owner_ts = txn->get_timestamp();
        }
        owner_cnt ++;
        if(lock_type == LOCK_NONE) {
          own_starttime = get_sys_clock();
        }
        lock_type = type;
        rc = RCOK;

    }
final:
    uint64_t curr_time = get_sys_clock();
    uint64_t timespan = curr_time - starttime;
    if (rc == WAIT && txn->twopl_wait_start == 0) {
        txn->twopl_wait_start = curr_time;
    }
    txn->txn_stats.cc_time += timespan;
    txn->txn_stats.cc_time_short += timespan;
INC_STATS(txn->get_thd_id(),twopl_getlock_time,timespan);
INC_STATS(txn->get_thd_id(),twopl_getlock_cnt,1);

    if (g_central_man)
        glob_manager.release_row(_row);
    else pthread_mutex_unlock( latch );


	return rc;
}


RC Row_lock::lock_release(TxnManager * txn) {

#if CC_ALG == CALVIN
    if (txn->isRecon()) {
        return RCOK;
    }
#endif
    uint64_t starttime = get_sys_clock();
      if (g_central_man)
          glob_manager.lock_row(_row);
      else {
      uint64_t mtx_wait_starttime = get_sys_clock();
          pthread_mutex_lock( latch );
      INC_STATS(txn->get_thd_id(),mtx[18],get_sys_clock() - mtx_wait_starttime);
    }

  DEBUG("unlock (%ld,%ld): owners %d, own type %d, key %ld %lx\n", 
        txn->get_batch_id(), txn->get_txn_id(), owner_cnt, lock_type, _row->get_primary_key(), (uint64_t)_row);

      // If CC is NO_WAIT or WAIT_DIE, txn should own this lock
      // What about Calvin?
#if CC_ALG == NO_WAIT
      assert(owner_cnt > 0);
      owner_cnt--;
      if (owner_cnt == 0) {
        INC_STATS(txn->get_thd_id(),twopl_owned_cnt,1);
        uint64_t endtime = get_sys_clock();
        INC_STATS(txn->get_thd_id(),twopl_owned_time,endtime - own_starttime);
        if(lock_type == LOCK_SH) {
          INC_STATS(txn->get_thd_id(),twopl_sh_owned_time,endtime - own_starttime);
          INC_STATS(txn->get_thd_id(),twopl_sh_owned_cnt,1);
    } else {
          INC_STATS(txn->get_thd_id(),twopl_ex_owned_time,endtime - own_starttime);
          INC_STATS(txn->get_thd_id(),twopl_ex_owned_cnt,1);
        }
        lock_type = LOCK_NONE;
      }

#else

      // Try to find the entry in the owners
      LockEntry * en = owners[hash(txn->get_txn_id())];
      LockEntry * prev = NULL;

      while (en != NULL && en->txn != txn) {
          prev = en;
          en = en->next;
      }

      if (en) { // find the entry in the owner list
    if (prev)
      prev->next = en->next;
    else
      owners[hash(txn->get_txn_id())] = en->next;
          return_entry(en);
          owner_cnt --;
      if (owner_cnt == 0) {
        INC_STATS(txn->get_thd_id(),twopl_owned_cnt,1);
        uint64_t endtime = get_sys_clock();
        INC_STATS(txn->get_thd_id(),twopl_owned_time,endtime - own_starttime);
        if(lock_type == LOCK_SH) {
          INC_STATS(txn->get_thd_id(),twopl_sh_owned_time,endtime - own_starttime);
          INC_STATS(txn->get_thd_id(),twopl_sh_owned_cnt,1);
      } else {
          INC_STATS(txn->get_thd_id(),twopl_ex_owned_time,endtime - own_starttime);
          INC_STATS(txn->get_thd_id(),twopl_ex_owned_cnt,1);
        }
        lock_type = LOCK_NONE;
      }

    } else {
      assert(false);
          en = waiters_head;
    while (en != NULL && en->txn != txn) en = en->next;
          ASSERT(en);

          LIST_REMOVE(en);
    if (en == waiters_head) waiters_head = en->next;
    if (en == waiters_tail) waiters_tail = en->prev;
          return_entry(en);
          waiter_cnt --;
      }
#endif

  if (owner_cnt == 0) ASSERT(lock_type == LOCK_NONE);
#if DEBUG_ASSERT && CC_ALG == WAIT_DIE
      for (en = waiters_head; en != NULL && en->next != NULL; en = en->next)
        assert(en->next->txn->get_timestamp() < en->txn->get_timestamp());
      for (en = waiters_head; en != NULL && en->next != NULL; en = en->next)
        assert(en->txn->get_txn_id() !=txn->get_txn_id());
#endif

      LockEntry * entry;
      // If any waiter can join the owners, just do it!
      while (waiters_head && !conflict_lock(lock_type, waiters_head->type)) {
          LIST_GET_HEAD(waiters_head, waiters_tail, entry);
#if DEBUG_TIMELINE
          printf("LOCK %ld %ld\n",entry->txn->get_txn_id(),get_sys_clock());
#endif
    DEBUG("2lock (%ld,%ld): owners %d, own type %d, req type %d, key %ld %lx\n",
          entry->txn->get_batch_id(), entry->txn->get_txn_id(), owner_cnt, lock_type, entry->type,
          _row->get_primary_key(), (uint64_t)_row);
          uint64_t timespan = get_sys_clock() - entry->txn->twopl_wait_start;
          entry->txn->twopl_wait_start = 0;
#if CC_ALG != CALVIN
          entry->txn->txn_stats.cc_block_time += timespan;
          entry->txn->txn_stats.cc_block_time_short += timespan;
#endif
          INC_STATS(txn->get_thd_id(),twopl_wait_time,timespan);

#if CC_ALG != NO_WAIT
          STACK_PUSH(owners[hash(entry->txn->get_txn_id())], entry);
#endif
          owner_cnt ++;
          waiter_cnt --;
          if(entry->txn->get_timestamp() > max_owner_ts) {
              max_owner_ts = entry->txn->get_timestamp();
          }
          ASSERT(entry->txn->lock_ready == false);
      //if(entry->txn->decr_lr() == 0 && entry->txn->locking_done) {
          if(entry->txn->decr_lr() == 0) {
              if(ATOM_CAS(entry->txn->lock_ready,false,true)) {
#if CC_ALG == CALVIN
                  entry->txn->txn_stats.cc_block_time += timespan;
                  entry->txn->txn_stats.cc_block_time_short += timespan;
#endif
        txn_table.restart_txn(txn->get_thd_id(), entry->txn->get_txn_id(),
                              entry->txn->get_batch_id());
              }
          }
          if(lock_type == LOCK_NONE) {
              own_starttime = get_sys_clock();
          }
          lock_type = entry->type;
#if CC_AlG == NO_WAIT
          return_entry(entry);
#endif
      }

      uint64_t timespan = get_sys_clock() - starttime;
      txn->txn_stats.cc_time += timespan;
      txn->txn_stats.cc_time_short += timespan;
      INC_STATS(txn->get_thd_id(),twopl_release_time,timespan);
      INC_STATS(txn->get_thd_id(),twopl_release_cnt,1);

      if (g_central_man)
          glob_manager.release_row(_row);
      else
          pthread_mutex_unlock( latch );


    return RCOK;
}

bool Row_lock::conflict_lock(lock_t l1, lock_t l2) {
    if (l1 == LOCK_NONE || l2 == LOCK_NONE)
        return false;
    else if (l1 == LOCK_EX || l2 == LOCK_EX)
        return true;
    else
        return false;
}

LockEntry * Row_lock::get_entry() {
  LockEntry *entry = (LockEntry *)mem_allocator.alloc(sizeof(LockEntry));
    entry->type = LOCK_NONE;
    entry->txn = NULL;
    //DEBUG_M("row_lock::get_entry alloc %lx\n",(uint64_t)entry);
    return entry;
}
void Row_lock::return_entry(LockEntry * entry) {
    //DEBUG_M("row_lock::return_entry free %lx\n",(uint64_t)entry);
    mem_allocator.free(entry, sizeof(LockEntry));
}
#endif

bool Row_lock::has_write_lock() {
    return lock_type == LOCK_EX;
}


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
#include "row_lock.h"

#ifndef ROW_SDPCC_H
#define ROW_SDPCC_H

class Row_sdpcc {
public:
  void init(row_t * row);
  RC lock_get(lock_t type, TxnManager * txn);
  RC lock_release(TxnManager * txn);
  bool has_write_lock();

private:
  pthread_mutex_t* latch;
	bool 		conflict_check(lock_t type);
	LockEntry* get_entry();
	void 		return_entry(LockEntry* entry);
  LockEntry* truncate_list(LockEntry* head, uint64_t txn_id);
  RC lock_succeeded(TxnManager *txn, lock_t type);
  RC lock_failed(TxnManager *txn);
  void deprive_lock(LockEntry *start);
  void move_to_waiter(LockEntry *start, LockEntry *end);
	row_t * _row;
  lock_t lock_type;
  UInt32 owner_cnt;
  UInt32 waiter_cnt;

	// owners and waiters are double linked list
	// waiters head is the oldest txn, tail is the youngest txn, so new txns are inserted into the tail
    // all txns in owners list are older than waiters head
  LockEntry * owners_head;
  LockEntry * owners_tail;
	LockEntry * waiters_head;
	LockEntry * waiters_tail;
  uint64_t own_starttime;

};

#endif

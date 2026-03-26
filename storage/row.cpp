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
#include "table.h"
#include "catalog.h"
#include "row.h"
#include "txn.h"

#include "catalog.h"
#include "global.h"
#include "mem_alloc.h"
#include "row_lock.h"
#include "row_occ.h"
#include "row_null.h"
#include "row_silo.h"
#include "row_aria.h"
#include "mem_alloc.h"
#include "manager.h"

#define SIM_FULL_ROW true

RC row_t::init(table_t *host_table, uint64_t part_id, uint64_t row_id) {
	part_info = true;
	_row_id = row_id;
	_part_id = part_id;
	this->table = host_table;
	Catalog * schema = host_table->get_schema();
	tuple_size = schema->get_tuple_size();
#if SIM_FULL_ROW
	data = (char *) mem_allocator.alloc(sizeof(char) * tuple_size);
#else
	data = (char *) mem_allocator.alloc(sizeof(uint64_t) * 1);
#endif
	return RCOK;
}

RC row_t::switch_schema(table_t *host_table) {
	this->table = host_table;
	return RCOK;
}

void row_t::init_manager(row_t * row) {
#if MODE==NOCC_MODE || MODE==QRY_ONLY_MODE
	return;
#endif
	DEBUG_M("row_t::init_manager alloc \n");
#if CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == CALVIN
	manager = (Row_lock *) mem_allocator.align_alloc(sizeof(Row_lock));
#elif CC_ALG == OCC
	manager = (Row_occ *) mem_allocator.align_alloc(sizeof(Row_occ));
#elif CC_ALG == CNULL
	manager = (Row_null *) mem_allocator.align_alloc(sizeof(Row_null));
#elif CC_ALG == SILO
    manager = (Row_silo *) mem_allocator.align_alloc(sizeof(Row_silo));
#elif CC_ALG == ARIA
	manager = (Row_aria *) mem_allocator.align_alloc(sizeof(Row_aria));
#endif
	manager->init(this);
}

table_t *row_t::get_table() { return table; }

Catalog *row_t::get_schema() { return get_table()->get_schema(); }

const char *row_t::get_table_name() { return get_table()->get_table_name(); };
uint64_t row_t::get_tuple_size() { return get_schema()->get_tuple_size(); }

uint64_t row_t::get_field_cnt() { return get_schema()->field_cnt; }

void row_t::set_value(int id, void * ptr) {
	int datasize = get_schema()->get_field_size(id);
	int pos = get_schema()->get_field_index(id);
	DEBUG("set_value pos %d datasize %d -- %lx\n", pos, datasize, (uint64_t)this);
#if SIM_FULL_ROW
	memcpy( &data[pos], ptr, datasize);
#else
	char d[tuple_size];
	memcpy( &d[pos], ptr, datasize);
#endif
}

void row_t::set_value(int id, void * ptr, int size) {
	int pos = get_schema()->get_field_index(id);
#if SIM_FULL_ROW
	memcpy( &data[pos], ptr, size);
#else
	char d[tuple_size];
	memcpy( &d[pos], ptr, size);
#endif
}

void row_t::set_value(const char * col_name, void * ptr) {
	uint64_t id = get_schema()->get_field_id(col_name);
	set_value(id, ptr);
}

SET_VALUE(uint64_t);
SET_VALUE(int64_t);
SET_VALUE(double);
SET_VALUE(UInt32);
SET_VALUE(SInt32);

GET_VALUE(uint64_t);
GET_VALUE(int64_t);
GET_VALUE(double);
GET_VALUE(UInt32);
GET_VALUE(SInt32);

char * row_t::get_value(int id) {
	int pos __attribute__ ((unused));
	pos = get_schema()->get_field_index(id);
	DEBUG("get_value pos %d -- %lx\n",pos,(uint64_t)this);
#if SIM_FULL_ROW
	return &data[pos];
#else
	return data;
#endif
}

char * row_t::get_value(char * col_name) {
	uint64_t pos __attribute__ ((unused));
	pos = get_schema()->get_field_index(col_name);
#if SIM_FULL_ROW
	return &data[pos];
#else
	return data;
#endif
}

char *row_t::get_data() { return data; }

void row_t::set_data(char * data) {
	int tuple_size = get_schema()->get_tuple_size();
#if SIM_FULL_ROW
	memcpy(this->data, data, tuple_size);
#else
	char d[tuple_size];
	memcpy(d, data, tuple_size);
#endif
}
// copy from the src to this
void row_t::copy(row_t * src) {
	assert(src->get_schema() == this->get_schema());
#if SIM_FULL_ROW
	set_data(src->get_data());
#else
	char d[tuple_size];
	set_data(d);
#endif
}

void row_t::free_row() {
	DEBUG_M("row_t::free_row free\n");
#if SIM_FULL
	mem_allocator.free(data, sizeof(char) * get_tuple_size());
#else
	mem_allocator.free(data, sizeof(uint64_t) * 1);
#endif
}

RC row_t::get_lock(access_t type, TxnManager * txn) {
	RC rc = RCOK;
#if CC_ALG == CALVIN
	lock_t lt = (type == RD || type == SCAN)? LOCK_SH : LOCK_EX;
	rc = this->manager->lock_get(lt, txn);
#endif
	return rc;
}

RC row_t::get_row(access_t type, TxnManager *txn, Access *access) {
  RC rc = RCOK;
#if MODE==NOCC_MODE || MODE==QRY_ONLY_MODE
	access->data = this;
		return rc;
#endif
#if ISOLATION_LEVEL == NOLOCK
	access->data = this;
		return rc;
#endif
	/*
#if ISOLATION_LEVEL == READ_UNCOMMITTED
	if(type == RD) {
	access->data = this;
		return rc;
	}
#endif
*/
#if CC_ALG == CNULL
  uint64_t init_time = get_sys_clock();
	txn->cur_row = (row_t *) mem_allocator.alloc(sizeof(row_t));
	txn->cur_row->init(get_table(), get_part_id());
  INC_STATS(txn->get_thd_id(), trans_cur_row_init_time, get_sys_clock() - init_time);

	rc = this->manager->access(type,txn);

  uint64_t copy_time = get_sys_clock();
	txn->cur_row->copy(this);
	access->data = txn->cur_row;
	assert(rc == RCOK);
  INC_STATS(txn->get_thd_id(), trans_cur_row_copy_time, get_sys_clock() - copy_time);
	goto end;
#endif

#if CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT
  uint64_t init_time = get_sys_clock();
	//uint64_t thd_id = txn->get_thd_id();
	lock_t lt = (type == RD || type == SCAN) ? LOCK_SH : LOCK_EX; // ! this wrong !!
  INC_STATS(txn->get_thd_id(), trans_cur_row_init_time, get_sys_clock() - init_time);

	rc = this->manager->lock_get(lt, txn);

  	uint64_t copy_time = get_sys_clock();
	if (rc == RCOK) {
		access->data = this;
	} else if (rc == Abort) {
	} else if (rc == WAIT) {
		ASSERT(CC_ALG == WAIT_DIE);
	}
  INC_STATS(txn->get_thd_id(), trans_cur_row_copy_time, get_sys_clock() - copy_time);
	goto end;

#elif CC_ALG == OCC
	// OCC always make a local copy regardless of read or write
  uint64_t init_time = get_sys_clock();
	DEBUG_M("row_t::get_row OCC alloc \n");
	txn->cur_row = (row_t *) mem_allocator.alloc(sizeof(row_t));
	txn->cur_row->init(get_table(), get_part_id());
  INC_STATS(txn->get_thd_id(), trans_cur_row_init_time, get_sys_clock() - init_time);

	rc = this->manager->access(txn, R_REQ);

  uint64_t copy_time = get_sys_clock();
	access->data = txn->cur_row;
  INC_STATS(txn->get_thd_id(), trans_cur_row_copy_time, get_sys_clock() - copy_time);
	goto end;

#elif CC_ALG == SILO
	// like OCC, tictoc also makes a local copy for each read/write
  uint64_t init_time = get_sys_clock();
 	DEBUG_M("row_t::get_row SILO alloc \n");
	txn->cur_row = (row_t *) mem_allocator.alloc(sizeof(row_t));
	txn->cur_row->init(get_table(), get_part_id());
	TsType ts_type = (type == RD)? R_REQ : P_REQ;
  INC_STATS(txn->get_thd_id(), trans_cur_row_init_time, get_sys_clock() - init_time);

	rc = this->manager->access(txn, ts_type, txn->cur_row);

  uint64_t copy_time = get_sys_clock();
  access->data = txn->cur_row;
  INC_STATS(txn->get_thd_id(), trans_cur_row_copy_time, get_sys_clock() - copy_time);
	goto end;

#elif CC_ALG == ARIA
	uint64_t init_time = get_sys_clock();
	DEBUG_M("row_t::get_row ARIA alloc \n");
	txn->cur_row = (row_t *) mem_allocator.alloc(sizeof(row_t));
	txn->cur_row->init(get_table(), get_part_id());
	INC_STATS(txn->get_thd_id(), trans_cur_row_init_time, get_sys_clock() - init_time);
	txn->cur_row->copy(this);
	uint64_t copy_time = get_sys_clock();
	access->data = txn->cur_row;
	INC_STATS(txn->get_thd_id(), trans_cur_row_copy_time, get_sys_clock() - copy_time);
	goto end;
#elif CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC || CC_ALG == CALVIN
#if CC_ALG == HSTORE_SPEC
	if(txn_table.spec_mode) {
		DEBUG_M("row_t::get_row HSTORE_SPEC alloc \n");
		txn->cur_row = (row_t *) mem_allocator.alloc(sizeof(row_t));
		txn->cur_row->init(get_table(), get_part_id());
		rc = this->manager->access(txn, R_REQ);
		access->data = txn->cur_row;
		goto end;
	}
#endif
	access->data = this;
	goto end;
#else
	assert(false);
#endif

end:
	return rc;
}

// Return call for get_row if waiting
RC row_t::get_row_post_wait(access_t type, TxnManager * txn, row_t *& row) {
	RC rc = RCOK;
  uint64_t init_time = get_sys_clock();
	assert(CC_ALG == WAIT_DIE);
#if CC_ALG == WAIT_DIE
	assert(txn->lock_ready);
	rc = RCOK;
	//ts_t endtime = get_sys_clock();
	row = this;
#endif
  	INC_STATS(txn->get_thd_id(), trans_cur_row_init_time, get_sys_clock() - init_time);
	return rc;
}

// the "row" is the row read out in get_row(). For locking based CC_ALG,
// the "row" is the same as "this". For timestamp based CC_ALG, the
// "row" != "this", and the "row" must be freed.
uint64_t row_t::return_row(RC rc, access_t type, TxnManager *txn, row_t *row) {
#if MODE==NOCC_MODE || MODE==QRY_ONLY_MODE
	return 0;
#endif
#if ISOLATION_LEVEL == NOLOCK
	return 0;
#endif
	/*
#if ISOLATION_LEVEL == READ_UNCOMMITTED
	if(type == RD) {
		return;
	}
#endif
*/
#if CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == CALVIN
	assert (row == NULL || row == this || type == XP);
	if (CC_ALG != CALVIN && ROLL_BACK &&
			type == XP) {  // recover from previous writes. should not happen w/ Calvin
		this->copy(row);
	}
	this->manager->lock_release(txn);
	return 0;
#elif CC_ALG == OCC 
	assert (row != NULL);
	if (type == WR) manager->write(row, txn->get_end_timestamp());
	row->free_row();
	DEBUG_M("row_t::return_row OCC free \n");
	mem_allocator.free(row, sizeof(row_t));
	manager->release();
	return 0;
#elif CC_ALG == CNULL
	assert (row != NULL);
	if (rc == Abort) {
		manager->abort(type,txn);
	} else {
		manager->commit(type,txn);
	}

		row->free_row();
	DEBUG_M("row_t::return_row Maat free \n");
		mem_allocator.free(row, sizeof(row_t));
	return 0;
#elif CC_ALG == SILO
	assert (row != NULL);
	row->free_row();
    DEBUG_M("row_t::return_row XP free \n");
	mem_allocator.free(row, sizeof(row_t));
	return 0;
#elif CC_ALG == ARIA
	assert(row != NULL);
	row->free_row();
	mem_allocator.free(row, sizeof(row_t));
	return 0;
#else
	assert(false);
#endif
}

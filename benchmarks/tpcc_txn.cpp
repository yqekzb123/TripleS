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

#include "tpcc.h"
#include "tpcc_query.h"
#include "tpcc_helper.h"
#include "query.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "tpcc_const.h"
#include "transport.h"
#include "msg_queue.h"
#include "message.h"
#include "aria.h"
#if CC_ALG == CALVIN
#include "row_lock.h"
#endif

void TPCCTxnManager::init(uint64_t thd_id, Workload * h_wl) {
	TxnManager::init(thd_id, h_wl);
	_wl = (TPCCWorkload *) h_wl;
	reset();
	TxnManager::reset();
}

void TPCCTxnManager::reset() {
	TPCCQuery* tpcc_query = (TPCCQuery*) query;
	state = TPCC_PAYMENT0;
	if(tpcc_query->txn_type == TPCC_PAYMENT) {
		state = TPCC_PAYMENT0;
	} else if (tpcc_query->txn_type == TPCC_NEW_ORDER) {
		state = TPCC_NEWORDER0;
	} else if (tpcc_query->txn_type == TPCC_ORDER_STATUS) {
		state = TPCC_ORDER_STATUS0;
	} else if (tpcc_query->txn_type == TPCC_DELIVERY) {
		state = TPCC_DELIVERY0;
	} else if (tpcc_query->txn_type == TPCC_STOCK_LEVEL) {
		state = TPCC_STOCK_LEVEL0;
	}
	next_item_id = 0;
	district_row = NULL;
	items = NULL;
	leaf = NULL;
	sum_amount = 0;
	leaf_traversal_cnt = 0;
	s_i_id = 0;
	s_i_ids.clear();
	TxnManager::reset();
}

RC TPCCTxnManager::run_txn_post_wait() {
	uint64_t starttime = get_sys_clock();
	get_row_post_wait(row);
	uint64_t curr_time = get_sys_clock();
	txn_stats.process_time += curr_time - starttime;
	txn_stats.process_time_short += curr_time - starttime;

	next_tpcc_state();
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - curr_time);
	return RCOK;
}


RC TPCCTxnManager::run_txn() {
#if MODE == SETUP_MODE
	return RCOK;
#endif
	RC rc = RCOK;
	uint64_t starttime = get_sys_clock();

#if CC_ALG == CALVIN
	rc = run_calvin_txn();
	return rc;
#endif

	if(IS_LOCAL(txn->txn_id) && (state == TPCC_PAYMENT0 || state == TPCC_NEWORDER0 || state == TPCC_ORDER_STATUS0 || state == TPCC_DELIVERY0 || state == TPCC_STOCK_LEVEL0)) {
		DEBUG("Running txn %ld\n",txn->txn_id);
#if DISTR_DEBUG
		query->print();
#endif
		query->partitions_touched.add_unique(GET_PART_ID(0,g_node_id));
	}


	while(rc == RCOK && !is_done()) {
		rc = run_txn_state();
	}

	uint64_t curr_time = get_sys_clock();
	txn_stats.process_time += curr_time - starttime;
	txn_stats.process_time_short += curr_time - starttime;

	if(IS_LOCAL(get_txn_id())) {
	#if DETERMINISTIC_ABORT_MODE
		if (query->isDeterministicAbort && rc == RCOK && is_done()) {
			query->isDeterministicAbort = false;
			rc = Abort;
			INC_STATS(get_thd_id(), deterministic_abort_cnt_silo, 1);
		}
	#endif
		if(is_done() && rc == RCOK)
			rc = start_commit();
		else if(rc == Abort)
			rc = start_abort();
	}

	return rc;

}

bool TPCCTxnManager::is_done() {
	bool done = false;
	TPCCQuery* tpcc_query = (TPCCQuery*) query;
	switch(tpcc_query->txn_type) {
		case TPCC_PAYMENT:
			//done = state == TPCC_PAYMENT5 || state == TPCC_FIN;
			done = state == TPCC_FIN;
			break;
		case TPCC_NEW_ORDER:
			//done = next_item_id == tpcc_query->ol_cnt || state == TPCC_FIN;
			done = next_item_id >= tpcc_query->items.size() || state == TPCC_FIN;
			break;
		case TPCC_ORDER_STATUS:
			done = items == NULL;
			if (!done) {
				items = items->next;
			}
			break;
		case TPCC_DELIVERY:
			done = items == NULL;
			if (!done) {
				items = items->next;
			}
			break;
		case TPCC_STOCK_LEVEL:
			done = next_item_id == 20;
			break;
		default:
			assert(false);
	}

	return done;
}

RC TPCCTxnManager::acquire_locks() {
	uint64_t starttime = get_sys_clock();
	assert(CC_ALG == CALVIN);
	locking_done = false;
	RC rc = RCOK;
	RC rc2;
	INDEX * index;
	itemid_t * item;
	row_t* row;
	uint64_t key;
	incr_lr();
	TPCCQuery* tpcc_query = (TPCCQuery*) query;

	uint64_t w_id = tpcc_query->w_id;
	uint64_t d_id = tpcc_query->d_id;
	uint64_t c_id = tpcc_query->c_id;
	uint64_t d_w_id = tpcc_query->d_w_id;
	uint64_t c_w_id = tpcc_query->c_w_id;
	uint64_t c_d_id = tpcc_query->c_d_id;
	char * c_last = tpcc_query->c_last;
	uint64_t part_id_w = wh_to_part(w_id);
	uint64_t part_id_c_w = wh_to_part(c_w_id);
	switch(tpcc_query->txn_type) {
		case TPCC_PAYMENT:
			if(GET_NODE_ID(part_id_w) == g_node_id) {
			// WH
				index = _wl->i_warehouse;
				item = index_read(index, w_id, part_id_w);
				row_t * row = ((row_t *)item->location);
				rc2 = get_lock(row,g_wh_update? WR:RD);
				if (rc2 != RCOK) rc = rc2;

			// Dist
				key = distKey(d_id, d_w_id);
				item = index_read(_wl->i_district, key, part_id_w);
				row = ((row_t *)item->location);
				rc2 = get_lock(row, WR);
				if (rc2 != RCOK) rc = rc2;
			}
			if(GET_NODE_ID(part_id_c_w) == g_node_id) {
			// Cust
				if (tpcc_query->by_last_name) {

					key = custNPKey(c_last, c_d_id, c_w_id);
					index = _wl->i_customer_last;
					item = index_read(index, key, part_id_c_w);
					int cnt = 0;
					itemid_t * it = item;
					itemid_t * mid = item;
					while (it != NULL) {
						cnt ++;
						it = it->next;
						if (cnt % 2 == 0) mid = mid->next;
					}
					row = ((row_t *)mid->location);

				} else {
					key = custKey(c_id, c_d_id, c_w_id);
					index = _wl->i_customer_id;
					item = index_read(index, key, part_id_c_w);
					row = (row_t *) item->location;
				}
				rc2  = get_lock(row, WR);
				if (rc2 != RCOK) rc = rc2;
			}
			break;
		case TPCC_NEW_ORDER:
			if(GET_NODE_ID(part_id_w) == g_node_id) {
			// WH
				index = _wl->i_warehouse;
				item = index_read(index, w_id, part_id_w);
				row_t * row = ((row_t *)item->location);
				rc2 = get_lock(row,RD);
				if (rc2 != RCOK) rc = rc2;
			// Cust
				index = _wl->i_customer_id;
				key = custKey(c_id, d_id, w_id);
				item = index_read(index, key, wh_to_part(w_id));
				row = (row_t *) item->location;
				rc2 = get_lock(row, RD);
				if (rc2 != RCOK) rc = rc2;
			// Dist
				key = distKey(d_id, w_id);
				item = index_read(_wl->i_district, key, wh_to_part(w_id));
				row = ((row_t *)item->location);
				rc2 = get_lock(row, WR);
				if (rc2 != RCOK) rc = rc2;
#if TXN_TYPE == TPCC_ALL
			// Order
				bt_node * leaf;
				_wl->i_order->leaf_row_access(UINT64_MAX, LF_LAST, wd_to_part(w_id, d_id), this, leaf, row);
				rc2 = get_lock(row, WR);
				if (rc2 != RCOK) rc = rc2;
			// New Order
				_wl->i_neworder->leaf_row_access(UINT64_MAX, LF_LAST, wd_to_part(w_id, d_id), this, leaf, row);
				rc2 = get_lock(row, WR);
				if (rc2 != RCOK) rc = rc2;
#endif
			}
			// Items
			for(uint64_t i = 0; i < tpcc_query->ol_cnt; i++) {
				if (GET_NODE_ID(wh_to_part(tpcc_query->items[i]->ol_supply_w_id)) != g_node_id) continue;
				key = tpcc_query->items[i]->ol_i_id;
				item = index_read(_wl->i_item, key, 0);
				row = ((row_t *)item->location);
				rc2 = get_lock(row, RD);
				if (rc2 != RCOK) rc = rc2;
				key = stockKey(tpcc_query->items[i]->ol_i_id, tpcc_query->items[i]->ol_supply_w_id);
				index = _wl->i_stock;
				item = index_read(index, key, wh_to_part(tpcc_query->items[i]->ol_supply_w_id));
				row = ((row_t *)item->location);
				rc2 = get_lock(row, WR);
				if (rc2 != RCOK) rc = rc2;
#if TXN_TYPE == TPCC_ALL
				bt_node * leaf __attribute__((unused));
				_wl->i_orderline->leaf_row_access(UINT64_MAX, LF_LAST, wd_to_part(w_id, d_id), this, leaf, row);
				rc2 = get_lock(row, WR);
				if (rc2 != RCOK) rc = rc2;
#endif
			}
			break;
		case TPCC_ORDER_STATUS:
			if (tpcc_query->by_last_name) {
				key = custNPKey(c_last, d_id, w_id);
				index = _wl->i_customer_last;
				item = index_read(index, key, part_id_c_w);
				int cnt = 0;
				itemid_t * it = item;
				itemid_t * mid = item;
				while (it != NULL) {
					cnt ++;
					it = it->next;
					if (cnt % 2 == 0) mid = mid->next;
				}
				row = ((row_t *)mid->location);
				row->get_value(C_ID, c_id);
			} else {
				key = custKey(c_id, d_id, w_id);
				index = _wl->i_customer_id;
				item = index_read(index, key, part_id_c_w);
				row = (row_t *) item->location;
			}
			rc2  = get_lock(row, RD);
			if (rc2 != RCOK) rc = rc2;

			key = custKey(c_id, d_id, w_id);
			index = _wl->i_order_cust;
			item = index_read(index, key, wh_to_part(w_id));
			row = (row_t *) item->location;
#if CC_ALG == CALVIN
			while (row->manager->has_write_lock()) {
				item = item->next;
				row = (row_t *)item->location;
			}
#endif
			uint64_t o_id;
			row->get_value(O_ID, o_id);
			tpcc_query->o_id = o_id;
			rc2 = get_lock(row, RD);
			if (rc2!= RCOK) rc = rc2;

			key = orderlineKey(w_id, d_id, o_id);
			_wl->i_orderline->index_read(key, items, wd_to_part(w_id, d_id), get_thd_id(), this);
			while (items != NULL) {
				row = (row_t *)items->location;
				rc2 = get_lock(row, RD);
				if (rc2!= RCOK) rc = rc2;
				items = items->next;
			}
			break;
		case TPCC_DELIVERY:
#if TXN_TYPE == TPCC_ALL
			bt_node * leaf;
			_wl->i_neworder->leaf_row_access(0, LF_FIRST, wd_to_part(w_id, d_id), this, leaf, row);
			rc2 = get_lock(row, WR);
			if (rc2 != RCOK) rc = rc2;
			row = NULL;
			row_t * temp;
			while (row == NULL) {
				for (uint32_t i = 0; i < leaf->num_keys - 1; i++) {
					item = (itemid_t *)leaf->pointers[i];
					if (!item->valid) continue;
					temp = (row_t *)item->location;
#if CC_ALG == CAVLIN
					if (temp->manager->has_write_lock()) continue;
#endif
					row = temp;
					break;
				}
				leaf = leaf->next;
			}
			rc2 = get_lock(row, WR);
			if (rc2 != RCOK) rc = rc2;
			row->get_value(NO_O_ID, tpcc_query->o_id);
			key = orderPrimaryKey(w_id, d_id, tpcc_query->o_id);
			_wl->i_order->index_read(key, item, wd_to_part(w_id, d_id), get_thd_id(), this);
			row = (row_t *)item->location;
			rc2 = get_lock(row, WR);
			if (rc2 != RCOK) rc = rc2;
			row->get_value(O_C_ID, c_id);
			index = _wl->i_customer_id;
			key = custKey(c_id, d_id, w_id);
			item = index_read(index, key, wh_to_part(w_id));
			row = (row_t *) item->location;
			rc2 = get_lock(row, WR);
			if (rc2 != RCOK) rc = rc2;
#endif
			break;
		case TPCC_STOCK_LEVEL:
			key = distKey(d_id, w_id);
			index = _wl->i_district;
			item = index_read(index, key, part_id_w);
			row = (row_t *) item->location;
			rc2 = get_lock(row, WR);
			if (rc2 != RCOK) rc = rc2;
#if TXN_TYPE == TPCC_ALL
			_wl->i_orderline->leaf_row_access(UINT64_MAX, LF_LAST, wd_to_part(w_id, d_id), this, leaf, row);
			rc2 = get_lock(row, WR);
			if (rc2 != RCOK) rc = rc2;
#endif
			break;
		default:
			assert(false);
	}
	if(decr_lr() == 0) {
		if (ATOM_CAS(lock_ready, false, true)) rc = RCOK;
	}
	txn_stats.wait_starttime = get_sys_clock();
	locking_done = true;
	INC_STATS(get_thd_id(),calvin_sched_time,get_sys_clock() - starttime);
	return rc;
}

#if CC_ALG == SNAPPER
void TPCCTxnManager::get_read_write_set() {
  	uint64_t starttime = get_sys_clock();
	INDEX * index;
	itemid_t * item;
	row_t* row;
	uint64_t key;
	TPCCQuery* tpcc_query = (TPCCQuery*) query;
	uint64_t w_id = tpcc_query->w_id;
	uint64_t d_id = tpcc_query->d_id;
	uint64_t c_id = tpcc_query->c_id;
	uint64_t d_w_id = tpcc_query->d_w_id;
	uint64_t c_w_id = tpcc_query->c_w_id;
	uint64_t c_d_id = tpcc_query->c_d_id;
	char * c_last = tpcc_query->c_last;
	uint64_t part_id_w = wh_to_part(w_id);
	uint64_t part_id_c_w = wh_to_part(c_w_id);
	switch(tpcc_query->txn_type) {
		case TPCC_PAYMENT:
			if(GET_NODE_ID(part_id_w) == g_node_id) {
			// WH
				index = _wl->i_warehouse;
				item = index_read(index, w_id, part_id_w);
				row_t * row = ((row_t *)item->location);
				read_write_set.emplace_back(&*row, g_wh_update? WR:RD);
				wait_for_locks.insert(&*row);
			// Dist
				key = distKey(d_id, d_w_id);
				item = index_read(_wl->i_district, key, part_id_w);
				row = ((row_t *)item->location);
				read_write_set.emplace_back(&*row, WR);
				wait_for_locks.insert(&*row);
			}
			if(GET_NODE_ID(part_id_c_w) == g_node_id) {
			// Cust
				if (tpcc_query->by_last_name) {

					key = custNPKey(c_last, c_d_id, c_w_id);
					index = _wl->i_customer_last;
					item = index_read(index, key, part_id_c_w);
					int cnt = 0;
					itemid_t * it = item;
					itemid_t * mid = item;
					while (it != NULL) {
						cnt ++;
						it = it->next;
						if (cnt % 2 == 0) mid = mid->next;
					}
					row = ((row_t *)mid->location);

				} else {
					key = custKey(c_id, c_d_id, c_w_id);
					index = _wl->i_customer_id;
					item = index_read(index, key, part_id_c_w);
					row = (row_t *) item->location;
				}
				read_write_set.emplace_back(&*row, WR);
				wait_for_locks.insert(&*row);
			}
			break;
		case TPCC_NEW_ORDER:
			if(GET_NODE_ID(part_id_w) == g_node_id) {
			// WH
				index = _wl->i_warehouse;
				item = index_read(index, w_id, part_id_w);
				row_t * row = ((row_t *)item->location);
				read_write_set.emplace_back(&*row, RD);
				wait_for_locks.insert(&*row);
			// Cust
				index = _wl->i_customer_id;
				key = custKey(c_id, d_id, w_id);
				item = index_read(index, key, wh_to_part(w_id));
				row = (row_t *) item->location;
				read_write_set.emplace_back(&*row, RD);
				wait_for_locks.insert(&*row);
			// Dist
				key = distKey(d_id, w_id);
				item = index_read(_wl->i_district, key, wh_to_part(w_id));
				row = ((row_t *)item->location);
				read_write_set.emplace_back(&*row, WR);
				wait_for_locks.insert(&*row);
			}
			// Items
			for(uint64_t i = 0; i < tpcc_query->ol_cnt; i++) {
				if (GET_NODE_ID(wh_to_part(tpcc_query->items[i]->ol_supply_w_id)) != g_node_id) continue;
				key = tpcc_query->items[i]->ol_i_id;
				item = index_read(_wl->i_item, key, 0);
				row = ((row_t *)item->location);
				read_write_set.emplace_back(&*row, RD);
				wait_for_locks.insert(&*row);
				key = stockKey(tpcc_query->items[i]->ol_i_id, tpcc_query->items[i]->ol_supply_w_id);
				index = _wl->i_stock;
				item = index_read(index, key, wh_to_part(tpcc_query->items[i]->ol_supply_w_id));
				row = ((row_t *)item->location);
				read_write_set.emplace_back(&*row, WR);
				wait_for_locks.insert(&*row);
			}
			break;
		default:
			assert(false);
	}

	INC_STATS(get_thd_id(),calvin_sched_time,get_sys_clock() - starttime);
}

RC TPCCTxnManager::acquire_lock(row_t * row, access_t acctype) {
  RC rc = WAIT;
//   uint64_t starttime = get_sys_clock();
  RC rc2 = get_lock(row,acctype);
  if (rc2 == RCOK) {
    if (wait_for_locks.empty()) {
		// printf("start txn: %ld\n", get_txn_id());
        if (ATOM_CAS(lock_ready, false, true)) rc = RCOK;
    }
  }
  return rc;
}
#endif

void TPCCTxnManager::next_tpcc_state() {
	//TPCCQuery* tpcc_query = (TPCCQuery*) query;

	switch(state) {
		case TPCC_PAYMENT_S:
			state = TPCC_PAYMENT0;
			break;
		case TPCC_PAYMENT0:
			state = TPCC_PAYMENT1;
			break;
		case TPCC_PAYMENT1:
			state = TPCC_PAYMENT2;
			break;
		case TPCC_PAYMENT2:
			state = TPCC_PAYMENT3;
			break;
		case TPCC_PAYMENT3:
			state = TPCC_PAYMENT4;
			break;
		case TPCC_PAYMENT4:
			state = TPCC_PAYMENT5;
			break;
		case TPCC_PAYMENT5:
			state = TPCC_FIN;
			break;
		case TPCC_NEWORDER_S:
			state = TPCC_NEWORDER0;
			break;
		case TPCC_NEWORDER0:
			state = TPCC_NEWORDER1;
			break;
		case TPCC_NEWORDER1:
			state = TPCC_NEWORDER2;
			break;
		case TPCC_NEWORDER2:
			state = TPCC_NEWORDER3;
			break;
		case TPCC_NEWORDER3:
			state = TPCC_NEWORDER4;
			break;
		case TPCC_NEWORDER4:
			state = TPCC_NEWORDER5;
			break;
		case TPCC_NEWORDER5:
			state = TPCC_NEWORDER5_1;
			break;
		case TPCC_NEWORDER5_1:
			state = TPCC_NEWORDER5_2;
			break;
		case TPCC_NEWORDER5_2:
			state = TPCC_NEWORDER9_1;
			break;
		case TPCC_NEWORDER9_1:
			if(!IS_LOCAL(txn->txn_id) || !is_done()) {
				state = TPCC_NEWORDER6;
			} else {
				state = TPCC_FIN;
			}
			break;
		case TPCC_NEWORDER6: // loop pt 1
			state = TPCC_NEWORDER7;
			break;
		case TPCC_NEWORDER7:
			state = TPCC_NEWORDER8;
			break;
		case TPCC_NEWORDER8: // loop pt 2
			state = TPCC_NEWORDER9;
			break;
		case TPCC_NEWORDER9:
			++next_item_id;
			if(!IS_LOCAL(txn->txn_id) || !is_done()) {
				state = TPCC_NEWORDER6;
			} else {
				state = TPCC_FIN;
			}
			break;
		case TPCC_ORDER_STATUS0:
			state = TPCC_ORDER_STATUS1;
			break;
		case TPCC_ORDER_STATUS1:
			state = TPCC_ORDER_STATUS2;
			break;
		case TPCC_ORDER_STATUS2:
			state = TPCC_ORDER_STATUS3;
			break;
		case TPCC_ORDER_STATUS3:
			if (is_done()) {
				state = TPCC_FIN;
			}
		case TPCC_DELIVERY0:
			state = TPCC_DELIVERY1;
			break;
		case TPCC_DELIVERY1:
			state = TPCC_DELIVERY2;
			break;
		case TPCC_DELIVERY2:
			state = TPCC_DELIVERY3;
			break;
		case TPCC_DELIVERY3:
			state = TPCC_DELIVERY4;
			break;
		case TPCC_DELIVERY4:
			state = TPCC_DELIVERY5;
			break;
		case TPCC_DELIVERY5:
			state = TPCC_DELIVERY6;
			break;
		case TPCC_DELIVERY6:
			state = TPCC_DELIVERY7;
			break;
		case TPCC_DELIVERY7:
			if (!is_done()) {
				state = TPCC_DELIVERY6;
			} else {
				state = TPCC_DELIVERY8;
			}
			break;
		case TPCC_DELIVERY8:
			state = TPCC_DELIVERY9;
			break;
		case TPCC_DELIVERY9:
			state = TPCC_FIN;
			break;
		case TPCC_STOCK_LEVEL0:
			state = TPCC_STOCK_LEVEL1;
			break;
		case TPCC_STOCK_LEVEL1:
			state = TPCC_STOCK_LEVEL2;
			break;
		case TPCC_STOCK_LEVEL2:
			state = TPCC_STOCK_LEVEL3;
			break;
		case TPCC_STOCK_LEVEL3:
			state = TPCC_STOCK_LEVEL4;
			break;
		case TPCC_STOCK_LEVEL4:
			state = TPCC_STOCK_LEVEL5;
			break;
		case TPCC_STOCK_LEVEL5:
			if (!is_done()) {
				state = TPCC_STOCK_LEVEL3;
			} else {
				state = TPCC_FIN;
			}
		case TPCC_FIN:
			break;
		default:
			assert(false);
	}

}

bool TPCCTxnManager::is_local_item(uint64_t idx) {
	TPCCQuery* tpcc_query = (TPCCQuery*) query;
	uint64_t ol_supply_w_id = tpcc_query->items[idx]->ol_supply_w_id;
	uint64_t part_id_ol_supply_w = wh_to_part(ol_supply_w_id);
	return GET_NODE_ID(part_id_ol_supply_w) == g_node_id;
}


RC TPCCTxnManager::send_remote_request() {
	assert(IS_LOCAL(get_txn_id()));
	TPCCQuery* tpcc_query = (TPCCQuery*) query;
	TPCCRemTxnType next_state = TPCC_FIN;
	uint64_t w_id = tpcc_query->w_id;
	uint64_t c_w_id = tpcc_query->c_w_id;
	uint64_t dest_node_id = UINT64_MAX;
	if(state == TPCC_PAYMENT0) {
		dest_node_id = GET_NODE_ID(wh_to_part(w_id));
		next_state = TPCC_PAYMENT2;
	} else if(state == TPCC_PAYMENT4) {
		dest_node_id = GET_NODE_ID(wh_to_part(c_w_id));
		next_state = TPCC_FIN;
	} else if(state == TPCC_NEWORDER0) {
		dest_node_id = GET_NODE_ID(wh_to_part(w_id));
		next_state = TPCC_NEWORDER6;
	} else if(state == TPCC_NEWORDER8) {
		dest_node_id = GET_NODE_ID(wh_to_part(tpcc_query->items[next_item_id]->ol_supply_w_id));
		/*
		while(GET_NODE_ID(wh_to_part(tpcc_query->items[next_item_id]->ol_supply_w_id)) != dest_node_id)
		{
			msg->items.add(tpcc_query->items[next_item_id++]);
		}
		*/
		if(is_done())
			next_state = TPCC_FIN;
		else
			next_state = TPCC_NEWORDER6;
	} else {
		assert(false);
	}
	TPCCQueryMessage * msg = (TPCCQueryMessage*)Message::create_message(this,RQRY);
	msg->state = state;
	query->partitions_touched.add_unique(GET_PART_ID(0,dest_node_id));
	msg_queue.enqueue(get_thd_id(),msg,dest_node_id);
	state = next_state;
	return WAIT_REM;
}

void TPCCTxnManager::copy_remote_items(TPCCQueryMessage * msg) {
	TPCCQuery* tpcc_query = (TPCCQuery*) query;
	msg->items.init(tpcc_query->items.size());
	if (tpcc_query->txn_type == TPCC_PAYMENT) return;
	uint64_t dest_node_id = GET_NODE_ID(wh_to_part(tpcc_query->items[next_item_id]->ol_supply_w_id));
	while (next_item_id < tpcc_query->items.size() && !is_local_item(next_item_id) &&
				 GET_NODE_ID(wh_to_part(tpcc_query->items[next_item_id]->ol_supply_w_id)) == dest_node_id) {
		Item_no * req = (Item_no*) mem_allocator.alloc(sizeof(Item_no));
		req->copy(tpcc_query->items[next_item_id++]);
		msg->items.add(req);
	}
}


RC TPCCTxnManager::run_txn_state() {
	uint64_t starttime = get_sys_clock();
	TPCCQuery* tpcc_query = (TPCCQuery*) query;
	uint64_t w_id = tpcc_query->w_id;
	uint64_t d_id = tpcc_query->d_id;
	uint64_t c_id = tpcc_query->c_id;
	uint64_t d_w_id = tpcc_query->d_w_id;
	uint64_t c_w_id = tpcc_query->c_w_id;
	uint64_t c_d_id = tpcc_query->c_d_id;
	char * c_last = tpcc_query->c_last;
	double h_amount = tpcc_query->h_amount;
	bool by_last_name = tpcc_query->by_last_name;
	bool remote = tpcc_query->remote;
	uint64_t ol_cnt = tpcc_query->ol_cnt;
	uint64_t o_entry_d = tpcc_query->o_entry_d;
	uint64_t ol_i_id = 0;
	uint64_t ol_supply_w_id = 0;
	uint64_t ol_quantity = 0;
	if(tpcc_query->txn_type == TPCC_NEW_ORDER) {
			ol_i_id = tpcc_query->items[next_item_id]->ol_i_id;
			ol_supply_w_id = tpcc_query->items[next_item_id]->ol_supply_w_id;
			ol_quantity = tpcc_query->items[next_item_id]->ol_quantity;
	}
	uint64_t ol_number = next_item_id;
	uint64_t ol_amount = tpcc_query->ol_amount;
	uint64_t o_id = tpcc_query->o_id;

	uint64_t part_id_w = wh_to_part(w_id);
	uint64_t part_id_c_w = wh_to_part(c_w_id);
	uint64_t part_id_ol_supply_w = wh_to_part(ol_supply_w_id);
	bool w_loc = GET_NODE_ID(part_id_w) == g_node_id;
	bool c_w_loc = GET_NODE_ID(part_id_c_w) == g_node_id;
	bool ol_supply_w_loc = GET_NODE_ID(part_id_ol_supply_w) == g_node_id;

	itemid_t * item = NULL;

	RC rc = RCOK;
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	switch (state) {
		case TPCC_PAYMENT0 :
						if(w_loc)
										rc = run_payment_0(w_id, d_id, d_w_id, h_amount, row);
						else {
							rc = send_remote_request();
						}
						break;
		case TPCC_PAYMENT1 :
						rc = run_payment_1(w_id, d_id, d_w_id, h_amount, row);
						break;
		case TPCC_PAYMENT2 :
						rc = run_payment_2(w_id, d_id, d_w_id, h_amount, row);
						break;
		case TPCC_PAYMENT3 :
						rc = run_payment_3(w_id, d_id, d_w_id, h_amount, row);
						break;
		case TPCC_PAYMENT4 :
						if(c_w_loc)
								rc = run_payment_4( w_id,  d_id, c_id, c_w_id,  c_d_id, c_last, h_amount, by_last_name, row);
						else {
								rc = send_remote_request();
						}
						break;
		case TPCC_PAYMENT5 :
						rc = run_payment_5( w_id,  d_id, c_id, c_w_id,  c_d_id, c_last, h_amount, by_last_name, row);
						break;
		case TPCC_NEWORDER0 :
						if(w_loc)
								rc = new_order_0( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
						else {
								rc = send_remote_request();
						}
			break;
		case TPCC_NEWORDER1 :
						rc = new_order_1( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
						break;
		case TPCC_NEWORDER2 :
						rc = new_order_2( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
						break;
		case TPCC_NEWORDER3 :
						rc = new_order_3( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
						break;
		case TPCC_NEWORDER4 :
						rc = new_order_4( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
						break;
		case TPCC_NEWORDER5 :
						rc = new_order_5( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
						break;
		case TPCC_NEWORDER5_1 :
						rc = new_order_5_1( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
						break;
		case TPCC_NEWORDER5_2 :
						rc = new_order_5_2( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
						break;
		case TPCC_NEWORDER9_1 :
						rc = new_order_9_1( w_id, d_id, remote, tpcc_query->o_id, row);
						break;
		case TPCC_NEWORDER6 :
			rc = new_order_6(ol_i_id, row);
			break;
		case TPCC_NEWORDER7 :
			rc = new_order_7(ol_i_id, row);
			break;
		case TPCC_NEWORDER8 :
					if(ol_supply_w_loc) {
				rc = new_order_8(w_id, d_id, remote, ol_i_id, ol_supply_w_id, ol_quantity, ol_number, o_id,
												 row);
			} else {
									rc = send_remote_request();
					}
							break;
		case TPCC_NEWORDER9 :
			rc = new_order_9(w_id, d_id, remote, ol_i_id, ol_supply_w_id, ol_quantity, ol_number,
											 ol_amount, o_id, row);
						break;
		case TPCC_ORDER_STATUS0 :
			rc = run_order_status_0(w_id, d_id, by_last_name, c_id, c_last, row);
			break;
		case TPCC_ORDER_STATUS1 :
			rc = run_order_status_1(w_id, d_id, c_id, tpcc_query->o_id, row);
			break;
		case TPCC_ORDER_STATUS2 :
			rc = run_order_status_2(w_id, d_id, tpcc_query->o_id, items, row);
			break;
		case TPCC_ORDER_STATUS3 :
			row = (row_t *)items->location;
			rc = run_order_status_3(row);
			break;
		case TPCC_DELIVERY0 :
			rc = run_delivery_0(w_id, d_id, tpcc_query->o_id, row);
			break;
		case TPCC_DELIVERY1 :
			rc = run_delivery_1(tpcc_query->o_id, row);
			break;
		case TPCC_DELIVERY2 :
			rc = run_delivery_2(tpcc_query->o_id, row);
			break;
		case TPCC_DELIVERY3 :
			rc = run_delivery_3(w_id, d_id, tpcc_query->o_id, row);
			break;
		case TPCC_DELIVERY4 :
			rc = run_delivery_4(tpcc_query->o_carrier_id, c_id, row);
			break;
		case TPCC_DELIVERY5 :
			rc = run_delivery_5(w_id, d_id, tpcc_query->o_id, items);
			break;
		case TPCC_DELIVERY6 :
			row = (row_t *)items->location;
			rc = run_delivery_6(row);
			break;
		case TPCC_DELIVERY7 :
			rc = run_delivery_7(tpcc_query->ol_delivery_d, sum_amount, row);
			break;
		case TPCC_DELIVERY8 :
			rc = run_delivery_8(w_id, d_id, c_id, row);
			break;
		case TPCC_DELIVERY9 :
			rc = run_delivery_9(sum_amount, row);
			break;
		case TPCC_STOCK_LEVEL0 :
			rc = run_stock_level_0(w_id, d_id, row);
			break;
		case TPCC_STOCK_LEVEL1 :
			rc = run_stock_level_1(tpcc_query->o_id, row);
			break;
		case TPCC_STOCK_LEVEL2 :
			rc = run_stock_level_2(w_id, d_id, tpcc_query->o_id, leaf, row);
			break;
		case TPCC_STOCK_LEVEL3 :
			if (leaf_traversal_cnt == 0) {
				leaf_traversal_cnt = leaf->num_keys;
			}
			if (leaf_traversal_cnt > 0) {
				item = (itemid_t *)leaf->pointers[leaf_traversal_cnt-1];
				if (item != NULL) {
					row = (row_t *)item->location;
					rc = run_stock_level_3(w_id, d_id, tpcc_query->o_id, row);
				} else {
					rc = Abort;
				}
			} else {
				rc = Abort;
			}
			break;
		case TPCC_STOCK_LEVEL4 :
			rc = run_stock_level_4(w_id, s_i_id, row);
			break;
		case TPCC_STOCK_LEVEL5 :
			rc = run_stock_level_5(w_id, s_i_id, tpcc_query->threshold, s_i_ids, row);
			leaf_traversal_cnt--;
			if (leaf_traversal_cnt == 0) {
				leaf = leaf->prev;
				if (leaf == NULL) {
					state = TPCC_FIN;
				}
				leaf_traversal_cnt = leaf->num_keys;
			}
			next_item_id ++;
			break;
		case TPCC_FIN :
				state = TPCC_FIN;
			if (tpcc_query->rbk) return Abort;
						//return finish(tpcc_query,false);
				break;
		default:
				assert(false);
	}
	starttime = get_sys_clock();
	if (rc == RCOK) next_tpcc_state();
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return rc;
}

inline RC TPCCTxnManager::run_payment_0(uint64_t w_id, uint64_t d_id, uint64_t d_w_id,
																				double h_amount, row_t *&r_wh_local) {

	uint64_t starttime = get_sys_clock();
	uint64_t key;
	itemid_t * item;
/*====================================================+
			EXEC SQL UPDATE warehouse SET w_ytd = w_ytd + :h_amount
		WHERE w_id=:w_id;
	+====================================================*/
	/*===================================================================+
		EXEC SQL SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name
		INTO :w_street_1, :w_street_2, :w_city, :w_state, :w_zip, :w_name
		FROM warehouse
		WHERE w_id=:w_id;
	+===================================================================*/


	RC rc;
	key = w_id;
	INDEX * index = _wl->i_warehouse;
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	item = index_read(index, key, wh_to_part(w_id));
	starttime = get_sys_clock();
	assert(item != NULL);
	row_t * r_wh = ((row_t *)item->location);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	if (g_wh_update)
		rc = get_row(r_wh, WR, r_wh_local);
	else
		rc = get_row(r_wh, RD, r_wh_local);

	return rc;
}

inline RC TPCCTxnManager::run_payment_1(uint64_t w_id, uint64_t d_id, uint64_t d_w_id,
																				double h_amount, row_t *r_wh_local) {

	assert(r_wh_local != NULL);
/*====================================================+
			EXEC SQL UPDATE warehouse SET w_ytd = w_ytd + :h_amount
		WHERE w_id=:w_id;
	+====================================================*/
	/*===================================================================+
		EXEC SQL SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name
		INTO :w_street_1, :w_street_2, :w_city, :w_state, :w_zip, :w_name
		FROM warehouse
		WHERE w_id=:w_id;
	+===================================================================*/

	uint64_t starttime = get_sys_clock();
	double w_ytd;
	r_wh_local->get_value(W_YTD, w_ytd);

	if (g_wh_update) {
		r_wh_local->set_value(W_YTD, w_ytd + h_amount);
	}
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return RCOK;
}

inline RC TPCCTxnManager::run_payment_2(uint64_t w_id, uint64_t d_id, uint64_t d_w_id,
																				double h_amount, row_t *&r_dist_local) {
	/*=====================================================+
		EXEC SQL UPDATE district SET d_ytd = d_ytd + :h_amount
		WHERE d_w_id=:w_id AND d_id=:d_id;
	+=====================================================*/
	uint64_t starttime = get_sys_clock();
	uint64_t key;
	itemid_t * item;
	key = distKey(d_id, d_w_id);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	item = index_read(_wl->i_district, key, wh_to_part(w_id));
	starttime = get_sys_clock();
	assert(item != NULL);
	row_t * r_dist = ((row_t *)item->location);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	RC rc = get_row(r_dist, WR, r_dist_local);
	return rc;
}

inline RC TPCCTxnManager::run_payment_3(uint64_t w_id, uint64_t d_id, uint64_t d_w_id,
																				double h_amount, row_t *r_dist_local) {
	assert(r_dist_local != NULL);
	uint64_t starttime = get_sys_clock();
	/*=====================================================+
		EXEC SQL UPDATE district SET d_ytd = d_ytd + :h_amount
		WHERE d_w_id=:w_id AND d_id=:d_id;
	+=====================================================*/
	double d_ytd;
	r_dist_local->get_value(D_YTD, d_ytd);

	r_dist_local->set_value(D_YTD, d_ytd + h_amount);

	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return RCOK;
}

inline RC TPCCTxnManager::run_payment_4(uint64_t w_id, uint64_t d_id, uint64_t c_id,
																				uint64_t c_w_id, uint64_t c_d_id, char *c_last,
																				double h_amount, bool by_last_name, row_t *&r_cust_local) {
	/*====================================================================+
		EXEC SQL SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name
		INTO :d_street_1, :d_street_2, :d_city, :d_state, :d_zip, :d_name
		FROM district
		WHERE d_w_id=:w_id AND d_id=:d_id;
	+====================================================================*/
	uint64_t starttime = get_sys_clock();
	itemid_t * item;
	uint64_t key;
	row_t * r_cust;
	if (by_last_name) {
		/*==========================================================+
			EXEC SQL SELECT count(c_id) INTO :namecnt
			FROM customer
			WHERE c_last=:c_last AND c_d_id=:c_d_id AND c_w_id=:c_w_id;
		+==========================================================*/
		/*==========================================================================+
			EXEC SQL DECLARE c_byname CURSOR FOR
			SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city, c_state,
			c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since
			FROM customer
			WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_last=:c_last
			ORDER BY c_first;
			EXEC SQL OPEN c_byname;
		+===========================================================================*/

		key = custNPKey(c_last, c_d_id, c_w_id);
		// XXX: the list is not sorted. But let's assume it's sorted...
		// The performance won't be much different.
		INDEX * index = _wl->i_customer_last;
		INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
		item = index_read(index, key, wh_to_part(c_w_id));
		starttime = get_sys_clock();
		assert(item != NULL);

		int cnt = 0;
		itemid_t * it = item;
		itemid_t * mid = item;
		while (it != NULL) {
			cnt ++;
			it = it->next;
			if (cnt % 2 == 0) mid = mid->next;
		}
		r_cust = ((row_t *)mid->location);

		/*============================================================================+
			for (n=0; n<namecnt/2; n++) {
				EXEC SQL FETCH c_byname
				INTO :c_first, :c_middle, :c_id,
					 :c_street_1, :c_street_2, :c_city, :c_state, :c_zip,
					 :c_phone, :c_credit, :c_credit_lim, :c_discount, :c_balance, :c_since;
			}
			EXEC SQL CLOSE c_byname;
		+=============================================================================*/
		// XXX: we don't retrieve all the info, just the tuple we are interested in
	} else {  // search customers by cust_id
		/*=====================================================================+
			EXEC SQL SELECT c_first, c_middle, c_last, c_street_1, c_street_2,
			c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim,
			c_discount, c_balance, c_since
			INTO :c_first, :c_middle, :c_last, :c_street_1, :c_street_2,
			:c_city, :c_state, :c_zip, :c_phone, :c_credit, :c_credit_lim,
			:c_discount, :c_balance, :c_since
			FROM customer
			WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_id=:c_id;
		+======================================================================*/
		key = custKey(c_id, c_d_id, c_w_id);
		INDEX * index = _wl->i_customer_id;
		INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
		item = index_read(index, key, wh_to_part(c_w_id));
		starttime = get_sys_clock();
		assert(item != NULL);
		r_cust = (row_t *) item->location;
	}
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
		/*======================================================================+
		 	EXEC SQL UPDATE customer SET c_balance = :c_balance, c_data = :c_new_data
	 		WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id;
	 	+======================================================================*/
	RC rc  = get_row(r_cust, WR, r_cust_local);

	return rc;
}

inline RC TPCCTxnManager::run_payment_5(uint64_t w_id, uint64_t d_id, uint64_t c_id,
																				uint64_t c_w_id, uint64_t c_d_id, char *c_last,
																				double h_amount, bool by_last_name, row_t *r_cust_local) {
	uint64_t starttime = get_sys_clock();
	assert(r_cust_local != NULL);
	double c_balance;
	double c_ytd_payment;
	double c_payment_cnt;

	r_cust_local->get_value(C_BALANCE, c_balance);
	r_cust_local->set_value(C_BALANCE, c_balance - h_amount);
	r_cust_local->get_value(C_YTD_PAYMENT, c_ytd_payment);
	r_cust_local->set_value(C_YTD_PAYMENT, c_ytd_payment + h_amount);
	r_cust_local->get_value(C_PAYMENT_CNT, c_payment_cnt);
	r_cust_local->set_value(C_PAYMENT_CNT, c_payment_cnt + 1);

	//char * c_credit = r_cust_local->get_value(C_CREDIT);

	/*=============================================================================+
		EXEC SQL INSERT INTO
		history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data)
		VALUES (:c_d_id, :c_w_id, :c_id, :d_id, :w_id, :datetime, :h_amount, :h_data);
		+=============================================================================*/
	row_t * r_hist;
	uint64_t row_id;
	// Which partition should we be inserting into?
	_wl->t_history->get_new_row(r_hist, wh_to_part(c_w_id), row_id);
	r_hist->set_value(H_C_ID, c_id);
	r_hist->set_value(H_C_D_ID, c_d_id);
	r_hist->set_value(H_C_W_ID, c_w_id);
	r_hist->set_value(H_D_ID, d_id);
	r_hist->set_value(H_W_ID, w_id);
	int64_t date = 2013;
	r_hist->set_value(H_DATE, date);
	r_hist->set_value(H_AMOUNT, h_amount);
	// insert_row(r_hist, _wl->i_history);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return RCOK;
}

// new_order 0
inline RC TPCCTxnManager::new_order_0(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote,
																			uint64_t ol_cnt, uint64_t o_entry_d, uint64_t *o_id,
																			row_t *&r_wh_local) {
	uint64_t starttime = get_sys_clock();
	uint64_t key;
	itemid_t * item;
	/*=======================================================================+
	EXEC SQL SELECT c_discount, c_last, c_credit, w_tax
		INTO :c_discount, :c_last, :c_credit, :w_tax
		FROM customer, warehouse
		WHERE w_id = :w_id AND c_w_id = w_id AND c_d_id = :d_id AND c_id = :c_id;
	+========================================================================*/
	key = w_id;
	INDEX * index = _wl->i_warehouse;
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	item = index_read(index, key, wh_to_part(w_id));
	starttime = get_sys_clock();
	assert(item != NULL);
	row_t * r_wh = ((row_t *)item->location);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	RC rc = get_row(r_wh, RD, r_wh_local);
	return rc;
}

inline RC TPCCTxnManager::new_order_1(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote,
																			uint64_t ol_cnt, uint64_t o_entry_d, uint64_t *o_id,
																			row_t *r_wh_local) {
	uint64_t starttime = get_sys_clock();
	assert(r_wh_local != NULL);
	double w_tax;
	r_wh_local->get_value(W_TAX, w_tax);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return RCOK;
}

inline RC TPCCTxnManager::new_order_2(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote,
																			uint64_t ol_cnt, uint64_t o_entry_d, uint64_t *o_id,
																			row_t *&r_cust_local) {
	uint64_t starttime = get_sys_clock();
	uint64_t key;
	itemid_t * item;
	key = custKey(c_id, d_id, w_id);
	INDEX * index = _wl->i_customer_id;
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	item = index_read(index, key, wh_to_part(w_id));
	starttime = get_sys_clock();
	assert(item != NULL);
	row_t * r_cust = (row_t *) item->location;
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	RC rc = get_row(r_cust, RD, r_cust_local);
	return rc;
}

inline RC TPCCTxnManager::new_order_3(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote,
																			uint64_t ol_cnt, uint64_t o_entry_d, uint64_t *o_id,
																			row_t *r_cust_local) {
	uint64_t starttime = get_sys_clock();
	assert(r_cust_local != NULL);
	uint64_t c_discount;
	//char * c_last;
	//char * c_credit;
	r_cust_local->get_value(C_DISCOUNT, c_discount);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	//c_last = r_cust_local->get_value(C_LAST);
	//c_credit = r_cust_local->get_value(C_CREDIT);
	return RCOK;
}

inline RC TPCCTxnManager::new_order_4(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote,
																			uint64_t ol_cnt, uint64_t o_entry_d, uint64_t *o_id,
																			row_t *&r_dist_local) {
	uint64_t starttime = get_sys_clock();
	uint64_t key;
	itemid_t * item;
	/*==================================================+
	EXEC SQL SELECT d_next_o_id, d_tax
		INTO :d_next_o_id, :d_tax
		FROM district WHERE d_id = :d_id AND d_w_id = :w_id;
	EXEC SQL UPDATE d istrict SET d _next_o_id = :d _next_o_id + 1
		WH ERE d _id = :d_id AN D d _w _id = :w _id ;
	+===================================================*/
	key = distKey(d_id, w_id);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	item = index_read(_wl->i_district, key, wh_to_part(w_id));
	starttime = get_sys_clock();
	assert(item != NULL);
	row_t * r_dist = ((row_t *)item->location);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	RC rc = get_row(r_dist, WR, r_dist_local);
	return rc;
}

inline RC TPCCTxnManager::new_order_5(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote,
																			uint64_t ol_cnt, uint64_t o_entry_d, uint64_t *o_id,
																			row_t *r_dist_local) {
	uint64_t starttime = get_sys_clock();
	assert(r_dist_local != NULL);
	//double d_tax;
	//int64_t o_id;
	//d_tax = *(double *) r_dist_local->get_value(D_TAX);
	*o_id = *(int64_t *) r_dist_local->get_value(D_NEXT_O_ID);
	(*o_id) ++;
	r_dist_local->set_value(D_NEXT_O_ID, *o_id);

	// return o_id
	
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return RCOK;
}

inline RC TPCCTxnManager::new_order_5_1(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote,
																			uint64_t ol_cnt, uint64_t o_entry_d, uint64_t *o_id,
																			row_t *r_dist_local) {
	uint64_t starttime = get_sys_clock();
	/*========================================================================================+
	EXEC SQL INSERT INTO ORDERS (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
		VALUES (:o_id, :d_id, :w_id, :c_id, :datetime, :o_ol_cnt, :o_all_local);
	+========================================================================================*/
	row_t * r_order;
	uint64_t row_id;
	_wl->t_order->get_new_row(r_order, wd_to_part(w_id, d_id), row_id);
	r_order->set_primary_key(orderPrimaryKey(w_id, d_id, *o_id));
	r_order->set_value(O_ID, *o_id);
	r_order->set_value(O_C_ID, c_id);
	r_order->set_value(O_D_ID, d_id);
	r_order->set_value(O_W_ID, w_id);
	r_order->set_value(O_ENTRY_D, o_entry_d);
	r_order->set_value(O_OL_CNT, ol_cnt);
	int64_t all_local = (remote? 0 : 1);
	r_order->set_value(O_ALL_LOCAL, all_local);
	RC rc = RCOK;
#if TXN_TYPE == TPCC_ALL
#if CC_ALG == CALVIN
	rc = get_lock(r_order, WR);
#endif
	row_t * temp;
	rc = get_row(r_order, WR, temp);
	assert(rc == RCOK);
	rc = insert_row(r_order, _wl->i_order);
	if (rc == Abort) return rc;
// #if CC_ALG == CALVIN
// 	itemid_t *m_item = (itemid_t *)mem_allocator.alloc(sizeof(itemid_t));
// 	m_item->init();
// 	m_item->type = DT_row;
// 	m_item->location = r_order;
// 	m_item->valid = true;
// 	_wl->i_order_cust->index_insert(custKey(c_id, d_id, w_id), m_item);
// #elif CC_ALG == HDCC
// 	if (algo == CALVIN) {
// 		itemid_t *m_item = (itemid_t *)mem_allocator.alloc(sizeof(itemid_t));
// 		m_item->init();
// 		m_item->type = DT_row;
// 		m_item->location = r_order;
// 		m_item->valid = true;
// 		_wl->i_order_cust->index_insert(custKey(c_id, d_id, w_id), m_item);
// 	}
// #elif CC_ALG == SNAPPER
// 	if (algo == CALVIN) {
// 		itemid_t *m_item = (itemid_t *)mem_allocator.alloc(sizeof(itemid_t));
// 		m_item->init();
// 		m_item->type = DT_row;
// 		m_item->location = r_order;
// 		m_item->valid = true;
// 		_wl->i_order_cust->index_insert(custKey(c_id, d_id, w_id), m_item);
// 	}
// #endif
#else
	insert_row(r_order, _wl->t_order);
#endif

	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return rc;
}

inline RC TPCCTxnManager::new_order_5_2(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote,
																			uint64_t ol_cnt, uint64_t o_entry_d, uint64_t *o_id,
																			row_t *r_dist_local) {
	uint64_t starttime = get_sys_clock();
	/*=======================================================+
		EXEC SQL INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)
				VALUES (:o_id, :d_id, :w_id);
		+=======================================================*/
	row_t * r_no;
	uint64_t row_id;
	_wl->t_neworder->get_new_row(r_no, wd_to_part(w_id, d_id), row_id);
	r_no->set_primary_key(neworderKey(w_id, d_id, *o_id));
	r_no->set_value(NO_O_ID, *o_id);
	r_no->set_value(NO_D_ID, d_id);
	r_no->set_value(NO_W_ID, w_id);
	RC rc = RCOK;
#if TXN_TYPE == TPCC_ALL
#if CC_ALG == CALVIN
	rc = get_lock(r_no, WR);
#endif
	row_t * temp;
	rc = get_row(r_no, WR, temp);
	assert(rc == RCOK);
	rc = insert_row(r_no, _wl->i_neworder);
	if (rc == Abort) return rc;
#else
	insert_row(r_no, _wl->t_neworder);
#endif

	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return rc;
}


// new_order 1
// Read from replicated read-only item table
inline RC TPCCTxnManager::new_order_6(uint64_t ol_i_id, row_t *& r_item_local) {
	uint64_t starttime = get_sys_clock();
	uint64_t key;
	itemid_t * item;
	/*===========================================+
	EXEC SQL SELECT i_price, i_name , i_data
		INTO :i_price, :i_name, :i_data
		FROM item
		WHERE i_id = :ol_i_id;
	+===========================================*/
	key = ol_i_id;
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	item = index_read(_wl->i_item, key, 0);
	starttime = get_sys_clock();
	assert(item != NULL);
	row_t * r_item = ((row_t *)item->location);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	RC rc = get_row(r_item, RD, r_item_local);
	return rc;
}

inline RC TPCCTxnManager::new_order_7(uint64_t ol_i_id, row_t * r_item_local) {
	uint64_t starttime = get_sys_clock();
	assert(r_item_local != NULL);
	int64_t i_price;
	//char * i_name;
	//char * i_data;

	r_item_local->get_value(I_PRICE, i_price);
	//i_name = r_item_local->get_value(I_NAME);
	//i_data = r_item_local->get_value(I_DATA);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return RCOK;
}

// new_order 2
inline RC TPCCTxnManager::new_order_8(uint64_t w_id, uint64_t d_id, bool remote, uint64_t ol_i_id,
																			uint64_t ol_supply_w_id, uint64_t ol_quantity,
																			uint64_t ol_number, uint64_t o_id, row_t *&r_stock_local) {
	uint64_t starttime = get_sys_clock();
	uint64_t key;
	itemid_t * item;

	/*===================================================================+
	EXEC SQL SELECT s_quantity, s_data,
			s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05,
			s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10
		INTO :s_quantity, :s_data,
			:s_dist_01, :s_dist_02, :s_dist_03, :s_dist_04, :s_dist_05,
			:s_dist_06, :s_dist_07, :s_dist_08, :s_dist_09, :s_dist_10
		FROM stock
		WHERE s_i_id = :ol_i_id AND s_w_id = :ol_supply_w_id;
	EXEC SQL UPDATE stock SET s_quantity = :s_quantity
		WHERE s_i_id = :ol_i_id
		AND s_w_id = :ol_supply_w_id;
	+===============================================*/

	key = stockKey(ol_i_id, ol_supply_w_id);
	INDEX * index = _wl->i_stock;
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	item = index_read(index, key, wh_to_part(ol_supply_w_id));
	starttime = get_sys_clock();
	assert(item != NULL);
	row_t * r_stock = ((row_t *)item->location);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	RC rc = get_row(r_stock, WR, r_stock_local);
	return rc;
}

inline RC TPCCTxnManager::new_order_9(uint64_t w_id, uint64_t d_id, bool remote, uint64_t ol_i_id,
																			uint64_t ol_supply_w_id, uint64_t ol_quantity,
																			uint64_t ol_number, uint64_t ol_amount, uint64_t o_id,
																			row_t *r_stock_local) {
	uint64_t starttime = get_sys_clock();
	assert(r_stock_local != NULL);
		// XXX s_dist_xx are not retrieved.
	UInt64 s_quantity;
	int64_t s_remote_cnt;
	s_quantity = *(int64_t *)r_stock_local->get_value(S_QUANTITY);
#if !TPCC_SMALL
	int64_t s_ytd;
	int64_t s_order_cnt;
	char * s_data __attribute__ ((unused));
	r_stock_local->get_value(S_YTD, s_ytd);
	r_stock_local->set_value(S_YTD, s_ytd + ol_quantity);
	// In Coordination Avoidance, this record must be protected!
	r_stock_local->get_value(S_ORDER_CNT, s_order_cnt);
	r_stock_local->set_value(S_ORDER_CNT, s_order_cnt + 1);
	s_data = r_stock_local->get_value(S_DATA);
#endif
	if (remote) {
		s_remote_cnt = *(int64_t*)r_stock_local->get_value(S_REMOTE_CNT);
		s_remote_cnt ++;
		r_stock_local->set_value(S_REMOTE_CNT, &s_remote_cnt);
	}
	uint64_t quantity;
	if (s_quantity > ol_quantity + 10) {
		quantity = s_quantity - ol_quantity;
	} else {
		quantity = s_quantity - ol_quantity + 91;
	}
	r_stock_local->set_value(S_QUANTITY, &quantity);

	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return RCOK;
}

inline RC TPCCTxnManager::new_order_9_1(uint64_t w_id, uint64_t d_id, bool remote, uint64_t o_id, row_t *r_stock_local) {
	uint64_t starttime = get_sys_clock();
	/*====================================================+
	EXEC SQL INSERT
		INTO order_line(ol_o_id, ol_d_id, ol_w_id, ol_number,
			ol_i_id, ol_supply_w_id,
			ol_quantity, ol_amount, ol_dist_info)
		VALUES(:o_id, :d_id, :w_id, :ol_number,
			:ol_i_id, :ol_supply_w_id,
			:ol_quantity, :ol_amount, :ol_dist_info);
	+====================================================*/
	TPCCQuery* tpcc_query = (TPCCQuery*) query;
	itemid_t * r_ol_last = NULL;
	for(uint64_t i = 0; i < tpcc_query->ol_cnt; i++) {
		uint64_t ol_number = i;
		uint64_t ol_i_id = tpcc_query->items[ol_number]->ol_i_id;
		uint64_t ol_supply_w_id = tpcc_query->items[ol_number]->ol_supply_w_id;
		uint64_t ol_quantity = tpcc_query->items[ol_number]->ol_quantity;
		uint64_t ol_amount = tpcc_query->ol_amount;
		row_t * r_ol;
		uint64_t row_id;
		_wl->t_orderline->get_new_row(r_ol, wd_to_part(w_id, d_id), row_id);
		r_ol->set_primary_key(orderlineKey(w_id, d_id, o_id));
		r_ol->set_value(OL_O_ID, &o_id);
		r_ol->set_value(OL_D_ID, &d_id);
		r_ol->set_value(OL_W_ID, &w_id);
		r_ol->set_value(OL_NUMBER, &ol_number);
		r_ol->set_value(OL_I_ID, &ol_i_id);
#if !TPCC_SMALL
		r_ol->set_value(OL_SUPPLY_W_ID, &ol_supply_w_id);
		r_ol->set_value(OL_QUANTITY, &ol_quantity);
		r_ol->set_value(OL_AMOUNT, &ol_amount);
#endif
		itemid_t *m_item = (itemid_t *)mem_allocator.alloc(sizeof(itemid_t));
		m_item->init();
		m_item->type = DT_row;
		m_item->location = r_ol;
		m_item->valid = true;
		m_item->next = r_ol_last;
		r_ol_last = m_item;
	}
	RC rc = RCOK;
#if TXN_TYPE == TPCC_ALL
	rc = insert_item(r_ol_last, _wl->i_orderline);
	if (rc == Abort) return rc;
#else
	insert_item(r_ol_last, _wl->i_orderline);
#endif

	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return rc;
}

inline RC TPCCTxnManager::run_order_status_0(uint64_t w_id, uint64_t d_id, bool by_last_name, uint64_t c_id, char* c_last,
                   row_t*& r_cust_local) {
	uint64_t starttime = get_sys_clock();
	RC rc;
	itemid_t * item;
	if (by_last_name) {
		uint64_t key = custNPKey(c_last, d_id, w_id);
		INDEX * index = _wl->i_customer_last;
		item = index_read(index, key, wh_to_part(w_id));
	} else {
		uint64_t key = custKey(c_id, d_id, w_id);
		INDEX * index = _wl->i_customer_id;
		item = index_read(index, key, wh_to_part(w_id));
	}
	assert(item != NULL);
	row_t * row = ((row_t *)item->location);
	rc = get_row(row, RD, r_cust_local);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return rc;
}

inline RC TPCCTxnManager::run_order_status_1(uint64_t w_id, uint64_t d_id, uint64_t c_id, uint64_t o_id, row_t*& r_row) {
	uint64_t starttime = get_sys_clock();
	r_row->get_value(C_ID, c_id);
	uint64_t key = custKey(c_id, d_id, w_id);
	itemid_t * item;
	INDEX * index = _wl->i_order_cust;
	item = index_read(index, key, wh_to_part(w_id));
	assert(item != NULL);
	row_t * row = ((row_t *)item->location);
	RC rc = get_row(row, RD, r_row);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return rc;
}

inline RC TPCCTxnManager::run_order_status_2(uint64_t w_id, uint64_t d_id, uint64_t o_id, itemid_t * items, row_t*& l_order_local) {
	uint64_t starttime = get_sys_clock();
#if CC_ALG != CALVIN
	l_order_local->get_value(O_ID, o_id);
#endif
	uint64_t key = orderlineKey(w_id, d_id, o_id);
	_wl->i_orderline->index_read(key, items, wd_to_part(w_id, d_id), get_thd_id(), this);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return RCOK;
}

inline RC TPCCTxnManager::run_order_status_3(row_t * l_orderline_local) {
	uint64_t starttime = get_sys_clock();
	RC rc = get_row(row, RD, l_orderline_local);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return rc;
}

inline RC TPCCTxnManager::run_delivery_0(uint64_t w_id, uint64_t d_id, uint64_t &o_id, row_t*& l_row) {
	uint64_t starttime = get_sys_clock();
	row_t * row = NULL;
	bt_node * leaf = NULL;
#if TXN_TYPE == TPCC_ALL
	_wl->i_neworder->leaf_row_access(0, LF_FIRST, wd_to_part(w_id, d_id), this, leaf, row);
#endif
	RC rc = get_row(row, WR, l_row);
	itemid_t * item;
	for (uint32_t i = 0; i < leaf->num_keys - 1; i++) {
		item = (itemid_t *)leaf->pointers[i];
		if(item->valid) break;
	}
	l_row = (row_t *)item->location;
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return rc;
}

inline RC TPCCTxnManager::run_delivery_1(uint64_t &no_o_id, row_t *&r_new_order_local) {
	uint64_t starttime = get_sys_clock();
	assert(r_new_order_local != NULL);
	row_t * row = r_new_order_local;
	RC rc = get_row(row, WR, r_new_order_local);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return rc;
}

inline RC TPCCTxnManager::run_delivery_2(uint64_t &no_o_id, row_t *&r_new_order_local) {
	uint64_t starttime = get_sys_clock();
#if CC_ALG != CALVIN
	r_new_order_local->get_value(NO_O_ID, no_o_id);
#endif
#if TXN_TYPE == TPCC_ALL
	delete_row(r_new_order_local, _wl->i_neworder);
#endif
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return RCOK;
}

inline RC TPCCTxnManager::run_delivery_3(uint64_t o_w_id, uint64_t o_d_id, uint64_t no_o_id, row_t *&l_order_local) {
	uint64_t starttime = get_sys_clock();
	uint64_t key = orderPrimaryKey(o_w_id, o_d_id, no_o_id);
	itemid_t * item;
	_wl->i_order->index_read(key, item, wd_to_part(o_w_id, o_d_id), get_thd_id(), this);
	assert(item != NULL);
	row_t * row = ((row_t *)item->location);
	RC rc = get_row(row, WR, l_order_local);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return rc;
}

inline RC TPCCTxnManager::run_delivery_4(uint64_t o_carrier_id, uint64_t &c_id, row_t *&l_order_local) {
	uint64_t starttime = get_sys_clock();
	l_order_local->get_value(O_C_ID, c_id);
	l_order_local->set_value(O_CARRIER_ID, o_carrier_id);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return RCOK;
}

inline RC TPCCTxnManager::run_delivery_5(uint64_t o_w_id, uint64_t o_d_id, uint64_t no_o_id, itemid_t *& items) {
	uint64_t starttime = get_sys_clock();
	uint64_t key = orderlineKey(o_w_id, o_d_id, no_o_id);
	_wl->i_orderline->index_read(key, items, wd_to_part(o_w_id, o_d_id), get_thd_id(), this);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return RCOK;
}

inline RC TPCCTxnManager::run_delivery_6(row_t *&l_orderline_local) {
	uint64_t starttime = get_sys_clock();
	RC rc = get_row(row, WR, l_orderline_local);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return rc;
}

inline RC TPCCTxnManager::run_delivery_7(uint64_t ol_delivery_d, uint64_t &sum_amount, row_t *&l_orderline_local) {
	uint64_t starttime = get_sys_clock();
	uint64_t ol_amount;
	l_orderline_local->get_value(OL_AMOUNT, ol_amount);
	sum_amount += ol_amount;
	l_orderline_local->set_value(OL_DELIVERY_D, ol_delivery_d);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return RCOK;
}

inline RC TPCCTxnManager::run_delivery_8(uint64_t c_w_id, uint64_t c_d_id, uint64_t c_id, row_t *&r_cust_local) {
	uint64_t starttime = get_sys_clock();
	uint64_t key = custKey(c_id, c_d_id, c_w_id);
	itemid_t * item;
	INDEX * index = _wl->i_customer_id;
	item = index_read(index, key, wh_to_part(c_w_id));
	assert(item != NULL);
	row_t * row = ((row_t *)item->location);
	RC rc = get_row(row, WR, r_cust_local);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return rc;
}

inline RC TPCCTxnManager::run_delivery_9(uint64_t &sum_amount, row_t *&r_cust_local) {
	uint64_t starttime = get_sys_clock();
	assert(r_cust_local != NULL);
	double c_balance;
	r_cust_local->get_value(C_BALANCE, c_balance);
	double new_balance = c_balance + sum_amount;
	r_cust_local->set_value(C_BALANCE, new_balance);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return RCOK;
}

inline RC TPCCTxnManager::run_stock_level_0(uint64_t w_id, uint64_t d_id, row_t *&r_dist_local) {
	uint64_t starttime = get_sys_clock();
	uint64_t key = distKey(d_id, w_id);
	itemid_t * item;
	INDEX * index = _wl->i_district;
	item = index_read(index, key, wh_to_part(w_id));
	assert(item != NULL);
	row_t * row = ((row_t *)item->location);
	RC rc = get_row(row, RD, r_dist_local);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return rc;
}

inline RC TPCCTxnManager::run_stock_level_1(uint64_t &d_next_o_id, row_t *&r_dist_local) {
	uint64_t starttime = get_sys_clock();
	r_dist_local->get_value(D_NEXT_O_ID, d_next_o_id);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return RCOK;
}

inline RC TPCCTxnManager::run_stock_level_2(uint64_t w_id, uint64_t d_id, uint64_t d_next_o_id, bt_node *& leaf, row_t *&r_leaf_local) {
	uint64_t starttime = get_sys_clock();
	row_t * row = NULL;
#if TXN_TYPE == TPCC_ALL
	_wl->i_orderline->leaf_row_access(UINT64_MAX, LF_LAST, wd_to_part(w_id, d_id), this, leaf, row);
#endif
	RC rc = get_row(row, RD, r_leaf_local);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return rc;
}

inline RC TPCCTxnManager::run_stock_level_3(uint64_t w_id, uint64_t d_id, uint64_t d_next_o_id, row_t *&r_orderline_local) {
	uint64_t starttime = get_sys_clock();
	row_t * row = r_orderline_local;
	RC rc = get_row(row, RD, r_orderline_local);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return rc;
}

inline RC TPCCTxnManager::run_stock_level_4(uint64_t w_id, uint64_t &s_i_id, row_t *&r_local) {
	uint64_t starttime = get_sys_clock();
	r_local->get_value(OL_I_ID, s_i_id);
	itemid_t * item = NULL;
	item = index_read(_wl->i_stock, stockKey(s_i_id, w_id), wh_to_part(w_id));
	assert(item != NULL);
	row_t * row = ((row_t *)item->location);
	for (uint64_t i = 0; i < txn->accesses.size(); i++) {
		if (txn->accesses[i]->orig_row == row) {
			return RCOK;
		}
	}
	RC rc = get_row(row, RD, r_local);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return rc;
}

inline RC TPCCTxnManager::run_stock_level_5(uint64_t w_id, uint64_t s_i_id, uint64_t threshold, set<uint64_t> &s_i_ids, row_t *&r_local) {
	uint64_t starttime = get_sys_clock();
	uint64_t s_quantity;
	r_local->get_value(S_QUANTITY, s_quantity);
	if (s_quantity < threshold) {
		s_i_ids.insert(s_i_id);
	}
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return RCOK;
}


RC TPCCTxnManager::run_calvin_txn() {
	RC rc = RCOK;
	uint64_t starttime = get_sys_clock();
	TPCCQuery* tpcc_query = (TPCCQuery*) query;
	DEBUG("(%ld,%ld) Run calvin txn\n",txn->txn_id,txn->batch_id);
	while(!calvin_exec_phase_done() && rc == RCOK) {
		DEBUG("(%ld,%ld) phase %d\n",txn->txn_id,txn->batch_id,this->phase);
		switch(this->phase) {
			case CALVIN_RW_ANALYSIS:
				// Phase 1: Read/write set analysis
				calvin_expected_rsp_cnt = tpcc_query->get_participants(_wl);
				if(query->participant_nodes[g_node_id] == 1) {
					calvin_expected_rsp_cnt--;
				}

				DEBUG("(%ld,%ld) expects %d responses; %ld participants, %ld active\n", txn->txn_id,
							txn->batch_id, calvin_expected_rsp_cnt, query->participant_nodes.size(),
							query->active_nodes.size());

				this->phase = CALVIN_LOC_RD;
				break;
			case CALVIN_LOC_RD:
				// Phase 2: Perform local reads
				DEBUG("(%ld,%ld) local reads\n",txn->txn_id,txn->batch_id);
				rc = run_tpcc_phase2();
				//release_read_locks(tpcc_query);

				this->phase = CALVIN_SERVE_RD;
				break;
			case CALVIN_SERVE_RD:
				// Phase 3: Serve remote reads
				if(query->participant_nodes[g_node_id] == 1) {
					rc = send_remote_reads();
				}
				if(query->active_nodes[g_node_id] == 1) {
					this->phase = CALVIN_COLLECT_RD;
					if(calvin_collect_phase_done()) {
						rc = RCOK;
					} else {
						assert(calvin_expected_rsp_cnt > 0);
						DEBUG("(%ld,%ld) wait in collect phase; %d / %d rfwds received\n", txn->txn_id,
									txn->batch_id, rsp_cnt, calvin_expected_rsp_cnt);
						rc = WAIT;
					}
				} else { // Done
					rc = RCOK;
					this->phase = CALVIN_DONE;
				}
				break;
			case CALVIN_COLLECT_RD:
				// Phase 4: Collect remote reads
				this->phase = CALVIN_EXEC_WR;
				break;
			case CALVIN_EXEC_WR:
				// Phase 5: Execute transaction / perform local writes
				DEBUG("(%ld,%ld) execute writes\n",txn->txn_id,txn->batch_id);
				rc = run_tpcc_phase5();
				this->phase = CALVIN_DONE;
				break;
			default:
				assert(false);
		}
	}
	uint64_t curr_time = get_sys_clock();
	txn_stats.process_time += curr_time - starttime;
	txn_stats.process_time_short += curr_time - starttime;
	txn_stats.wait_starttime = get_sys_clock();
	return rc;

}

#if CC_ALG == ARIA
RC TPCCTxnManager::run_aria_txn() {
	RC rc = RCOK;
	uint64_t starttime = get_sys_clock();
	TPCCQuery* tpcc_query = (TPCCQuery*) query;
	DEBUG_WRK("(%ld,%ld) Run aria txn\n",txn->txn_id,txn->batch_id);
	uint64_t w_id = tpcc_query->w_id;
	uint64_t d_id = tpcc_query->d_id;
	uint64_t c_id = tpcc_query->c_id;
	uint64_t d_w_id = tpcc_query->d_w_id;
	uint64_t c_w_id = tpcc_query->c_w_id;
	uint64_t c_d_id = tpcc_query->c_d_id;
	char * c_last = tpcc_query->c_last;
	double h_amount = tpcc_query->h_amount;
	bool by_last_name = tpcc_query->by_last_name;
	bool remote = tpcc_query->remote;
	uint64_t ol_cnt = tpcc_query->ol_cnt;
	uint64_t o_entry_d = tpcc_query->o_entry_d;
	#if !LONG_TXN_SCHEDULE
	ARIA_PHASE phase = simulation->aria_phase;
	#else
	ARIA_PHASE phase = aria_phase;
	#endif
	switch (phase)
	{
	case ARIA_READ:
		if (tpcc_query->txn_type == TPCC_PAYMENT) {
			uint64_t wh_node = GET_NODE_ID(wh_to_part(w_id));
			if (wh_node != g_node_id) {
				w_loc = false;
			}
			query->partitions_touched.add_unique(wh_node);
			uint64_t c_node = GET_NODE_ID(wh_to_part(c_w_id));
			if (c_node != g_node_id) {
				c_w_loc = false;
			}
			query->partitions_touched.add_unique(c_node);
			// if (!w_loc || !c_w_loc) {
			// 	rc = send_remote_read_requests();
			// }
		} else if (tpcc_query->txn_type == TPCC_NEW_ORDER) {
			uint64_t wh_node = GET_NODE_ID(wh_to_part(w_id));
			if (wh_node == g_node_id) {
				rc = new_order_0( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
				rc = new_order_1( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
				rc = new_order_2( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
				rc = new_order_3( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
				rc = new_order_4( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
				tpcc_query->o_id = *(int64_t *) row->get_value(D_NEXT_O_ID);
			} else {
				w_loc = false;
			}
			query->partitions_touched.add_unique(wh_node);
			for (uint64_t i = 0; i < tpcc_query->ol_cnt; i++) {
				uint64_t ol_i_id = tpcc_query->items[i]->ol_i_id;
				uint64_t ol_supply_w_id = tpcc_query->items[i]->ol_supply_w_id;
				uint64_t ol_node = GET_NODE_ID(wh_to_part(ol_supply_w_id));
				if (ol_node == g_node_id) {
					rc = new_order_6(ol_i_id, row);
					rc = new_order_7(ol_i_id, row);
				} else {
					ol_supply_w_all_loc = false;
				}
				query->partitions_touched.add_unique(ol_node);
			}
			if (!w_loc || !ol_supply_w_all_loc) {
				rc = send_remote_read_requests();
			}
		} else if (tpcc_query->txn_type == TPCC_ORDER_STATUS) {
			uint64_t wh_node = GET_NODE_ID(wh_to_part(w_id));
			if (wh_node != g_node_id) {
				w_loc = false;
			}
			query->partitions_touched.add_unique(wh_node);
			assert(w_loc);
			rc = run_order_status_0(w_id, d_id, by_last_name, c_id, c_last, row);
			rc = run_order_status_1(w_id, d_id, c_id, tpcc_query->o_id, row);
			rc = run_order_status_2(w_id, d_id, tpcc_query->o_id, items, row);
			while (items != NULL) {
				row = (row_t *)items->location;
				rc = run_order_status_3(row);
				items = items->next;
			}
		} else if (tpcc_query->txn_type == TPCC_DELIVERY) {
		} else if (tpcc_query->txn_type == TPCC_STOCK_LEVEL) {
			uint64_t wh_node = GET_NODE_ID(wh_to_part(w_id));
			if (wh_node != g_node_id) {
				w_loc = false;
			}
			query->partitions_touched.add_unique(wh_node);
			assert(w_loc);
			bt_node * leaf;
			rc = run_stock_level_0(w_id, d_id, row);
			rc = run_stock_level_1(tpcc_query->o_id, row);
			rc = run_stock_level_2(w_id, d_id, tpcc_query->o_id, leaf, row);
			assert(*(uint64_t*)((row_t*)(((itemid_t*)(leaf->pointers[leaf->num_keys - 1]))->location))->get_value(OL_O_ID) == tpcc_query->o_id);
			leaf_traversal_cnt = leaf->num_keys;
			for (uint64_t i = 0; i < 20; i++) {
				itemid_t * item = (itemid_t *)leaf->pointers[leaf_traversal_cnt-1];
				row = (row_t *)item->location;
				rc = run_stock_level_3(w_id, d_id, tpcc_query->o_id, row);
				uint64_t s_i_id;
				rc = run_stock_level_4(w_id, s_i_id, row);
				rc = run_stock_level_5(w_id, s_i_id, tpcc_query->threshold, s_i_ids, row);
				leaf_traversal_cnt--;
				if (leaf_traversal_cnt == 0) {
					leaf = leaf->prev;
					if (leaf == NULL) {
						break;
					}
					leaf_traversal_cnt = leaf->num_keys;
				}
			}
		} else {
			assert(false);
		}

		assert(rc == RCOK || rc == WAIT_REM);

		assert(aria_phase == ARIA_READ);
		#if LONG_TXN_SCHEDULE
		txn_next_aria_phase(get_thd_id(),aria_phase,this);
		#else
		assert(simulation->aria_phase == ARIA_READ);
		#endif
		aria_phase = (ARIA_PHASE) (aria_phase + 1);
		break;
	case ARIA_RESERVATION:
		if (tpcc_query->txn_type == TPCC_PAYMENT) {
			if (w_loc) {
				rc = run_payment_0(w_id, d_id, d_w_id, h_amount, row);
				rc = run_payment_1(w_id, d_id, d_w_id, h_amount, row);
				rc = run_payment_2(w_id, d_id, d_w_id, h_amount, row);
				rc = run_payment_3(w_id, d_id, d_w_id, h_amount, row);
			}
			if (c_w_loc) {
				rc = run_payment_4(w_id, d_id, c_id, c_w_id, c_d_id, c_last, h_amount, by_last_name, row);
				rc = run_payment_5(w_id, d_id, c_id, c_w_id, c_d_id, c_last, h_amount, by_last_name, row);
			}
			if (!w_loc || !c_w_loc) {
				rc = send_remote_write_requests();
			}
		} else if (tpcc_query->txn_type == TPCC_NEW_ORDER) {
			if (w_loc) {
				rc = new_order_5(w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
			}
			for (uint64_t i = 0; i < tpcc_query->ol_cnt; i++) {
				uint64_t ol_number = i;
				uint64_t ol_i_id = tpcc_query->items[ol_number]->ol_i_id;
				uint64_t ol_supply_w_id = tpcc_query->items[ol_number]->ol_supply_w_id;
				uint64_t ol_quantity = tpcc_query->items[ol_number]->ol_quantity;
				uint64_t ol_amount = tpcc_query->ol_amount;
				uint64_t ol_node = GET_NODE_ID(wh_to_part(ol_supply_w_id));
				if (ol_node == g_node_id) {
					rc = new_order_8(w_id, d_id, remote, ol_i_id, ol_supply_w_id, ol_quantity, ol_number,
													 tpcc_query->o_id, row);
					rc = new_order_9(w_id, d_id, remote, ol_i_id, ol_supply_w_id, ol_quantity, ol_number,
													 ol_amount, tpcc_query->o_id, row);
				}
			}

			if (!w_loc || !ol_supply_w_all_loc) {
				rc = send_remote_write_requests();
			}
		} else if (tpcc_query->txn_type == TPCC_ORDER_STATUS) {
		} else if (tpcc_query->txn_type == TPCC_DELIVERY) {
			uint64_t wh_node = GET_NODE_ID(wh_to_part(w_id));
			if (wh_node != g_node_id) {
				w_loc = false;
			}
			query->partitions_touched.add_unique(wh_node);
			assert(w_loc);
			rc = run_delivery_0(w_id, d_id, tpcc_query->o_id, row);
			rc = run_delivery_1(tpcc_query->o_id, row);
			rc = run_delivery_2(tpcc_query->o_id, row);
			rc = run_delivery_3(w_id, d_id, tpcc_query->o_id, row);
			rc = run_delivery_4(tpcc_query->o_carrier_id, c_id, row);
			rc = run_delivery_5(w_id, d_id, tpcc_query->o_id, items);
			
			while (items != NULL) {
				row = (row_t *)items->location;
				rc = run_delivery_6(row);
				rc = run_delivery_7(tpcc_query->ol_delivery_d, sum_amount, row);
				items = items->next;
			}
			rc = run_delivery_8(w_id, d_id, c_id, row);
			rc = run_delivery_9(sum_amount, row);
		} else if (tpcc_query->txn_type == TPCC_STOCK_LEVEL) {
		} else {
			assert(false);
		}

		rc = reserve();
		if (rc == Abort) {
			txn->rc = Abort;
		}

		assert(rc == RCOK || rc == Abort || rc == WAIT_REM);

		assert(aria_phase == ARIA_RESERVATION);
		#if LONG_TXN_SCHEDULE
		txn_next_aria_phase(get_thd_id(),aria_phase,this);
		#else
		assert(simulation->aria_phase == ARIA_RESERVATION);
		#endif
		aria_phase = (ARIA_PHASE) (aria_phase + 1);
		break;
	case ARIA_CHECK:
		if (txn->rc == Abort) {
		} else {
			send_prepare_messages();

			rc = check();
			if (rc == Abort) {
				txn->rc = Abort;
			}

			if (rsp_cnt != 0) {
				rc = WAIT_REM;
			}
		}

		assert(aria_phase == ARIA_CHECK);
		#if LONG_TXN_SCHEDULE
		txn_next_aria_phase(get_thd_id(),aria_phase,this);
		#else
		assert(simulation->aria_phase == ARIA_CHECK);
		#endif
		aria_phase = (ARIA_PHASE) (aria_phase + 1);
		break;
	case ARIA_COMMIT:
		send_finish_messages();

		if (txn->rc == Abort) {
		abort();
		} else {
		commit();
		}

		if (rsp_cnt != 0) {
		rc = WAIT_REM;
		}

		assert(aria_phase == ARIA_COMMIT);
		#if LONG_TXN_SCHEDULE
		txn_next_aria_phase(get_thd_id(),aria_phase,this);
		#else
		assert(simulation->aria_phase == ARIA_COMMIT);
		#endif
		aria_phase = (ARIA_PHASE) (aria_phase + 1);
		break;
	default:
		assert(false);
	}
	uint64_t curr_time = get_sys_clock();
	txn_stats.process_time += curr_time - starttime;
	txn_stats.process_time_short += curr_time - starttime;
	txn_stats.wait_starttime = get_sys_clock();
	INC_STATS(get_thd_id(),worker_activate_txn_time,curr_time - starttime);
	return rc;
}

RC TPCCTxnManager::send_remote_read_requests() {
	for (uint64_t i = 0; i < query->partitions_touched.size(); i++) {
		uint64_t node = query->partitions_touched[i];
		if (node == g_node_id) {
			continue;
		}
		TPCCQueryMessage * msg = (TPCCQueryMessage *) Message::create_message(this, RQRY);
		msg->aria_phase = ARIA_READ;
		msg_queue.enqueue(get_thd_id(), msg, node);
		participants_cnt++;
	}
	txn_stats.trans_process_network_start_time = get_sys_clock();
  	return participants_cnt == 0? RCOK : WAIT_REM;
}

RC TPCCTxnManager::send_remote_write_requests() {
	for (uint64_t i = 0; i < query->partitions_touched.size(); i++) {
		uint64_t node = query->partitions_touched[i];
		if (node == g_node_id) {
			continue;
		}
		TPCCQueryMessage * msg = (TPCCQueryMessage *) Message::create_message(this, RQRY);
		msg->aria_phase = ARIA_RESERVATION;
		msg_queue.enqueue(get_thd_id(), msg, node);
		participants_cnt++;
	}
	txn_stats.trans_process_network_start_time = get_sys_clock();
  	return participants_cnt == 0? RCOK : WAIT_REM;
}

RC TPCCTxnManager::process_aria_remote(ARIA_PHASE aria_phase) {
	RC rc = RCOK;
	TPCCQuery* tpcc_query = (TPCCQuery*) query;
	DEBUG("(%ld,%ld) Run calvin txn\n",txn->txn_id,txn->batch_id);
	uint64_t w_id = tpcc_query->w_id;
	uint64_t d_id = tpcc_query->d_id;
	uint64_t c_id = tpcc_query->c_id;
	uint64_t d_w_id = tpcc_query->d_w_id;
	uint64_t c_w_id = tpcc_query->c_w_id;
	uint64_t c_d_id = tpcc_query->c_d_id;
	char * c_last = tpcc_query->c_last;
	double h_amount = tpcc_query->h_amount;
	bool by_last_name = tpcc_query->by_last_name;
	bool remote = tpcc_query->remote;
	uint64_t ol_cnt = tpcc_query->ol_cnt;
	uint64_t o_entry_d = tpcc_query->o_entry_d;
	switch (aria_phase)
	{
	case ARIA_READ:
		if (tpcc_query->txn_type == TPCC_PAYMENT) {
			assert(false);
		} else if (tpcc_query->txn_type == TPCC_NEW_ORDER) {
			uint64_t wh_node = GET_NODE_ID(wh_to_part(w_id));
			if (wh_node == g_node_id) {
				rc = new_order_0( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
				rc = new_order_1( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
				rc = new_order_2( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
				rc = new_order_3( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
				rc = new_order_4( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
				tpcc_query->o_id = *(int64_t *) row->get_value(D_NEXT_O_ID);
			}
			for (uint64_t i = 0; i < tpcc_query->ol_cnt; i++) {
				uint64_t ol_i_id = tpcc_query->items[i]->ol_i_id;
				uint64_t ol_supply_w_id = tpcc_query->items[i]->ol_supply_w_id;
				uint64_t ol_node = GET_NODE_ID(wh_to_part(ol_supply_w_id));
				if (ol_node == g_node_id) {
					rc = new_order_6(ol_i_id, row);
					rc = new_order_7(ol_i_id, row);
				}
			}
		}
		break;
	case ARIA_RESERVATION:
		if (tpcc_query->txn_type == TPCC_PAYMENT) {
			uint64_t wh_node = GET_NODE_ID(wh_to_part(w_id));
			if (wh_node == g_node_id) {
				rc = run_payment_0(w_id, d_id, d_w_id, h_amount, row);
				rc = run_payment_1(w_id, d_id, d_w_id, h_amount, row);
				rc = run_payment_2(w_id, d_id, d_w_id, h_amount, row);
				rc = run_payment_3(w_id, d_id, d_w_id, h_amount, row);
			}
			uint64_t c_node = GET_NODE_ID(wh_to_part(c_w_id));
			if (c_node == g_node_id) {
				rc = run_payment_4(w_id, d_id, c_id, c_w_id, c_d_id, c_last, h_amount, by_last_name, row);
				rc = run_payment_5(w_id, d_id, c_id, c_w_id, c_d_id, c_last, h_amount, by_last_name, row);
			}
		} else if (tpcc_query->txn_type == TPCC_NEW_ORDER) {
			uint64_t wh_node = GET_NODE_ID(wh_to_part(w_id));
			if (wh_node == g_node_id) {
				rc = new_order_5(w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
			}
			for (uint64_t i = 0; i < tpcc_query->ol_cnt; i++) {
				uint64_t ol_number = i;
				uint64_t ol_i_id = tpcc_query->items[ol_number]->ol_i_id;
				uint64_t ol_supply_w_id = tpcc_query->items[ol_number]->ol_supply_w_id;
				uint64_t ol_quantity = tpcc_query->items[ol_number]->ol_quantity;
				uint64_t ol_amount = tpcc_query->ol_amount;
				uint64_t ol_node = GET_NODE_ID(wh_to_part(ol_supply_w_id));
				if (ol_node == g_node_id) {
					rc = new_order_8(w_id, d_id, remote, ol_i_id, ol_supply_w_id, ol_quantity, ol_number,
													 tpcc_query->o_id, row);
					rc = new_order_9(w_id, d_id, remote, ol_i_id, ol_supply_w_id, ol_quantity, ol_number,
													 ol_amount, tpcc_query->o_id, row);
				}
			}
		}

		rc = reserve();
		if (rc == Abort) {
			txn->rc = Abort;
		}
		break;
	case ARIA_CHECK:
		assert(txn->rc != Abort);
		rc = check();
		if (rc == Abort) {
		txn->rc = Abort;
		}
		break;
	default:
		assert(false);
		break;
	}
	return rc;
}
#endif

RC TPCCTxnManager::run_tpcc_phase2() {
	TPCCQuery* tpcc_query = (TPCCQuery*) query;
	RC rc = RCOK;
	assert(CC_ALG == CALVIN);

	uint64_t w_id = tpcc_query->w_id;
	uint64_t d_id = tpcc_query->d_id;
	uint64_t c_id = tpcc_query->c_id;
	//uint64_t d_w_id = tpcc_query->d_w_id;
	//uint64_t c_w_id = tpcc_query->c_w_id;
	//uint64_t c_d_id = tpcc_query->c_d_id;
	char * c_last = tpcc_query->c_last;
	//double h_amount = tpcc_query->h_amount;
	bool by_last_name = tpcc_query->by_last_name;
	bool remote = tpcc_query->remote;
	uint64_t ol_cnt = tpcc_query->ol_cnt;
	uint64_t o_entry_d = tpcc_query->o_entry_d;
	//uint64_t o_id = tpcc_query->o_id;

	uint64_t part_id_w = wh_to_part(w_id);
	//uint64_t part_id_c_w = wh_to_part(c_w_id);
	bool w_loc = GET_NODE_ID(part_id_w) == g_node_id;
	//bool c_w_loc = GET_NODE_ID(part_id_c_w) == g_node_id;

	set<uint64_t> s_i_ids;
	uint32_t leaf_traversal_cnt;

	switch (tpcc_query->txn_type) {
		case TPCC_PAYMENT :
			break;
		case TPCC_NEW_ORDER :
			if(w_loc) {
				rc = new_order_0( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
				rc = new_order_1( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
				rc = new_order_2( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
				rc = new_order_3( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
				rc = new_order_4( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
				tpcc_query->o_id = *(int64_t *) row->get_value(D_NEXT_O_ID);
				district_row = row;
				//rc = new_order_5( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
			}
			for(uint64_t i = 0; i < tpcc_query->ol_cnt; i++) {

				uint64_t ol_number = i;
				uint64_t ol_i_id = tpcc_query->items[ol_number]->ol_i_id;
				uint64_t ol_supply_w_id = tpcc_query->items[ol_number]->ol_supply_w_id;
				//uint64_t ol_quantity = tpcc_query->items[ol_number].ol_quantity;
				//uint64_t ol_amount = tpcc_query->ol_amount;
				uint64_t part_id_ol_supply_w = wh_to_part(ol_supply_w_id);
				bool ol_supply_w_loc = GET_NODE_ID(part_id_ol_supply_w) == g_node_id;
				if(ol_supply_w_loc) {
					rc = new_order_6(ol_i_id, row);
					rc = new_order_7(ol_i_id, row);
				}
			}
			break;
		case TPCC_ORDER_STATUS:
			assert(w_loc);
			rc = run_order_status_0(w_id, d_id, by_last_name, c_id, c_last, row);
			rc = run_order_status_1(w_id, d_id, c_id, tpcc_query->o_id, row);
			rc = run_order_status_2(w_id, d_id, tpcc_query->o_id, items, row);
			while (items != NULL) {
				row = (row_t *)items->location;
				rc = run_order_status_3(row);
				items = items->next;
			}
			break;
		case TPCC_DELIVERY:
			break;
		case TPCC_STOCK_LEVEL:
			bt_node * leaf;
			rc = run_stock_level_0(w_id, d_id, row);
			rc = run_stock_level_1(tpcc_query->o_id, row);
			rc = run_stock_level_2(w_id, d_id, tpcc_query->o_id, leaf, row);
			assert(*(uint64_t*)((row_t*)(((itemid_t*)(leaf->pointers[leaf->num_keys - 1]))->location))->get_value(OL_O_ID) == tpcc_query->o_id);
			leaf_traversal_cnt = leaf->num_keys;
			for (uint64_t i = 0; i < 20; i++) {
				itemid_t * item = (itemid_t *)leaf->pointers[leaf_traversal_cnt-1];
				row = (row_t *)item->location;
				rc = run_stock_level_3(w_id, d_id, tpcc_query->o_id, row);
				uint64_t s_i_id;
				rc = run_stock_level_4(w_id, s_i_id, row);
				rc = run_stock_level_5(w_id, s_i_id, tpcc_query->threshold, s_i_ids, row);
				leaf_traversal_cnt--;
				if (leaf_traversal_cnt == 0) {
					leaf = leaf->prev;
					if (leaf == NULL) {
						break;
					}
					leaf_traversal_cnt = leaf->num_keys;
				}
			}
			break;
		default:
			assert(false);
	}
	return rc;
}

RC TPCCTxnManager::run_tpcc_phase5() {
	TPCCQuery* tpcc_query = (TPCCQuery*) query;
	RC rc = RCOK;
	assert(CC_ALG == CALVIN);

	uint64_t w_id = tpcc_query->w_id;
	uint64_t d_id = tpcc_query->d_id;
	uint64_t c_id = tpcc_query->c_id;
	uint64_t d_w_id = tpcc_query->d_w_id;
	uint64_t c_w_id = tpcc_query->c_w_id;
	uint64_t c_d_id = tpcc_query->c_d_id;
	char * c_last = tpcc_query->c_last;
	double h_amount = tpcc_query->h_amount;
	bool by_last_name = tpcc_query->by_last_name;
	bool remote = tpcc_query->remote;
	uint64_t ol_cnt = tpcc_query->ol_cnt;
	uint64_t o_entry_d = tpcc_query->o_entry_d;
	uint64_t o_id = tpcc_query->o_id;

	uint64_t part_id_w = wh_to_part(w_id);
	uint64_t part_id_c_w = wh_to_part(c_w_id);
	bool w_loc = GET_NODE_ID(part_id_w) == g_node_id;
	bool c_w_loc = GET_NODE_ID(part_id_c_w) == g_node_id;

	uint64_t sum_amount = 0;

	switch (tpcc_query->txn_type) {
		case TPCC_PAYMENT :
			if(w_loc) {
				rc = run_payment_0(w_id, d_id, d_w_id, h_amount, row);
				rc = run_payment_1(w_id, d_id, d_w_id, h_amount, row);
				rc = run_payment_2(w_id, d_id, d_w_id, h_amount, row);
				rc = run_payment_3(w_id, d_id, d_w_id, h_amount, row);
			}
			if(c_w_loc) {
				rc = run_payment_4( w_id,  d_id, c_id, c_w_id,  c_d_id, c_last, h_amount, by_last_name, row);
				rc = run_payment_5( w_id,  d_id, c_id, c_w_id,  c_d_id, c_last, h_amount, by_last_name, row);
			}
			break;
		case TPCC_NEW_ORDER :
			if(w_loc) {
				row = district_row;
				//rc = new_order_4( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
				rc = new_order_5( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, district_row);
				rc = new_order_5_1( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, district_row);
				rc = new_order_5_2( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, district_row);
				rc = new_order_9_1(w_id, d_id, remote, tpcc_query->o_id, row);
			}
			for(uint64_t i = 0; i < tpcc_query->ol_cnt; i++) {

				uint64_t ol_number = i;
				uint64_t ol_i_id = tpcc_query->items[ol_number]->ol_i_id;
				uint64_t ol_supply_w_id = tpcc_query->items[ol_number]->ol_supply_w_id;
				uint64_t ol_quantity = tpcc_query->items[ol_number]->ol_quantity;
				uint64_t ol_amount = tpcc_query->ol_amount;
				uint64_t part_id_ol_supply_w = wh_to_part(ol_supply_w_id);
				bool ol_supply_w_loc = GET_NODE_ID(part_id_ol_supply_w) == g_node_id;
				if(ol_supply_w_loc) {
				rc = new_order_8(w_id, d_id, remote, ol_i_id, ol_supply_w_id, ol_quantity, ol_number,
													o_id, row);
				rc = new_order_9(w_id, d_id, remote, ol_i_id, ol_supply_w_id, ol_quantity, ol_number,
													ol_amount, o_id, row);
				}
			}
			do_insert();
			break;
		case TPCC_ORDER_STATUS:
			break;
		case TPCC_DELIVERY:
			rc = run_delivery_0(w_id, d_id, tpcc_query->o_id, row);
			rc = run_delivery_1(tpcc_query->o_id, row);
			rc = run_delivery_2(tpcc_query->o_id, row);
			rc = run_delivery_3(w_id, d_id, tpcc_query->o_id, row);
			rc = run_delivery_4(tpcc_query->o_carrier_id, c_id, row);
			rc = run_delivery_5(w_id, d_id, tpcc_query->o_id, items);
			
			while (items != NULL) {
				row = (row_t *)items->location;
				rc = run_delivery_6(row);
				rc = run_delivery_7(tpcc_query->ol_delivery_d, sum_amount, row);
				items = items->next;
			}
			rc = run_delivery_8(w_id, d_id, c_id, row);
			rc = run_delivery_9(sum_amount, row);
			break;
		case TPCC_STOCK_LEVEL:
			break;
		default:
			assert(false);
	}
	return rc;
}

#if TXN_TYPE == TPCC_ALL
RC TPCCTxnManager::do_insert() {
	RC rc = RCOK;
	for (uint64_t i = 0; i < txn->insert_rows.size(); i++) {
		row_t * row = txn->insert_rows[i].first;
		itemid_t * m_item = (itemid_t *) mem_allocator.alloc(sizeof(itemid_t));
		m_item->init();
		m_item->type = DT_row;
		m_item->location = row;
		m_item->valid = true;
		index_btree * index = txn->insert_rows[i].second;
		rc = index->index_insert(row->get_primary_key(), m_item, row->get_part_id(), this);
		if (rc == Abort) {
			return Abort;
		}
		if (index == _wl->i_order) {
			TPCCQuery * tpcc_query = (TPCCQuery*) query;
			_wl->i_order_cust->index_insert(custKey(tpcc_query->c_id, tpcc_query->d_id, tpcc_query->w_id), m_item);
		}
	}
	if (txn->insert_items != NULL) {
		itemid_t * m_item = txn->insert_items;
		row_t * row = (row_t *)m_item->location;
		rc = _wl->i_orderline->index_insert(row->get_primary_key(), m_item, row->get_part_id(), this);
		if (rc == Abort) {
			return Abort;
		}
	}
	return rc;
}
#else
RC TPCCTxnManager::do_insert() {
	return RCOK;
}
#endif

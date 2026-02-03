#include "txn.h"
#include "row.h"
#include "row_aria.h"
#include "aria.h"
#include "global.h"
#include "water_mark.h"

#if CC_ALG == ARIA
std::string get_aria_phase_str(ARIA_PHASE phase) {
    std::string phase_str;
    switch (phase) {
    case ARIA_READ:
        phase_str = "READ";
        break;
    case ARIA_RESERVATION:	
        phase_str = "RESERVATION";
        break;
    case ARIA_CHECK:
        phase_str = "CHECK";
        break;
    case ARIA_COMMIT:
        phase_str = "COMMIT";
        break;
    case ARIA_DONE:
        phase_str = "DONE";
        break;
    default:
        phase_str = "UNKNOWN";
        break;
    }
    return phase_str;
}
#if LONG_TXN_SCHEDULE
void txn_next_aria_phase(uint64_t thd_id, ARIA_PHASE &current_phase, TxnManager * txn_manager) {
    // return;
    uint64_t bid = txn_manager->get_batch_id();
    uint64_t return_id = txn_manager->return_id;
    uint64_t txn_id = txn_manager->get_txn_id();
	uint64_t id = thd_id % g_thread_cnt;
	uint64_t key = get_calvin_key(bid, return_id, txn_id);
    uint64_t old_min_sid = 0;
    bool suc = false;
	switch (current_phase) {
	case ARIA_READ:
        break;
	case ARIA_RESERVATION:
    // 可以进行验证的事务号
        old_min_sid = reservation_check_water_mark->minSid;

        reservation_check_water_mark->mark_consumed_by_pointer(
            txn_manager->rld_pointer,
            thd_id
        );
        // suc = reservation_check_water_mark->mark_completed(key);
        // if (!suc) assert(false);
        // 打印一下，把哪个key置为true了，然后新的min_sid是多少
        DEBUG_SCH("[ARIA] %ld set %s key %ld [%ld,%ld] to complete, and set sid from %ld to %ld\n", thd_id, "RESERVATION->CHECK", key, bid,txn_id, old_min_sid, reservation_check_water_mark->minSid);
        break;
	case ARIA_CHECK:
    // 可以提交的事务号
        old_min_sid = check_commit_water_mark->minSid;

        check_commit_water_mark->mark_consumed_by_pointer(
            txn_manager->cld_pointer,
            thd_id
        );
        // suc = check_commit_water_mark->mark_completed(key);
        // if (!suc) assert(false);
        DEBUG_SCH("[ARIA] %ld set %s key %ld [%ld,%ld] to complete, and set sid from %ld to %ld\n", thd_id, "CHECK->COMMIT", key,bid,txn_id, old_min_sid, check_commit_water_mark->minSid);
        break;
	case ARIA_COMMIT:
		// return ARIA_DONE;
        break;
	default:
		assert(false);
	}
}
#endif

RC TxnManager::reserve() {
    RC rc = RCOK;
    uint64_t txn_id = get_txn_id();
    for (uint64_t i = 0; i < txn->row_cnt; i++) {
        row_t * row = txn->accesses[i]->orig_row;
        if (txn->accesses[i]->type == WR) {
            if (row->manager->write_reseration > txn_id) {
                row->manager->write_reseration = txn_id;
            } else {
                txn->rc = Abort;
                rc = Abort;
            }
        } else {
            if (row->manager->read_reseration > txn_id) {
                row->manager->read_reseration = txn_id;
            }
        }
    }
    return rc;
}

RC TxnManager::check() {
    RC rc = RCOK;
    uint64_t txn_id = get_txn_id();
    #if LONG_TXN_SCHEDULE
    for (uint64_t i = 0; i < txn->row_cnt; i++) {
        row_t * row = txn->accesses[i]->orig_row;
        rc = row->manager->check(this, txn->accesses[i]->type, row);
        // 如果要返回Abort，那么对于流水线Aria来说，应该是重试
        if (rc == WAIT || rc == Abort) {
            rc = WAIT;
            // txn->rc = Abort;
            break;
        }
    }
    #else
    for (uint64_t i = 0; i < txn->row_cnt; i++) {
        row_t * row = txn->accesses[i]->orig_row;
        if (txn->accesses[i]->type == WR) {
            // W->W
            if (row->manager->write_reseration < txn_id) {
                txn->rc = Abort;
                rc = Abort;
                break;
            }
            // R->W, WAR 
            if (row->manager->read_reseration < txn_id) {
                war = true;
            }
        } else {
            // W->R, RAW
            if (row->manager->write_reseration < txn_id) {
                raw = true;
            }
        }
        if (war && raw) {
            txn->rc = Abort;
            rc = Abort;
            break;
        }
    }
    #endif
    return rc;
}

RC TxnManager::finish(RC rc) {
    #if LONG_TXN_SCHEDULE
    return rc;
    #endif
    uint64_t txn_id = get_txn_id();
    // If the txn is commited, then write the value into the original row
    if (rc == Commit) {
        for (uint64_t i = 0; i < txn->row_cnt; i++) {
            if (txn->accesses[i]->type == WR) {
                row_t * row = txn->accesses[i]->orig_row;
                row->copy(txn->accesses[i]->data);
            }
        }
    }
    //If the row is reserved by this txn, then reset it to UINT64_MAX.
    for (uint64_t i = 0; i < txn->row_cnt; i++) {
        row_t * row = txn->accesses[i]->orig_row;
        if (txn->accesses[i]->type == WR) {
            if (row->manager->write_reseration == txn_id) {
                row->manager->write_reseration = UINT64_MAX;
            }
        } else {
            if (row->manager->read_reseration == txn_id) {
                row->manager->read_reseration = UINT64_MAX;
            }
        }
    }
    return rc;
}

#endif

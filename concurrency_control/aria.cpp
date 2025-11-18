#include "txn.h"
#include "row.h"
#include "row_aria.h"
#include "aria.h"

#if CC_ALG == ARIA

#if LONG_TXN_SCHEDULE
bool update_aria_sid(uint64_t thd_id, uint64_t key, uint64_t*& sids, uint64_t& min_sid, ARIA_PHASE phase) {
	uint64_t id = thd_id % g_thread_cnt;
	uint64_t old_sid = sids[id];
	sids[id] = key;
	std::string phase_str;
	switch (phase) {
	case ARIA_READ:
		phase_str = "READ->RESERVATION";
		break;
	case ARIA_RESERVATION:	
		phase_str = "RESERVATION->CHECK";
		break;
	case ARIA_CHECK:
		phase_str = "CHECK->COMMIT";
		break;
	case ARIA_COMMIT:
		phase_str = "COMMIT->READ";
		break;
	default:
		phase_str = "UNKNOWN";
		break;
	}
	DEBUG_SCH("[ARIA] %ld set %s sid from %ld to %ld, now min_sid %ld\n", thd_id, phase_str.c_str(), old_sid, sids[id], min_sid);
	if (id == 0) {
		uint64_t min = UINT64_MAX;
		#if DEBUG_SCHEDULER
		std::string sid_log = "[ARIA] " + std::to_string(thd_id) + phase_str + " min_sid update: sids = ";
		#endif
		for (uint64_t i = 0; i < g_thread_cnt; i++) {
			// sid_log += std::to_string(sids[i]) + " ";
			if (sids[i] < min) min = sids[i];
		}
		assert(min >= min_sid);
		min_sid = min;
		#if DEBUG_SCHEDULER
		sid_log += "| new min_sid = " + std::to_string(min_sid);
		std::cout << sid_log << std::endl;
		#endif
	}
	return true;
}

bool txn_next_aria_phase(uint64_t thd_id, ARIA_PHASE current_phase, uint64_t bid, uint64_t return_id, uint64_t txn_id) {
	uint64_t id = thd_id % g_thread_cnt;
	uint64_t key = get_calvin_key(bid, return_id, txn_id);
	switch (current_phase) {
	case ARIA_READ:
		update_aria_sid(thd_id, key, read_reservation_sids, min_read_reservation_sid, ARIA_READ);
		return ARIA_RESERVATION;
	case ARIA_RESERVATION:
		update_aria_sid(thd_id, key, reservation_check_sids, min_reservation_check_sid, ARIA_RESERVATION);
		return ARIA_CHECK;
	case ARIA_CHECK:
		update_aria_sid(thd_id, key, check_commit_sids, min_check_commit_sid, ARIA_CHECK);
		return ARIA_COMMIT;
	case ARIA_COMMIT:
		update_aria_sid(thd_id, key, commit_read_sids, min_commit_read_sid, ARIA_COMMIT);
		return ARIA_DONE;
	default:
		assert(false);
		return ARIA_DONE;
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
    for (uint64_t i = 0; i < txn->row_cnt; i++) {
        row_t * row = txn->accesses[i]->orig_row;
        if (txn->accesses[i]->type == WR) {
            if (row->manager->write_reseration < txn_id) {
                txn->rc = Abort;
                rc = Abort;
                break;
            }
            if (row->manager->read_reseration < txn_id) {
                war = true;
            }
        } else {
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
    return rc;
}

RC TxnManager::finish(RC rc) {
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

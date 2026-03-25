#include "txn.h"
#include "row.h"
#include "row_aria.h"

#if CC_ALG == ARIA

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

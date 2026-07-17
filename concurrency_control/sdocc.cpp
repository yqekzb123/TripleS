#include "txn.h"
#include "row.h"
#include "row_sdocc.h"
#include "sdocc.h"
#include "global.h"
#include "water_mark.h"

#if CC_ALG == SDOCC
std::string get_sdocc_phase_str(SDOCC_PHASE phase) {
    std::string phase_str;
    switch (phase) {
    case SDOCC_INIT:
        phase_str = "INIT";
        break;
    case SDOCC_EXECUTION:
        phase_str = "EXECUTION";
        break;
    case SDOCC_CHECK:
        phase_str = "CHECK";
        break;
    case SDOCC_COMMIT:
        phase_str = "COMMIT";
        break;
    default:
        assert(false);
        break;
    }
    return phase_str;
}

RC TxnManager::check() {
    RC rc = RCOK;
    // return RCOK;
    uint64_t txn_id = get_txn_id();
    for (uint64_t i = 0; i < txn->row_cnt; i++) {
        Access * access = txn->accesses[i];
        row_t * row = access->orig_row;
        RC rc2 = row->manager->check(this, access->type, row, access);
        // 如果要返回Abort，那么对于SDOCC来说，应该是重试
        if (rc2 == RETRY || rc2 == Abort) {
            rc = RETRY;
        }
        // ! 这里暂时强行设定RCOK，不会因为check而重试
        // rc = RCOK;
    }
    DEBUG_WRK("[%ld] Check SDOCC txn %ld,%ld, rc: %s\n",get_thd_id(),get_batch_id(),txn_id,rc == RCOK? "OK" : "RETRY");
    return rc;
}

void update_local_watermark(uint64_t thd_id, TxnManager * txn_manager) {
    if (!IS_LOCAL(txn_manager->get_txn_id())) {
        // 说明不是本地事务，不需要更新水印
        return;
    }
    uint64_t bid = txn_manager->get_batch_id();
    uint64_t return_id = txn_manager->return_id;
    uint64_t txn_id = txn_manager->get_txn_id();
	uint64_t id = thd_id % g_thread_cnt;
	uint64_t key = get_batch_key(bid, return_id, txn_id);
    uint64_t old_min_sid = 0;
    bool suc = false;

    if (txn_manager->marked_for_retry) {
        // 说明已经标记过了，不需要再标记了
        return;
    }

    old_min_sid = check_water_mark->get_current_watermark();

    check_water_mark->mark_completed(key, thd_id);
    txn_manager->marked_for_retry = true;

    DEBUG_SCH("[SDOCC] %ld set %s key %ld [%ld,%ld] to complete, and set sid from %ld to %ld\n", thd_id, "CHECK", key,bid,txn_id, old_min_sid, check_water_mark->get_current_watermark());
}

#endif

#include "txn.h"
#include "row.h"
#include "row_sdocc.h"
#include "sdocc.h"
#include "global.h"
#include "water_mark.h"
#include "work_queue.h"
#include "msg_queue.h"
#include "message.h"

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
    // 验证开始之前，先把自己塞到队列里。即，给Tj加Tj
    pthread_mutex_lock(&predecessor_lock);
    predecessor_transaction.insert(this);
    pthread_mutex_unlock(&predecessor_lock);

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
    // 验证结束后，再把自己从队列里删除，代表Tj的验证结束了。
    pthread_mutex_lock(&predecessor_lock);
    predecessor_transaction.erase(this);
    pthread_mutex_unlock(&predecessor_lock);
    DEBUG_WRK("[%ld] Check SDOCC txn %ld,%ld, rc: %s\n",get_thd_id(),get_batch_id(),txn_id,rc == RCOK? "OK" : "RETRY");
    return rc;
}

RC TxnManager::finish() {
    pthread_mutex_lock(&successor_lock);
    for (TxnManager* successor : successor_transaction) {
        pthread_mutex_lock(&successor->predecessor_lock);
        auto iter = successor->predecessor_transaction.find(this);
        // assert(iter != successor->predecessor_transaction.end());
        if (iter ==  successor->predecessor_transaction.end()) {
            // 已经被Tj自己处理了
            pthread_mutex_unlock(&successor->predecessor_lock);
            continue;
        }
        successor->predecessor_transaction.erase(this);
        DEBUG_WAIT("txn %ld,%ld clean successor %ld,%ld from its queue, its queue remain %ld.\n",get_batch_id(),get_txn_id(),successor->get_batch_id(),successor->get_txn_id(),successor->predecessor_transaction.size());

        if (successor->predecessor_transaction.size() == 0 && 
            successor->recover_txn == 0) {
            // 如果遗留事务为空，那么此时需要获得Ti的处理权。
            successor->recover_txn = 2;
            // successor->recover_txn = get_txn_id();
        }
        pthread_mutex_unlock(&successor->predecessor_lock);

        // if (successor->predecessor_transaction.size() == 0) {
        // 如果管理权在自己手上
        // if (successor->recover_txn == get_txn_id()) {
        if (successor->recover_txn == 2) {
            if (IS_LOCAL(successor->get_txn_id())) {
                // 本地直接重启
                work_queue.sdocc_enqueue(get_thd_id(), successor->last_msg, false);
                DEBUG_WAIT("txn %ld,%ld re-enqueue successor %ld,%ld into queue\n",get_batch_id(),get_txn_id(),successor->get_batch_id(),successor->get_txn_id());
            } else {
                // 远程发消息
                assert(OPEN_REMOTE_WAIT_COMMIT);
                // if(ATOM_CAS(successor->wait_ready,false,true)) {
                msg_queue.enqueue(get_thd_id(), Message::create_message(successor, SDOCC_ACK),
                    GET_NODE_ID(successor->get_txn_id()));
                DEBUG_WAIT("txn %ld,%ld notice successor %ld,%ld to remote node %ld\n",get_batch_id(),get_txn_id(),successor->get_batch_id(),successor->get_txn_id(),GET_NODE_ID(successor->get_txn_id()));
                // } else {
                    // DEBUG_WRK("txn %ld,%ld no notice successor %ld,%ld because not lock_ready\n",get_batch_id(),get_txn_id(),successor->get_batch_id(),successor->get_txn_id());
                // }
            }
        }
    }
    pthread_mutex_unlock(&successor_lock);
    return RCOK;
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

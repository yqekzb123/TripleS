#include "bomb.h"

#include "catalog.h"
#include "message.h"
#include "msg_queue.h"
#include "row.h"
#if CC_ALG == SDMVCC
#include "row_sdmvcc.h"
#endif

void BombTxnManager::init(uint64_t thd_id, Workload *wl) {
  TxnManager::init(thd_id, wl);
  _bomb_wl = static_cast<BombWorkload *>(wl);
  reset();
}

void BombTxnManager::reset() {
  checksum = 0;
  TxnManager::reset();
}

RC BombTxnManager::run_txn() {
  assert(CALVIN_FAMILY);
  return run_calvin_txn();
}

RC BombTxnManager::run_txn_post_wait() { return RCOK; }

RC BombTxnManager::acquire_locks() {
  assert(CALVIN_FAMILY);
  BombQuery *bomb_query = static_cast<BombQuery *>(query);
  locking_done = false;
  RC rc = RCOK;
  incr_lr();
  for (uint64_t i = 0; i < bomb_query->requests.size(); ++i) {
    BombRequest *request = bomb_query->requests[i];
    uint64_t part = _bomb_wl->request_to_part(*request);
    if (GET_NODE_ID(part) != g_node_id) continue;
    itemid_t *item = index_read(_bomb_wl->request_index(*request),
                                request->key, part);
    assert(item != NULL && item->location != NULL);
    RC lock_rc = get_lock(static_cast<row_t *>(item->location),
                          request->acctype);
    if (lock_rc != RCOK) rc = lock_rc;
  }
  if (decr_lr() == 0 && ATOM_CAS(lock_ready, false, true)) rc = RCOK;
  txn_stats.wait_starttime = get_sys_clock();
  locking_done = true;
  return rc;
}

RC BombTxnManager::access_request(BombRequest *request, bool write_phase) {
  if ((request->acctype == WR) != write_phase) return RCOK;
  uint64_t part = _bomb_wl->request_to_part(*request);
  if (GET_NODE_ID(part) != g_node_id) return RCOK;
  itemid_t *item = index_read(_bomb_wl->request_index(*request),
                              request->key, part);
  assert(item != NULL && item->location != NULL);
  row_t *local = NULL;
  RC rc = get_row(static_cast<row_t *>(item->location), request->acctype, local);
  assert(rc == RCOK && local != NULL);
  if (!write_phase) {
    const char *data = local->get_data();
    for (uint64_t i = 0; i < local->get_tuple_size(); ++i)
      checksum = checksum * 131 + static_cast<unsigned char>(data[i]);
    return RCOK;
  }

  switch (request->role) {
    case BOMB_ROLE_MATERIAL_RMW: {
      double quantity = 0, amount = 0;
      local->get_value(2, quantity); local->get_value(3, amount);
      quantity += 1.0; amount += 1.0;
      local->set_value(2, &quantity, sizeof(quantity));
      local->set_value(3, &amount, sizeof(amount));
      break;
    }
    case BOMB_ROLE_RESULT_WRITE: {
      double cost = static_cast<double>((checksum ^ request->arg0) % 1000000) / 100.0;
      local->set_value(2, &cost, sizeof(cost));
      break;
    }
    case BOMB_ROLE_VOUCHER_WRITE: {
      double amount = static_cast<double>((checksum ^ request->arg0) % 1000000) / 10.0;
      uint64_t active = 1;
      local->set_value(4, &amount, sizeof(amount));
      local->set_value(5, &active, sizeof(active));
      break;
    }
    case BOMB_ROLE_PRODUCT_REPLACE: {
      uint64_t item_id = request->arg0, active = 1, version = 0;
      local->get_value(4, version);
      ++version;
      local->set_value(1, &item_id, sizeof(item_id));
      local->set_value(3, &active, sizeof(active));
      local->set_value(4, &version, sizeof(version));
      break;
    }
    case BOMB_ROLE_BOM_REPLACE: {
      uint64_t child_id = request->arg0, active = 1, version = 0;
      local->get_value(4, version);
      ++version;
      local->set_value(1, &child_id, sizeof(child_id));
      local->set_value(3, &active, sizeof(active));
      local->set_value(4, &version, sizeof(version));
      break;
    }
    case BOMB_ROLE_QUANTITY_WRITE: {
      double quantity = request->value;
      local->set_value(2, &quantity, sizeof(quantity));
      break;
    }
    default:
      assert(false);
  }
  return RCOK;
}

RC BombTxnManager::validate_plan() {
  BombQuery *bomb_query = static_cast<BombQuery *>(query);
  bool mismatch = false;
  for (uint64_t i = 0; i < bomb_query->requests.size(); ++i) {
    BombRequest *request = bomb_query->requests[i];
    if (request->role != BOMB_ROLE_PRODUCT_REPLACE &&
        request->role != BOMB_ROLE_BOM_REPLACE) continue;
    const uint64_t part = _bomb_wl->request_to_part(*request);
    if (GET_NODE_ID(part) != g_node_id) continue;
    itemid_t *item = index_read(_bomb_wl->request_index(*request),
                                request->key, part);
    assert(item != NULL && item->location != NULL);
    uint64_t version = 0;
    row_t *row = static_cast<row_t *>(item->location);
#if CC_ALG == SDMVCC
    // The base row can lag the version chain until GC promotes the newest
    // committed version. Validate against this transaction's snapshot.
    RC read_rc = row->manager->read_value(
        sdmvcc_snapshot(), 4, &version, sizeof(version));
    assert(read_rc == RCOK);
#else
    row->get_value(4, version);
#endif
    if (version != request->expected_version) {
      request->expected_version = version;
#if CC_ALG == SDMVCC
      // The current dynamic prototype plans stable logical slots for S3/S4.
      // A newer predecessor version changes the guard, not the read/write
      // set, so refresh it at this deterministic snapshot and execute in the
      // original SID. Retrying at the tail would chase a moving hot version.
#else
      // Other deterministic engines retain their existing abort/retry path.
      mismatch = true;
#endif
    }
  }
  return mismatch ? Abort : RCOK;
}

RC BombTxnManager::execute_phase(bool writes) {
  BombQuery *bomb_query = static_cast<BombQuery *>(query);
  for (uint64_t i = 0; i < bomb_query->requests.size(); ++i) {
    RC rc = access_request(bomb_query->requests[i], writes);
    if (rc != RCOK) return rc;
  }
  return RCOK;
}

RC BombTxnManager::run_calvin_txn() {
  RC rc = RCOK;
  BombQuery *bomb_query = static_cast<BombQuery *>(query);
  while (!calvin_exec_phase_done() && rc == RCOK) {
    switch (phase) {
      case CALVIN_RW_ANALYSIS:
        bomb_query->get_participants(_bomb_wl);
        calvin_expected_rsp_cnt = 0;
        phase = CALVIN_LOC_RD;
        break;
      case CALVIN_LOC_RD:
        rc = execute_phase(false);
        if (rc == RCOK) rc = validate_plan();
        phase = CALVIN_SERVE_RD;
        break;
      case CALVIN_SERVE_RD:
        // The port intentionally simplifies business arithmetic. Every node
        // performs its faithful local read footprint; no remote values are
        // needed to derive the synthetic checksum written by this prototype.
        phase = query->active_nodes[g_node_id] ? CALVIN_COLLECT_RD : CALVIN_DONE;
        break;
      case CALVIN_COLLECT_RD:
        phase = CALVIN_EXEC_WR;
        break;
      case CALVIN_EXEC_WR:
        rc = execute_phase(true);
        phase = CALVIN_DONE;
        break;
      default:
        assert(false);
    }
  }
  txn_stats.wait_starttime = get_sys_clock();
  return rc;
}

#if CC_ALG == ARIA
bool BombTxnManager::node_has_requests(uint64_t node, bool reads_only) const {
  BombQuery *bomb_query = static_cast<BombQuery *>(query);
  for (uint64_t i = 0; i < bomb_query->requests.size(); ++i) {
    BombRequest *request = bomb_query->requests[i];
    if (GET_NODE_ID(_bomb_wl->request_to_part(*request)) != node) continue;
    if (!reads_only || request->acctype == RD) return true;
  }
  return false;
}

RC BombTxnManager::send_aria_remote(ARIA_PHASE remote_phase,
                                    bool reads_only) {
  for (uint64_t node = 0; node < g_node_cnt; ++node) {
    if (node == g_node_id || !node_has_requests(node, reads_only)) continue;
    BombQueryMessage *msg = static_cast<BombQueryMessage *>(
        Message::create_message(this, RQRY));
    msg->aria_phase = remote_phase;
    msg_queue.enqueue(get_thd_id(), msg, node);
    ++participants_cnt;
  }
  txn_stats.trans_process_network_start_time = get_sys_clock();
  return participants_cnt == 0 ? RCOK : WAIT_REM;
}

void BombTxnManager::merge_version_hints(Array<uint64_t> &hints) {
  assert(hints.size() % 3 == 0);
  BombQuery *bomb_query = static_cast<BombQuery *>(query);
  for (uint64_t i = 0; i < hints.size(); i += 3) {
    const uint64_t table = hints[i];
    const uint64_t key = hints[i + 1];
    const uint64_t version = hints[i + 2];
    for (uint64_t j = 0; j < bomb_query->requests.size(); ++j) {
      BombRequest *request = bomb_query->requests[j];
      if (request->table == table && request->key == key)
        request->expected_version = version;
    }
  }
}

RC BombTxnManager::process_aria_remote(ARIA_PHASE remote_phase) {
  RC rc = RCOK;
  // Remote Aria managers commit independently on RFIN and therefore need a
  // non-empty local footprint for the common commit statistics path.
  query->partitions_touched.add_unique(GET_PART_ID(0, g_node_id));
  switch (remote_phase) {
    case ARIA_READ:
      rc = execute_phase(false);
      break;
    case ARIA_RESERVATION:
      rc = validate_plan();
      if (rc == Abort) {
        txn->rc = Abort;
        break;
      }
      rc = execute_phase(true);
      if (rc == RCOK) rc = reserve();
      if (rc == Abort) txn->rc = Abort;
      break;
    case ARIA_CHECK:
      if (txn->rc != Abort) {
        rc = check();
        if (rc == Abort) txn->rc = Abort;
      } else {
        rc = Abort;
      }
      break;
    default:
      assert(false);
  }
  return rc;
}

RC BombTxnManager::run_aria_txn() {
  RC rc = RCOK;
  const uint64_t starttime = get_sys_clock();
  BombQuery *bomb_query = static_cast<BombQuery *>(query);

  // Aria is a globally-barriered protocol: every node executes the same phase
  // in lockstep. Drive execution from the global phase, exactly like the YCSB
  // Aria implementation. The transaction-local aria_phase is only a progress
  // marker used by the worker to decide re-enqueueing across phases.
  switch (simulation->aria_phase) {
    case ARIA_READ:
      for (uint64_t i = 0; i < bomb_query->requests.size(); ++i) {
        const uint64_t node = GET_NODE_ID(
            _bomb_wl->request_to_part(*bomb_query->requests[i]));
        bomb_query->partitions_touched.add_unique(GET_PART_ID(0, node));
      }
      rc = execute_phase(false);
      assert(rc == RCOK);
      rc = send_aria_remote(ARIA_READ, true);
      assert(rc == RCOK || rc == WAIT_REM);
      assert(aria_phase == ARIA_READ);
      aria_phase = static_cast<ARIA_PHASE>(aria_phase + 1);
      break;

    case ARIA_RESERVATION:
      rc = validate_plan();
      if (rc == Abort) {
        txn->rc = Abort;
      } else {
        rc = execute_phase(true);
        assert(rc == RCOK);
        rc = reserve();
        if (rc == Abort) txn->rc = Abort;
      }
      // All participant nodes receive this phase, including read-only ones,
      // so their local Aria transaction state remains available for CHECK and
      // COMMIT just like the existing YCSB implementation.
      rc = send_aria_remote(ARIA_RESERVATION, false);
      assert(rc == RCOK || rc == WAIT_REM);
      assert(aria_phase == ARIA_RESERVATION);
      aria_phase = static_cast<ARIA_PHASE>(aria_phase + 1);
      break;

    case ARIA_CHECK:
      if (txn->rc != Abort) {
        send_prepare_messages();
        rc = check();
        if (rc == Abort) txn->rc = Abort;
        if (rsp_cnt != 0) rc = WAIT_REM;
      }
      assert(aria_phase == ARIA_CHECK);
      aria_phase = static_cast<ARIA_PHASE>(aria_phase + 1);
      break;

    case ARIA_COMMIT:
      send_finish_messages();
      if (txn->rc == Abort) abort();
      else commit();
      if (rsp_cnt != 0) rc = WAIT_REM;
      assert(aria_phase == ARIA_COMMIT);
      aria_phase = static_cast<ARIA_PHASE>(aria_phase + 1);
      break;

    default:
      assert(false);
  }

  const uint64_t current = get_sys_clock();
  txn_stats.process_time += current - starttime;
  txn_stats.process_time_short += current - starttime;
  txn_stats.wait_starttime = current;
  INC_STATS(get_thd_id(), worker_activate_txn_time, current - starttime);
  return rc;
}
#endif

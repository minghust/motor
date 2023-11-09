// Author: Ming Zhang
// Copyright (c) 2023

#include "process/txn.h"

#include <bitset>

bool TXN::Execute(coro_yield_t& yield, bool fail_abort) {
  // Start executing transaction
  if (read_write_set.empty() && read_only_set.empty()) {
    return true;
  }

  // Run our system
  if (read_write_set.empty()) {
    if (!ExeRO(yield)) {
      goto ABORT;
    }
    // std::cout << "txid: " << tx_id << " CVT [" << std::endl;
    //   CVT* cvt = (CVT*)(read_only_set[0]->fetched_cvt_ptr);
    // for (int i = 0; i < MAX_VCELL_NUM; i++){
    //   std::cout << "sa: " << (int)cvt->vcell[i].sa << " valid: " << (int)cvt->vcell[i].valid << " version: " << cvt->vcell[i].version << " attri_so: " << cvt->vcell[i].attri_so << " attri_bitmap: " << std::bitset<5>(cvt->vcell[i].attri_bitmap) << " ea: " << (int)cvt->vcell[i].ea << std::endl;
    // }
    // std::cout << "]" << std::endl;
  } else {
    if (!ExeRW(yield)) {
      goto ABORT;
    }
  }

  return true;

ABORT:
  if (fail_abort) Abort();
  return false;
}

bool TXN::Commit(coro_yield_t& yield) {
  // In MVCC, read-only txn directly commits
  if (read_write_set.empty()) {
    return true;
  }

  // After obtaining all locks, I get the commit timestamp
  commit_time = ++tx_id_generator;

  if (!Validate(yield)) {
    goto ABORT;
  }

  CommitAll();

  return true;

ABORT:
  Abort();
  return false;
}

// Two reads. First reading the correct version's address, then reading the data itself
bool TXN::ExeRO(coro_yield_t& yield) {
  // You can read from primary or backup
  std::vector<DirectRead> pending_direct_ro;
  std::vector<HashRead> pending_hash_read;

  // Issue reads
  if (!IssueReadROCVT(pending_direct_ro, pending_hash_read)) {
    return false;
  }

  // Yield to other coroutines when waiting for network replies
  coro_sched->Yield(yield, coro_id);

  std::vector<ValueRead> pending_value_read;

  // Receive cvts and issue requests to obtain the raw data
  if (!CheckDirectROCVT(pending_direct_ro, pending_value_read)) {
    return false;
  }

  if (!CheckHashReadCVT(pending_hash_read, pending_value_read)) {
    return false;
  }

  if (!pending_value_read.empty()) {
    coro_sched->Yield(yield, coro_id);
    if (!CheckValueRO(pending_value_read)) {
      return false;
    }
  }

  return true;
}

bool TXN::ExeRW(coro_yield_t& yield) {
  std::vector<DirectRead> pending_direct_ro;
  std::vector<CasRead> pending_cas_rw;
  std::vector<HashRead> pending_hash_read;

  // About insert
  // Case 1) Local cached addr -> 1.1) SUCC. It's actually an update. 1.2) FAIL. Address stale and abort
  // Case 2) Local uncached addr -> HashRead and then find pos to insert
  std::vector<InsertOffRead> pending_insert_off_rw;

  // RW transactions may also have RO data
  if (!IssueReadROCVT(pending_direct_ro, pending_hash_read)) {
    return false;
  }

  if (!IssueReadLockCVT(pending_cas_rw, pending_hash_read, pending_insert_off_rw)) {
    return false;
  }

  // Yield to other coroutines when waiting for network replies
  coro_sched->Yield(yield, coro_id);

  // RDMA_LOG(DBG) << "coro: " << coro_id << " tx_id: " << tx_id << " check read rorw";
  std::vector<ValueRead> pending_value_read;
  std::vector<LockReadCVT> pending_cvt_insert;

  if (!CheckDirectROCVT(pending_direct_ro, pending_value_read)) {
    return false;
  }

  if (!CheckHashReadCVT(pending_hash_read, pending_value_read)) {
    return false;
  }

  if (!CheckCasReadCVT(pending_cas_rw, pending_value_read)) {
    return false;
  }

  if (!CheckInsertCVT(pending_insert_off_rw, pending_cvt_insert, pending_value_read)) {
    return false;
  }

  if (!pending_value_read.empty() || !pending_cvt_insert.empty()) {
    coro_sched->Yield(yield, coro_id);
    if (!CheckValueRW(pending_value_read, pending_cvt_insert)) {
      return false;
    }
  }

  return true;
}

bool TXN::Validate(coro_yield_t& yield) {
  if (read_only_set.empty()) {
    return true;
  }

  std::vector<ValidateRead> pending_validate;
  IssueValidate(pending_validate);

  // Yield to other coroutines when waiting for network replies
  coro_sched->Yield(yield, coro_id);

  auto res = CheckValidate(pending_validate);
  return res;
}

void TXN::Abort() {
  // When failures occur, transactions need to be aborted.
  // In general, the transaction will not abort during committing replicas if no hardware failure occurs
  char* unlock_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
  *((lock_t*)unlock_buf) = 0;
  for (auto& index : locked_rw_set) {
    node_id_t primary_node_id = global_meta_man->GetPrimaryNodeIDWithCrash(read_write_set[index]->header.table_id, PrimaryCrashTime::kAtAbort);
#if HAVE_PRIMARY_CRASH
    if (primary_node_id == PRIMARY_CRASH) {
      // This primary is not recovered yet. I skip it
      continue;
    }
#endif

#if HAVE_BACKUP_CRASH
    if (primary_node_id == BACKUP_CRASH) {
      continue;
    }
#endif
    RCQP* primary_qp = thread_qp_man->GetRemoteDataQPWithNodeID(primary_node_id);
    auto rc = primary_qp->post_send(IBV_WR_RDMA_WRITE, unlock_buf, sizeof(lock_t), read_write_set[index]->GetRemoteLockAddr(), 0);
    if (rc != SUCC) {
      RDMA_LOG(FATAL) << "Thread " << t_id << " , Coroutine " << coro_id << " unlock fails during abortion";
    }
  }
}
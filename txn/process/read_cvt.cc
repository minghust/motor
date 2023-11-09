// Author: Ming Zhang
// Copyright (c) 2023

#include "process/txn.h"
#include "util/latency.h"

bool TXN::IssueReadROCVT(std::vector<DirectRead>& pending_direct_ro,
                         std::vector<HashRead>& pending_hash_read) {
  for (int i = 0; i < read_only_set.size(); i++) {
    if (read_only_set[i]->is_fetched) continue;
    node_id_t remote_node_id = global_meta_man->GetPrimaryNodeIDWithCrash(read_only_set[i]->header.table_id);

#if HAVE_PRIMARY_CRASH
    if (remote_node_id == PRIMARY_CRASH) {
      // RDMA_LOG(INFO) << "Thread " << t_id << " primary fails at readrocvt. table_id = " << read_only_set[i]->header.table_id << " txn id: " << tx_id;
      RecoverPrimary(read_only_set[i]->header.table_id);
      event_counter.RegEvent(t_id, txn_name, "IssueReadROCVT:RecoverPrimary:Abort");
      return false;
    }
#endif

#if HAVE_BACKUP_CRASH
    if (remote_node_id == BACKUP_CRASH) {
      event_counter.RegEvent(t_id, txn_name, "IssueReadROCVT:DiscoveryBackupFail");
      return false;
    }
#endif

    read_only_set[i]->read_which_node = remote_node_id;
    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);
    auto offset = addr_cache->Search(remote_node_id, read_only_set[i]->header.table_id, read_only_set[i]->header.key);
    if (offset != NOT_FOUND) {
      // Find the addr in local addr cache
      read_only_set[i]->header.remote_offset = offset;
      char* cvt_buf = thread_rdma_buffer_alloc->Alloc(CVTSize);
      pending_direct_ro.emplace_back(DirectRead{
          .qp = qp,
          .item = read_only_set[i].get(),
          .buf = cvt_buf,
          .remote_node = remote_node_id,
          .is_ro = true});
      coro_sched->RDMARead(coro_id, qp, cvt_buf, offset, CVTSize);
      // CheckAddr(offset, CVTSize, "IssueReadROCVT:cached_read");
    } else {
      // Local cache does not have
      HashMeta meta = global_meta_man->GetPrimaryHashMetaWithTableID(read_only_set[i]->header.table_id);
      uint64_t bkt_idx = GetHash(read_only_set[i]->header.key, meta.bucket_num, meta.hash_core);
      offset_t bucket_off = bkt_idx * meta.bucket_size + meta.base_off;

      size_t bkt_size = SLOT_NUM[read_only_set[i]->header.table_id] * CVTSize;
      char* local_hash_bucket = thread_rdma_buffer_alloc->Alloc(bkt_size);

      pending_hash_read.emplace_back(HashRead{
          .qp = qp,
          .item = read_only_set[i].get(),
          .buf = local_hash_bucket,
          .remote_node = remote_node_id,
          .item_idx = i,  // not unsed for r-o data
          .is_ro = true});
      coro_sched->RDMARead(coro_id, qp, local_hash_bucket, bucket_off, bkt_size);
      // CheckAddr(bucket_off, bkt_size, "IssueReadROCVT:hash_read");
    }
  }

  return true;
}

bool TXN::IssueReadLockCVT(std::vector<CasRead>& pending_cas_rw,
                           std::vector<HashRead>& pending_hash_read,
                           std::vector<InsertOffRead>& pending_insert_off_rw) {
  // For read-write set, we need to read and lock them
  for (int i = 0; i < read_write_set.size(); i++) {
    if (read_write_set[i]->is_fetched) continue;

    auto remote_node_id = global_meta_man->GetPrimaryNodeIDWithCrash(read_write_set[i]->header.table_id);
#if HAVE_PRIMARY_CRASH
    if (remote_node_id == PRIMARY_CRASH) {
      // RDMA_LOG(INFO) << "Thread " << t_id << " primary fails at rlockcvt. table_id = " << read_write_set[i]->header.table_id << " txn id: " << tx_id;
      RecoverPrimary(read_write_set[i]->header.table_id);
      event_counter.RegEvent(t_id, txn_name, "IssueReadLockCVT:RecoverPrimary:Abort");
      return false;
    }
#endif

#if HAVE_BACKUP_CRASH
    if (remote_node_id == BACKUP_CRASH) {
      event_counter.RegEvent(t_id, txn_name, "IssueReadLockCVT:DiscoveryBackupFail");
      return false;
    }
#endif

    read_write_set[i]->read_which_node = remote_node_id;
    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);
    auto offset = addr_cache->Search(remote_node_id, read_write_set[i]->header.table_id, read_write_set[i]->header.key);
    // Addr cached in local
    if (offset != NOT_FOUND) {
      read_write_set[i]->header.remote_offset = offset;
      // After getting address, use doorbell CAS + READ
      char* cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
      char* cvt_buf = thread_rdma_buffer_alloc->Alloc(CVTSize);
      pending_cas_rw.emplace_back(CasRead{
          .qp = qp,
          .item = read_write_set[i].get(),
          .cas_buf = cas_buf,
          .cvt_buf = cvt_buf,
          .primary_node_id = remote_node_id});

      RecordLockKey(remote_node_id, read_write_set[i]->GetRemoteLockAddr());
      std::shared_ptr<LockReadBatch> doorbell = std::make_shared<LockReadBatch>();
      doorbell->SetLockReq(cas_buf, read_write_set[i]->GetRemoteLockAddr(), STATE_UNLOCKED, tx_id);
      doorbell->SetReadReq(cvt_buf, offset, CVTSize);
      doorbell->SendReqs(coro_sched, qp, coro_id);

      // CheckAddr(read_write_set[i]->GetRemoteLockAddr(), 8, "IssueReadLockCVT:SetLockReq");
      // CheckAddr(offset, CVTSize, "IssueReadLockCVT:SetReadReq");

      locked_rw_set.emplace_back(i);
    } else {
      // Only read
      const HashMeta& meta = global_meta_man->GetPrimaryHashMetaWithTableID(read_write_set[i]->header.table_id);
      uint64_t bkt_idx = GetHash(read_write_set[i]->header.key, meta.bucket_num, meta.hash_core);
      offset_t bucket_off = bkt_idx * meta.bucket_size + meta.base_off;

      size_t bkt_size = SLOT_NUM[read_write_set[i]->header.table_id] * CVTSize;
      char* local_hash_bucket = thread_rdma_buffer_alloc->Alloc(bkt_size);

      if (read_write_set[i]->user_op == UserOP::kInsert) {
        pending_insert_off_rw.emplace_back(InsertOffRead{
            .qp = qp,
            .item = read_write_set[i].get(),
            .buf = local_hash_bucket,
            .remote_node = remote_node_id,
            .item_idx = i,
            .bucket_off = bucket_off});
      } else {
        pending_hash_read.emplace_back(HashRead{
            .qp = qp,
            .item = read_write_set[i].get(),
            .buf = local_hash_bucket,
            .remote_node = remote_node_id,
            .item_idx = i,
            .is_ro = false});
      }
      coro_sched->RDMARead(coro_id, qp, local_hash_bucket, bucket_off, bkt_size);
      // CheckAddr(bucket_off, bkt_size, "IssueReadLockCVT:hash_read");
    }
  }

  return true;
}

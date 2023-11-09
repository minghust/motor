// Author: Ming Zhang
// Copyright (c) 2023

#include "process/txn.h"

extern std::atomic<bool> cannot_lock_new_primary;

void TXN::CommitAll() {
  for (auto& set_it : read_write_set) {
#if OUTPUT_KEY_STAT
    key_counter.RegKey(t_id, KeyType::kKeyCommit, txn_name, set_it->header.table_id, set_it->header.key);
#endif

    assert(set_it->target_write_pos != UN_INIT_POS);

    // Read-write data can only be read from primary
    node_id_t p_node_id = global_meta_man->GetPrimaryNodeIDWithCrash(set_it->header.table_id, PrimaryCrashTime::kDuringCommit);

#if HAVE_PRIMARY_CRASH
    if (p_node_id == PRIMARY_CRASH) {
      // Before I commit, other coordinators cannot lock the new primary
      cannot_lock_new_primary = true;
      // RDMA_LOG(INFO) << "Thread " << t_id << " primary fails at commit. table_id = " << set_it->header.table_id << " txn id: " << tx_id;
      RecoverPrimary(set_it->header.table_id, PrimaryCrashTime::kDuringCommit);
      event_counter.RegEvent(t_id, txn_name, "CommitAll:RecoverPrimary:Commit");
      // Re-get
      p_node_id = global_meta_man->GetPrimaryNodeID(set_it->header.table_id);
    }
#endif

    bool new_attr_bar = false;

    if ((set_it->user_op == UserOP::kUpdate) &&
        (set_it->header.remote_attribute_offset == UN_INIT_POS)) {
      // Update
      // This key is updated the first time, we need to alloc a new attr bar
      set_it->header.remote_attribute_offset = thread_delta_offset_alloc->NextDeltaOffset(ATTR_BAR_SIZE[set_it->header.table_id]);
      new_attr_bar = true;
    } else if (set_it->user_op == UserOP::kInsert) {
      // Insert
      auto vpkg_size = TABLE_VALUE_SIZE[set_it->header.table_id] + sizeof(anchor_t) * 2;
      set_it->header.remote_full_value_offset = thread_delta_offset_alloc->NextDeltaOffset(vpkg_size);

      // Only when successfully inserting item to remote memroy, can we cache this addr in local
      addr_cache->Insert(p_node_id,
                         set_it->header.table_id,
                         set_it->header.key,
                         set_it->header.remote_offset);
    }

    RCQP* primary_qp = thread_qp_man->GetRemoteDataQPWithNodeID(p_node_id);
    WriteReplica(primary_qp,
                 set_it.get(),
                 set_it->target_write_pos,
                 set_it->user_op,
                 new_attr_bar);

    // Commit backup
    bool need_recovery = false;
    auto* backup_node_ids = global_meta_man->GetBackupNodeIDWithCrash(set_it->header.table_id, need_recovery);

    if (!backup_node_ids) {
      // There are no backups in memory pool
      continue;
    }

#if HAVE_BACKUP_CRASH
    if (need_recovery) {
      RecoverBackup(set_it->header.table_id, backup_node_ids->at(0));
      event_counter.RegEvent(t_id, txn_name, "CommitAll:RecoverBackup:Commit");
    }
#endif

    for (size_t i = 0; i < backup_node_ids->size(); i++) {
      RCQP* backup_qp = thread_qp_man->GetRemoteDataQPWithNodeID(backup_node_ids->at(i));

      WriteReplica(backup_qp,
                   set_it.get(),
                   set_it->target_write_pos,
                   set_it->user_op,
                   new_attr_bar);
    }
  }

  thread_locked_key_table[coro_id].num_entry = 0;

#if HAVE_PRIMARY_CRASH
  // The new primary is visible. Others can lock the new primary
  cannot_lock_new_primary = false;
#endif

  return;
}

#if LargeAttrBar
static inline in_offset_t GetStartOff(const DataSetItem* item, bool& has_victim) {
  in_offset_t write_next_attr_startoff = item->remote_so;
  uint64_t mask = 1;
  for (int i = 1; i <= ATTRIBUTE_NUM[item->header.table_id]; i++) {
    if (item->remote_bmp & mask) {
      write_next_attr_startoff += ATTR_SIZE[item->header.table_id][i];
    }
    mask <<= 1;
  }

  if (write_next_attr_startoff + item->current_p > ATTR_BAR_SIZE[item->header.table_id]) {
    return 0;
  }

  return write_next_attr_startoff;
}

#else
static inline int GetAttrLen(bitmap_t bmp, table_id_t table_id) {
  uint64_t mask = 1;
  uint64_t attr_len = 0;

  for (int i = 1; i <= ATTRIBUTE_NUM[table_id]; i++) {
    if (bmp & mask) {
      attr_len += ATTR_SIZE[table_id][i];
    }
    mask <<= 1;
  }

  return attr_len;
}

static inline in_offset_t GetStartOff(const DataSetItem* item, bool& has_victim) {
  // I need to write at [left_margin, right_margin) in attr bar
  int left_margin = item->remote_so + GetAttrLen(item->remote_bmp, item->header.table_id);
  int right_margin = left_margin + item->current_p;

  if (right_margin > ATTR_BAR_SIZE[item->header.table_id]) {
    left_margin = 0;
    right_margin = item->current_p;
  }

  CVT* fetched_cvt = (CVT*)(item->fetched_cvt_ptr);

  for (int i = 0; i < MAX_VCELL_NUM; i++) {
    if (fetched_cvt->vcell[i].valid == 0 || i == item->target_write_pos) {
      continue;
    }

    // Examine each vcell's [left, right) attr margin
    int left = fetched_cvt->vcell[i].attri_so;
    int right = fetched_cvt->vcell[i].attri_so + GetAttrLen(fetched_cvt->vcell[i].attri_bitmap, item->header.table_id);

    if (right <= left_margin || left >= right_margin) {
      continue;
    } else {
      has_victim = true;
      fetched_cvt->vcell[i].valid = 0;
    }
  }

  return left_margin;
}
#endif

void TXN::WriteReplica(RCQP* qp,
                       const DataSetItem* item,
                       int write_pos,
                       uint8_t user_op,
                       bool new_attr_bar) {
  // We cannot use a shared data buf for all the written data, although it seems good
  // to save buffers thanks to the sequential data sending. But it is totally wrong. The reason
  // is that `ibv_post_send' does not guarantee that the RDMA NIC will actually send the data packets
  // when `ibv_post_send' returns. In fact, the RDMA device sends the packets later in an **asynchronous** way.
  // As a result, using a shared data buf will render a bug: The latter cvt will be written to the previous target machine, instead of the latter target machine.
  // Here is the description of `ibv_post_send':
  // ibv_post_send() posts a linked list of Work Requests (WRs) to the Send Queue of a Queue Pair (QP). ibv_post_send() go over all of the entries in the linked list, one by one, check that it is valid, generate a HW-specific Send Request out of it and add it to the tail of the QP's Send Queue without performing any context switch. The RDMA device will handle it (later) in **asynchronous** way. If there is a failure in one of the WRs because the Send Queue is full or one of the attributes in the WR is bad, it stops immediately and return the pointer to that WR.

  switch (user_op) {
    case UserOP::kDelete: {
      HandleDelete(qp, item, write_pos);
      break;
    }
    case UserOP::kUpdate: {
      HandleUpdate(qp, item, write_pos, new_attr_bar);
      break;
    }
    case UserOP::kInsert: {
      HandleInsert(qp, item, write_pos);
      break;
    }
    default: {
      RDMA_LOG(FATAL) << "Invalid write type!";
    }
  }

  return;
}

void TXN::HandleDelete(RCQP* qp, const DataSetItem* item, int write_pos) {
  char* unlock_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
  *(lock_t*)unlock_buf = 0;

  if (item->is_delete_all_invalid) {
    coro_sched->RDMAWrite(coro_id, qp, unlock_buf, item->GetRemoteLockAddr(), sizeof(lock_t));

    // CheckAddr(item->GetRemoteLockAddr(), sizeof(lock_t), "HandleDelete:1:UnlockReq");
    return;
  }

  // New full value space
  auto target_table_id = item->header.table_id;
  auto vpkg_size = TABLE_VALUE_SIZE[target_table_id] + sizeof(anchor_t) * 2;
  char* valuepkg_buf = thread_rdma_buffer_alloc->Alloc(vpkg_size);
  char* p = valuepkg_buf;

  char* valid_buf = thread_rdma_buffer_alloc->Alloc(sizeof(valid_t));
  *(valid_t*)valid_buf = 0;

  if (item->is_delete_no_read_value) {
    std::shared_ptr<DeleteNoFVBatch> doorbell = std::make_shared<DeleteNoFVBatch>();
    doorbell->SetInvalidReq(valid_buf, item->GetRemoteValidAddr(write_pos), sizeof(valid_t));
    doorbell->UnlockReq(unlock_buf, item->GetRemoteLockAddr(), sizeof(lock_t));
    doorbell->SendReqs(coro_sched, qp, coro_id);

    // CheckAddr(item->GetRemoteValidAddr(write_pos), sizeof(valid_t), "HandleDelete:1:SetInvalidReq");
    // CheckAddr(item->GetRemoteLockAddr(), sizeof(lock_t), "HandleDelete:1:UnlockReq");

    return;
  }

  // Recover full value to an old version.

  uint8_t new_anchor;
  if (item->is_delete_newest) {
    // 1) Delete the newest version. anchor reduces by 1
    if (item->valuepkg.sa > 0) {
      new_anchor = item->valuepkg.sa - 1;
    } else {
      new_anchor = (item->header.remote_attribute_offset == UN_INIT_POS) ? 0 : 255;
    }
  } else {
    // 2) Delete middle versions. anchor remains
    new_anchor = item->valuepkg.sa;
  }

  *((anchor_t*)p) = new_anchor;
  p += sizeof(anchor_t);
  // The modified attributes of the deleted version are already copied into valuepkg.value.
  memcpy(p, (char*)&(item->valuepkg.value), TABLE_VALUE_SIZE[target_table_id]);
  p += TABLE_VALUE_SIZE[target_table_id];
  *((anchor_t*)p) = new_anchor;

  std::shared_ptr<DeleteBatch> doorbell = std::make_shared<DeleteBatch>();
  doorbell->SetInvalidReq(valid_buf, item->GetRemoteValidAddr(write_pos), sizeof(valid_t));
  doorbell->SetValueReq(valuepkg_buf, item->header.remote_full_value_offset, vpkg_size);
  doorbell->UnlockReq(unlock_buf, item->GetRemoteLockAddr(), sizeof(lock_t));
  doorbell->SendReqs(coro_sched, qp, coro_id);

  // CheckAddr(item->GetRemoteValidAddr(write_pos), sizeof(valid_t), "HandleDelete:2:SetInvalidReq");
  // CheckAddr(item->header.remote_full_value_offset, vpkg_size, "HandleDelete:2:SetValueReq");
  // CheckAddr(item->GetRemoteLockAddr(), sizeof(lock_t), "HandleDelete:2:UnlockReq");
}

void TXN::HandleUpdate(RCQP* qp,
                       const DataSetItem* item,
                       int write_pos,
                       bool new_attr_bar) {
  // New full value space
  auto target_table_id = item->header.table_id;
  auto vpkg_size = TABLE_VALUE_SIZE[target_table_id] + sizeof(anchor_t) * 2;
  char* valuepkg_buf = thread_rdma_buffer_alloc->Alloc(vpkg_size);
  char* p = valuepkg_buf;

  // Prepare full value
  uint8_t new_anchor = item->valuepkg.sa + 1;  // automatical wrap-around in 0-255

  *((anchor_t*)p) = new_anchor;
  p += sizeof(anchor_t);
  memcpy(p, (char*)&(item->valuepkg.value), TABLE_VALUE_SIZE[target_table_id]);
  p += TABLE_VALUE_SIZE[target_table_id];
  *((anchor_t*)p) = new_anchor;

  // New vcell
  char* vcell_buf = thread_rdma_buffer_alloc->Alloc(VCellSize);
  VCell* new_vcell = (VCell*)vcell_buf;

  new_vcell->sa = new_anchor;
  new_vcell->valid = 1;
  new_vcell->version = commit_time;
  bool has_victim = false;
  new_vcell->attri_so = GetStartOff(item, has_victim);
  new_vcell->attri_bitmap = item->update_bitmap;  // which attributes are modified
  new_vcell->ea = new_anchor;

  char* unlock_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
  *(lock_t*)unlock_buf = 0;

  char* delta_buf = thread_rdma_buffer_alloc->Alloc(item->current_p);  // I modify these attributes
  memcpy(delta_buf, item->old_value_ptr, item->current_p);

  if (new_attr_bar) {
    if (!has_victim) {
      char* attr_addr_buf = thread_rdma_buffer_alloc->Alloc(sizeof(offset_t));
      *(offset_t*)attr_addr_buf = item->header.remote_attribute_offset;

      auto doorbell = std::make_shared<UpdateBatchAttrAddr>();
      doorbell->SetValueReq(valuepkg_buf, item->header.remote_full_value_offset, vpkg_size);
      doorbell->SetDeltaReq(delta_buf, item->header.remote_attribute_offset + new_vcell->attri_so, item->current_p);
      doorbell->SetAttrAddrReq(attr_addr_buf, item->GetRemoteAttrAddr(), sizeof(offset_t));
      doorbell->SetVCellReq(vcell_buf, item->GetRemoteVCellAddr(write_pos), VCellSize);
      doorbell->UnlockReq(unlock_buf, item->GetRemoteLockAddr(), sizeof(lock_t));
      doorbell->SendReqs(coro_sched, qp, coro_id);

      // CheckAddr(item->header.remote_full_value_offset, vpkg_size, "HandleUpdate:1:SetValueReq");
      // CheckAddr(item->header.remote_attribute_offset + new_vcell->attri_so, item->current_p, "HandleUpdate:1:SetDeltaReq");
      // CheckAddr(item->GetRemoteVCellAddr(write_pos), VCellSize, "HandleUpdate:1:SetVCellReq");
      // CheckAddr(item->GetRemoteAttrAddr(), sizeof(offset_t), "HandleUpdate:1:SetAttrAddrReq");
      // CheckAddr(item->GetRemoteLockAddr(), sizeof(lock_t), "HandleUpdate:1:UnlockReq");
    } else {
      CVT* fetched_cvt = (CVT*)(item->fetched_cvt_ptr);
      fetched_cvt->header.lock = tx_id;
      fetched_cvt->header.remote_attribute_offset = item->header.remote_attribute_offset;
      fetched_cvt->vcell[write_pos] = *new_vcell;

      auto doorbell = std::make_shared<UpdateBatch>();
      doorbell->SetValueReq(valuepkg_buf, item->header.remote_full_value_offset, vpkg_size);
      doorbell->SetDeltaReq(delta_buf, item->header.remote_attribute_offset + new_vcell->attri_so, item->current_p);
      doorbell->SetVCellOrCVTReq(item->fetched_cvt_ptr, item->header.remote_offset, CVTSize);
      doorbell->UnlockReq(unlock_buf, item->GetRemoteLockAddr(), sizeof(lock_t));
      doorbell->SendReqs(coro_sched, qp, coro_id);
    }
  } else {
    auto doorbell = std::make_shared<UpdateBatch>();
    doorbell->SetValueReq(valuepkg_buf, item->header.remote_full_value_offset, vpkg_size);
    doorbell->SetDeltaReq(delta_buf, item->header.remote_attribute_offset + new_vcell->attri_so, item->current_p);
    if (!has_victim) {
      doorbell->SetVCellOrCVTReq(vcell_buf, item->GetRemoteVCellAddr(write_pos), VCellSize);
    } else {
      CVT* fetched_cvt = (CVT*)(item->fetched_cvt_ptr);
      fetched_cvt->header.lock = tx_id;
      fetched_cvt->vcell[write_pos] = *new_vcell;
      doorbell->SetVCellOrCVTReq(item->fetched_cvt_ptr, item->header.remote_offset, CVTSize);
    }
    doorbell->UnlockReq(unlock_buf, item->GetRemoteLockAddr(), sizeof(lock_t));
    doorbell->SendReqs(coro_sched, qp, coro_id);

    // CheckAddr(item->header.remote_full_value_offset, vpkg_size, "HandleUpdate:2:SetValueReq");
    // CheckAddr(item->header.remote_attribute_offset + new_vcell->attri_so, item->current_p, "HandleUpdate:2:SetDeltaReq");
    // CheckAddr(item->GetRemoteVCellAddr(write_pos), VCellSize, "HandleUpdate:2:SetVCellReq");
    // CheckAddr(item->GetRemoteLockAddr(), sizeof(lock_t), "HandleUpdate:2:UnlockReq");
  }
}

void TXN::HandleInsert(RCQP* qp,
                       const DataSetItem* item,
                       int write_pos) {
  // New full value space
  auto target_table_id = item->header.table_id;
  auto vpkg_size = TABLE_VALUE_SIZE[target_table_id] + sizeof(anchor_t) * 2;
  char* valuepkg_buf = thread_rdma_buffer_alloc->Alloc(vpkg_size);
  char* p = valuepkg_buf;

  // Prepare header
  char* header_buf = thread_rdma_buffer_alloc->Alloc(HeaderSize);
  Header* new_header = (Header*)header_buf;
  new_header->table_id = target_table_id;
  new_header->lock = STATE_UNLOCKED;
  new_header->key = item->header.key;
  // This offset is set in FindInsertOff
  new_header->remote_offset = item->header.remote_offset;
  // Allocate an offset in remote delta region to insert a full value
  new_header->remote_full_value_offset = item->header.remote_full_value_offset;
  new_header->remote_attribute_offset = UN_INIT_POS;
  new_header->value_size = item->header.value_size;
  new_header->user_inserted = true;

  // Prepare vcell
  char* vcell_buf = thread_rdma_buffer_alloc->Alloc(VCellSize);
  VCell* new_vcell = (VCell*)vcell_buf;

  uint8_t new_anchor = 0;

  new_vcell->sa = new_anchor;
  new_vcell->valid = 1;
  new_vcell->version = commit_time;
  new_vcell->attri_so = 0;
  new_vcell->attri_bitmap = 0;  // which attributes are modified
  new_vcell->ea = new_anchor;

  // Prepare full value
  *((anchor_t*)p) = new_anchor;
  p += sizeof(anchor_t);
  memcpy(p, (char*)&(item->valuepkg.value), TABLE_VALUE_SIZE[target_table_id]);
  p += TABLE_VALUE_SIZE[target_table_id];
  *((anchor_t*)p) = new_anchor;

  std::shared_ptr<InsertBatch> doorbell = std::make_shared<InsertBatch>();
  doorbell->SetValueReq(valuepkg_buf, new_header->remote_full_value_offset, vpkg_size);
  doorbell->SetVCellReq(vcell_buf, item->GetRemoteVCellAddr(write_pos), VCellSize);
  doorbell->SetHeaderReq(header_buf, new_header->remote_offset, HeaderSize);
  doorbell->SendReqs(coro_sched, qp, coro_id);

  // CheckAddr(new_header->remote_full_value_offset, vpkg_size, "HandleInsert:SetValueReq");
  // CheckAddr(item->GetRemoteVCellAddr(write_pos), VCellSize, "HandleInsert:SetVCellReq");
  // CheckAddr(new_header->remote_offset, HeaderSize, "HandleInsert:SetHeaderReq");
}

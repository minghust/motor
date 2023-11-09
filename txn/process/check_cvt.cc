// Author: Ming Zhang
// Copyright (c) 2023

#include "process/txn.h"

bool TXN::CheckDirectROCVT(std::vector<DirectRead>& pending_direct_ro,
                           std::vector<ValueRead>& pending_value_read) {
  // Check results from direct read via local cache
  for (auto& res : pending_direct_ro) {
    auto* fetched_cvt = (CVT*)res.buf;
    DataSetItem* local_item = res.item;

    if (likely(fetched_cvt->header.key == local_item->header.key &&
               fetched_cvt->header.table_id == local_item->header.table_id)) {
                
      local_item->fetched_cvt_ptr = res.buf;

      bool is_read_newest = true;
      int max_version_pos = 0;
      bool is_ea = false;
      bool is_all_invalid = true;

      int read_pos = FindReadPos(fetched_cvt, is_read_newest, max_version_pos, is_ea, is_all_invalid);

      if (is_all_invalid) {
        event_counter.RegEvent(t_id, txn_name, "CheckDirectROCVT:FindReadPos:AllInvalid");
        return false;
      }

      if (is_ea) {
        event_counter.RegEvent(t_id, txn_name, "CheckDirectROCVT:FindReadPos:NoReadPos:EarlyAbort");
        return false;
      }

      if (read_pos == NO_POS) {
        event_counter.RegEvent(t_id, txn_name, "CheckDirectROCVT:FindReadPos:NoReadPos (could due to try read)");
        return false;
      }

      local_item->is_fetched = true;
      local_item->latest_anchor = fetched_cvt->vcell[max_version_pos].sa;

      local_item->header = fetched_cvt->header;

      if (fetched_cvt->vcell[read_pos].IsWritten()) {
        event_counter.RegEvent(t_id, txn_name, "CheckDirectROCVT:VcellIsWritten");
        return false;
      }

      local_item->vcell = fetched_cvt->vcell[read_pos];

      if (!ReadValueRO(res.qp, fetched_cvt, res.item, read_pos, pending_value_read, is_read_newest)) {
        return false;
      }

    } else {
      // The cached address is stale, i.e., key1 is deleted and the slot is occupied by another key2
      // You need a hash read next time to get the correct new slot of the key1
      addr_cache->Insert(res.remote_node,
                         local_item->header.table_id,
                         local_item->header.key,
                         NOT_FOUND);
      event_counter.RegEvent(t_id, txn_name, "CheckDirectROCVT:CachedAddrStale");
      return false;
    }
  }
  return true;
}

bool TXN::CheckCasReadCVT(std::vector<CasRead>& pending_cas_rw,
                          std::vector<ValueRead>& pending_value_read) {
  for (auto& res : pending_cas_rw) {
    if (*((lock_t*)res.cas_buf) != STATE_UNLOCKED) {
      event_counter.RegEvent(t_id, txn_name, "CheckCasReadCVT:LockFail");
      return false;  // Abort() will release all the remote locks
    }
    auto* fetched_cvt = (CVT*)(res.cvt_buf);
    DataSetItem* local_item = res.item;

    if (likely(fetched_cvt->header.key == local_item->header.key &&
               fetched_cvt->header.table_id == local_item->header.table_id)) {
      local_item->is_fetched = true;

      local_item->fetched_cvt_ptr = res.cvt_buf;

      int read_pos = NO_POS;
      int write_pos = NO_POS;

      bool is_read_newest = true;
      int max_version_pos = 0;
      bool is_ea = false;
      bool is_all_invalid = true;

      if (local_item->user_op == UserOP::kDelete) {
        read_pos = FindReadPos(fetched_cvt, is_read_newest, max_version_pos, is_ea, is_all_invalid);

        if (is_all_invalid) {
          // All versions are invalid, I only need to unlock at commit
          local_item->is_delete_all_invalid = true;
          local_item->is_delete_no_read_value = true;
          local_item->header = fetched_cvt->header;
          continue;
        }

        if (is_ea) {
          event_counter.RegEvent(t_id, txn_name, "CheckCasReadCVT:Delete:FindReadPos:NoReadPos:EarlyAbort");
          return false;
        }

        if (read_pos == NO_POS) {
          event_counter.RegEvent(t_id, txn_name, "CheckCasReadCVT:Delete:FindReadPos:NoReadPos");
          return false;
        }

        local_item->target_write_pos = read_pos;
        local_item->latest_anchor = fetched_cvt->vcell[max_version_pos].sa;
      } else {
        // Update
        write_pos = FindCasReadPos(fetched_cvt, read_pos, is_read_newest, max_version_pos, is_ea);

        if (is_ea) {
          event_counter.RegEvent(t_id, txn_name, "CheckCasReadCVT:FindCasReadPos:EarlyAbort");
          return false;
        }

        if (read_pos == NO_POS) {
          if (local_item->user_op == UserOP::kInsert && IsAllInvalid(fetched_cvt)) {
            local_item->header = fetched_cvt->header;
            local_item->target_write_pos = 0;
            continue;
          } else {
            event_counter.RegEvent(t_id, txn_name, "CheckCasReadCVT:FindCasReadPos:NoReadPos");
            return false;
          }
        }

        if (write_pos == NO_POS) {
          event_counter.RegEvent(t_id, txn_name, "CheckCasReadCVT:FindCasReadPos:NoWritePos");
          return false;
        }

        local_item->target_write_pos = write_pos;
        local_item->latest_anchor = fetched_cvt->vcell[max_version_pos].sa;
        local_item->remote_so = fetched_cvt->vcell[max_version_pos].attri_so;
        local_item->remote_bmp = fetched_cvt->vcell[max_version_pos].attri_bitmap;
      }

      if (fetched_cvt->vcell[read_pos].IsWritten()) {
        event_counter.RegEvent(t_id, txn_name, "CheckCasReadCVT:VcellIsWritten");
        return false;
      }

      local_item->header = fetched_cvt->header;
      local_item->vcell = fetched_cvt->vcell[read_pos];

      if (local_item->user_op == UserOP::kInsert) {
        local_item->user_op = UserOP::kUpdate;
      }

      if (!ReadValueRW(res.qp, fetched_cvt, res.item, read_pos, pending_value_read, is_read_newest)) {
        return false;
      }
    } else {
      // The cached address is stale, i.e., key1 is deleted and the slot is occupied by another key2
      // You need a hash read next time to get the correct new slot of the key1
      addr_cache->Insert(res.primary_node_id,
                         local_item->header.table_id,
                         local_item->header.key,
                         NOT_FOUND);
      event_counter.RegEvent(t_id, txn_name, "CheckCasReadCVT:CachedAddrStale");
      return false;  // Abort() will release all the remote locks
    }
  }

  return true;
}

// --------------- Processing reading Hash buckets -----------------
bool TXN::CheckHashReadCVT(std::vector<HashRead>& pending_hash_read,
                           std::vector<ValueRead>& pending_value_read) {
  // Check results from hash read
  for (auto& res : pending_hash_read) {
    res.item->is_fetched = true;

    int read_pos = NO_POS;
    bool is_read_newest = true;
    auto cvt_idx = FindMatch(res, read_pos, is_read_newest);

    if (cvt_idx == NOT_FOUND) {
      return false;
    }

    // CVT* fetched_cvt = &(((HashBucket*)res.buf)->cvts[cvt_idx]);
    CVT* fetched_cvt = (CVT*)(res.buf + cvt_idx * CVTSize);

    if (res.is_ro) {
      // 1. Read ro data
      if (!ReadValueRO(res.qp, fetched_cvt, res.item, read_pos, pending_value_read, is_read_newest)) {
        return false;
      }
    } else {
      // 2. Lock and read rw data
      if (!LockReadValueRW(res.qp, res.remote_node, fetched_cvt, res.item, read_pos, pending_value_read, res.item_idx, is_read_newest)) {
        return false;
      }
    }
  }
  return true;
}

int TXN::FindMatch(HashRead& res,
                   int& read_pos,
                   bool& is_read_newest) {
  // auto* fetched_hash_bucket = (HashBucket*)res.buf;
  DataSetItem* local_item = res.item;

  for (int slot_idx = 0; slot_idx < SLOT_NUM[local_item->header.table_id]; slot_idx++) {
    // CVT* fetched_cvt = &(fetched_hash_bucket->cvts[slot_idx]);
    CVT* fetched_cvt = (CVT*)(res.buf + slot_idx * CVTSize);

    if (fetched_cvt->header.value_size > 0) {
      addr_cache->Insert(
          res.remote_node,
          fetched_cvt->header.table_id,
          fetched_cvt->header.key,
          fetched_cvt->header.remote_offset);
    }

    if (fetched_cvt->header.key == local_item->header.key &&
        fetched_cvt->header.table_id == local_item->header.table_id) {
      
      local_item->fetched_cvt_ptr = res.buf + slot_idx * CVTSize;

      int max_version_pos = 0;
      bool is_ea = false;
      bool is_all_invalid = true;

      read_pos = FindReadPos(fetched_cvt, is_read_newest, max_version_pos, is_ea, is_all_invalid);

      if (is_all_invalid && res.item->user_op == UserOP::kDelete) {
        // All versions are invalid, I only need to unlock at commit
        local_item->is_delete_all_invalid = true;
        local_item->is_delete_no_read_value = true;
        local_item->header = fetched_cvt->header;
        return slot_idx;
      }

      if (is_ea) {
        event_counter.RegEvent(t_id, txn_name, "HashFindMatch:FindReadPos:NoReadPos:EarlyAbort");
        return NOT_FOUND;
      }

      if (read_pos == NO_POS) {
        if (res.item->user_op == UserOP::kDelete) {
          event_counter.RegEvent(t_id, txn_name, "HashFindMatch:Delete:FindReadPos:NoReadPos");
        } else {
          event_counter.RegEvent(t_id, txn_name, "HashFindMatch:Update:FindReadPos:NoReadPos");
        }

        return NOT_FOUND;
      }

      if (fetched_cvt->vcell[read_pos].IsWritten()) {
        event_counter.RegEvent(t_id, txn_name, "HashFindMatch:VcellIsWritten");
        return NOT_FOUND;
      }

      local_item->header = fetched_cvt->header;
      local_item->vcell = fetched_cvt->vcell[read_pos];

      if (local_item->user_op == UserOP::kDelete) {
        local_item->target_write_pos = read_pos;
      }

      local_item->latest_anchor = fetched_cvt->vcell[max_version_pos].sa;

      return slot_idx;
    }
  }

  event_counter.RegEvent(t_id, txn_name, "HashFindMatch:NoMatch (Could due to try read)");
  return NOT_FOUND;
}

// --------------- Processing insertions -----------------

bool TXN::CheckInsertCVT(std::vector<InsertOffRead>& pending_insert_off_rw,
                         std::vector<LockReadCVT>& pending_cvt_insert,
                         std::vector<ValueRead>& pending_value_read) {
  for (auto& res : pending_insert_off_rw) {
    res.item->is_fetched = true;

    int read_pos = NO_POS;
    bool is_read_newest = true;
    auto cvt_idx = FindInsertOff(res, read_pos, is_read_newest);

    if (cvt_idx == NOT_FOUND) {
      return false;
    }

    // For real insert, we do not need to read old value, because:
    // 1) the client does not use it, and 2) the client will prepare a new value
    // Hence, we only need to read + lock remote CVTs
    if (res.item->user_op == UserOP::kInsert) {
      char* lock_buff = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
      *(lock_t*)lock_buff = 0xdeadbeaf;

      char* cvt_buff = thread_rdma_buffer_alloc->Alloc(CVTSize);

      RecordLockKey(res.remote_node, res.item->GetRemoteLockAddr());

      auto doorbell = std::make_shared<LockReadBatch>();
      doorbell->SetLockReq(lock_buff, res.item->GetRemoteLockAddr(), STATE_UNLOCKED, tx_id);
      doorbell->SetReadReq(cvt_buff, res.item->header.remote_offset, CVTSize);
      doorbell->SendReqs(coro_sched, res.qp, coro_id);

      // CheckAddr(res.item->GetRemoteLockAddr(), 8, "CheckInsertCVT:SetLockReq");
      // CheckAddr(res.item->header.remote_offset, CVTSize, "CheckInsertCVT:SetReadReq");

      locked_rw_set.emplace_back(res.item_idx);

      pending_cvt_insert.emplace_back(LockReadCVT{.item = res.item,
                                                  .lock_buf = lock_buff,
                                                  .cvt_buf = cvt_buff});
    } else {
      // For updates
      // CVT* fetched_cvt = &(((HashBucket*)res.buf)->cvts[cvt_idx]);
      CVT* fetched_cvt = (CVT*)(res.buf + cvt_idx * CVTSize);
      if (!LockReadValueRW(res.qp, res.remote_node, fetched_cvt, res.item, read_pos, pending_value_read, res.item_idx, is_read_newest)) {
        return false;
      }
    }
  }
  return true;
}

int TXN::FindInsertOff(InsertOffRead& res,
                       int& read_pos,
                       bool& is_read_newest) {
  offset_t insert_cvt_pos = NOT_FOUND;
  // auto* fetched_hash_bucket = (HashBucket*)res.buf;
  DataSetItem* local_item = res.item;

  int target_slot = 0;

  bool real_insert = true;  // is insert or update?

  for (int i = 0; i < SLOT_NUM[local_item->header.table_id]; i++) {
    // CVT* fetched_cvt = &(fetched_hash_bucket->cvts[i]);
    CVT* fetched_cvt = (CVT*)(res.buf + i * CVTSize);

    if (fetched_cvt->header.value_size > 0) {
      addr_cache->Insert(
          res.remote_node,
          fetched_cvt->header.table_id,
          fetched_cvt->header.key,
          fetched_cvt->header.remote_offset);
    }

    // Here we do not need to judge whether the empty cvt is locked or not, since
    // the locking status can be detected in Validation phase
    if ((insert_cvt_pos == NOT_FOUND) && (fetched_cvt->header.value_size == 0)) {
      // Within a txn, multiple items cannot insert into the same slot
      std::pair<node_id_t, offset_t> new_pos(res.remote_node, res.bucket_off + i * CVTSize);
      if (inserted_pos.find(new_pos) != inserted_pos.end()) {
        continue;
      } else {
        inserted_pos.insert(new_pos);
      }
      // We only need one possible empty and unlocked place to insert.
      // This case is entered only once
      insert_cvt_pos = res.bucket_off + i * CVTSize;
      target_slot = i;

    } else if ((fetched_cvt->header.key == local_item->header.key) &&
               (fetched_cvt->header.table_id == local_item->header.table_id)) {
      target_slot = i;

      int max_version_pos = 0;
      bool is_ea = false;
      bool is_all_invalid = true;

      read_pos = FindReadPos(fetched_cvt, is_read_newest, max_version_pos, is_ea, is_all_invalid);

      if (is_all_invalid) {
        // Do as an insert
        insert_cvt_pos = res.bucket_off + i * CVTSize;
        local_item->is_insert_all_invalid = true;
        break;
      }

      local_item->user_op = UserOP::kUpdate;
      real_insert = false;
      
      local_item->fetched_cvt_ptr = res.buf + i * CVTSize;

      if (is_ea) {
        event_counter.RegEvent(t_id, txn_name, "FindInsertOff:FindReadPos:EarlyAbort");
        return NOT_FOUND;
      }

      if (read_pos == NO_POS) {
        event_counter.RegEvent(t_id, txn_name, "FindInsertOff:FindReadPos:NoReadPos");
        return NOT_FOUND;
      }

      // Actually Update due to inserting an existing Key
      if (fetched_cvt->vcell[read_pos].IsWritten()) {
        event_counter.RegEvent(t_id, txn_name, "FindInsertOff:VcellIsWritten");
        return NOT_FOUND;
      }

      local_item->header = fetched_cvt->header;
      local_item->vcell = fetched_cvt->vcell[read_pos];

      local_item->latest_anchor = fetched_cvt->vcell[max_version_pos].sa;

      break;
    }
  }

  if (real_insert) {
    if (insert_cvt_pos == NOT_FOUND) {
      event_counter.RegEvent(t_id, txn_name, "FindInsertOff:NoEmptySlot");
      return NOT_FOUND;
    }
    // If it is an insert, we do not assign fetched_cvt's header and vcell to local item
    // Because fetched_cvt could be empty without a useful header and vcell
    local_item->header.remote_offset = insert_cvt_pos;  // set cvt's remote addr
    local_item->insert_slot_idx = target_slot;
  }

  return target_slot;
}

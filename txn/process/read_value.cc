// Author: Ming Zhang
// Copyright (c) 2023

#include <bitset>

#include "process/txn.h"

// --------------- Reading Full Data Value and Attributes -----------------
bool TXN::ReadValueRO(RCQP* qp,
                      CVT* fetched_cvt,
                      DataSetItem* item_ptr,
                      int read_pos,
                      std::vector<ValueRead>& pending_value_read,
                      bool is_read_newest) {
  table_id_t table_id = item_ptr->header.table_id;
  offset_t val_off = item_ptr->header.remote_full_value_offset;

  size_t fv_size = TABLE_VALUE_SIZE[table_id] + sizeof(anchor_t) * 2;  // full value size
  char* fv_buff = thread_rdma_buffer_alloc->Alloc(fv_size);

  if (is_read_newest) {
    // event_counter.RegEvent(t_id, txn_name, "ReadValueRO:ReadNewest");
    // Only need to read value

    coro_sched->RDMARead(coro_id, qp, fv_buff, val_off, fv_size);

    // CheckAddr(val_off, fv_size, "ReadValueRO:ReadNew");

    pending_value_read.emplace_back(
        ValueRead{
            .item = item_ptr,
            .value_buf = fv_buff,
            .lock_buf = nullptr,
            .cvt_buf = nullptr,
            .attr_pos = nullptr,
            .old_attr_pos = nullptr,
            .cont = Content::kValue});
  } else {
    if (fetched_cvt->header.remote_attribute_offset <= 0) {
      event_counter.RegEvent(t_id, txn_name, "ReadValueRO:ReadOld:remote_attribute_offset_not_set");
      return false;
    }

    // event_counter.RegEvent(t_id, txn_name, "ReadValueRO:NotReadNewest");
    // Read full value + all modified attributes

    //                 +--> this version records the modifications of read pos's version
    //                 |
    // [read pos] [next pos]
    auto next_pos = (read_pos + 1) % MAX_VCELL_NUM;

    std::vector<AttrRead> attr_read_list;

    AttrPos* attr_pos = new AttrPos();
    std::vector<OldAttrPos>* old_attr_pos = new std::vector<OldAttrPos>;

    CollectAttr(attr_read_list, attr_pos, old_attr_pos, table_id, fetched_cvt, next_pos, item_ptr);

    auto doorbell = std::make_shared<ReadValueAttrBatch>(attr_read_list.size());
    doorbell->SetReadValueReq(fv_buff, val_off, fv_size);
    doorbell->SetReadAttrReq(attr_read_list);
    doorbell->SendReqs(coro_sched, qp, coro_id);

    // CheckAddr(val_off, fv_size, "ReadValueRO:ReadOld:SetReadValueReq");
    // for (int i = 0; i < attr_read_list.size(); i++) {
    //   const std::string desc = "ReadValueRO:ReadOld:attr_read_list:" + std::to_string(i);
    //   CheckAddr(attr_read_list[i].remote_attr_off, attr_read_list[i].attr_size, desc);
    // }

    pending_value_read.emplace_back(
        ValueRead{
            .item = item_ptr,
            .value_buf = fv_buff,
            .lock_buf = nullptr,
            .cvt_buf = nullptr,
            .attr_pos = attr_pos,
            .old_attr_pos = old_attr_pos,
            .cont = Content::kValue_Attr});
  }

  return true;
}

bool TXN::ReadValueRW(RCQP* qp,
                      CVT* fetched_cvt,
                      DataSetItem* item_ptr,
                      int read_pos,
                      std::vector<ValueRead>& pending_value_read,
                      bool is_read_newest) {
  // handle update and delete
  table_id_t table_id = item_ptr->header.table_id;
  offset_t val_off = item_ptr->header.remote_full_value_offset;

  size_t fv_size = TABLE_VALUE_SIZE[table_id] + sizeof(anchor_t) * 2;  // full value size
  char* fv_buff = thread_rdma_buffer_alloc->Alloc(fv_size);

  if (is_read_newest) {
    if (item_ptr->user_op == kUpdate) {
      // event_counter.RegEvent(t_id, txn_name, "ReadValueRW:ReadNewest:Update");
      // Only need to read value
      coro_sched->RDMARead(coro_id, qp, fv_buff, val_off, fv_size);
      // CheckAddr(val_off, fv_size, "ReadValueRW:ReadNew:Update");

      pending_value_read.emplace_back(
          ValueRead{
              .item = item_ptr,
              .value_buf = fv_buff,
              .lock_buf = nullptr,
              .cvt_buf = nullptr,
              .attr_pos = nullptr,
              .old_attr_pos = nullptr,
              .cont = Content::kValue});
    } else if (item_ptr->user_op == kDelete) {
      // Read value and the newest vcell's undos
      AttrPos* attr_pos = new AttrPos();

      auto attr_len = CollectDeleteNewestAttr(attr_pos, fetched_cvt->vcell[read_pos].attri_bitmap, table_id);

      if (attr_len == 0) {
        // event_counter.RegEvent(t_id, txn_name, "ReadValueRW:ReadNewest:Delete:NotReadFVAttr");
        // Delete the init loaded fv
        pending_value_read.emplace_back(
            ValueRead{
                .item = nullptr,
                .value_buf = nullptr,
                .lock_buf = nullptr,
                .cvt_buf = nullptr,
                .attr_pos = nullptr,
                .old_attr_pos = nullptr,
                .cont = Content::kDelete_Vcell});
        item_ptr->is_delete_no_read_value = true;
        return true;
      }

      // event_counter.RegEvent(t_id, txn_name, "ReadValueRW:ReadNewest:Delete:ReadFVAttr");

      char* must_read_attrs_buf = thread_rdma_buffer_alloc->Alloc(attr_len);
      attr_pos->local_attr_buf = must_read_attrs_buf;

      auto doorbell = std::make_shared<DeleteRead>();
      doorbell->SetReadValueReq(fv_buff, val_off, fv_size);
      doorbell->SetReadAttrReq(must_read_attrs_buf,
                               fetched_cvt->vcell[read_pos].attri_so + fetched_cvt->header.remote_attribute_offset,
                               attr_len);

      doorbell->SendReqs(coro_sched, qp, coro_id);

      // CheckAddr(val_off, fv_size, "ReadValueRW:ReadNew:Delete:SetReadValueReq");
      // CheckAddr(fetched_cvt->vcell[read_pos].attri_so + fetched_cvt->header.remote_attribute_offset, attr_len, "ReadValueRW:ReadNew:Delete:SetReadAttrReq");

      pending_value_read.emplace_back(
          ValueRead{
              .item = item_ptr,
              .value_buf = fv_buff,
              .lock_buf = nullptr,
              .cvt_buf = nullptr,
              .attr_pos = attr_pos,
              .old_attr_pos = nullptr,
              .cont = Content::kDelete_Value_Attr});
    }
  } else {
    // Read full value + all modified attributes

    //                 +--> this version records the modifications of read pos's version
    //                 |
    // [read pos] [next pos]

    if (fetched_cvt->header.remote_attribute_offset <= 0) {
      event_counter.RegEvent(t_id, txn_name, "ReadValueRW:ReadOld:remote_attribute_offset_not_set");
      return false;
    }

    if (item_ptr->user_op == kUpdate) {
      // event_counter.RegEvent(t_id, txn_name, "ReadValueRW:NotReadNewest:Update");

      int next_pos;
      do {
        next_pos = (read_pos + 1) % MAX_VCELL_NUM;
      } while (!(fetched_cvt->vcell[next_pos].valid));

      std::vector<AttrRead> attr_read_list;

      AttrPos* attr_pos = new AttrPos();
      std::vector<OldAttrPos>* old_attr_pos = new std::vector<OldAttrPos>;

      CollectAttr(attr_read_list, attr_pos, old_attr_pos, table_id, fetched_cvt, next_pos, item_ptr);

      auto doorbell = std::make_shared<ReadValueAttrBatch>(attr_read_list.size());
      doorbell->SetReadValueReq(fv_buff, val_off, fv_size);
      doorbell->SetReadAttrReq(attr_read_list);
      doorbell->SendReqs(coro_sched, qp, coro_id);

      // CheckAddr(val_off, fv_size, "ReadValueRW:ReadOld:Update:SetReadValueReq");
      // for (int i = 0; i < attr_read_list.size(); i++) {
      //   const std::string desc = "ReadValueRW:ReadOld:attr_read_list:" + std::to_string(i);
      //   CheckAddr(attr_read_list[i].remote_attr_off, attr_read_list[i].attr_size, desc);
      // }

      pending_value_read.emplace_back(
          ValueRead{
              .item = item_ptr,
              .value_buf = fv_buff,
              .lock_buf = nullptr,
              .cvt_buf = nullptr,
              .attr_pos = attr_pos,
              .old_attr_pos = old_attr_pos,
              .cont = Content::kValue_Attr});
    } else if (item_ptr->user_op == kDelete) {
      // event_counter.RegEvent(t_id, txn_name, "ReadValueRW:NotReadNewest:Delete");
      item_ptr->is_delete_newest = false;

      AttrPos* attr_pos = new AttrPos();

      // We should read read_pos's modifications (these modifications are not modified by newer versions) to recover remote full value
      auto attr_len = CollectDeleteMiddleAttr(attr_pos, fetched_cvt, read_pos, table_id);

      if (attr_len == 0) {
        // event_counter.RegEvent(t_id, txn_name, "ReadValueRW:NotReadNewest:Delete:NoReadFVAttr");
        // Delete the init loaded fv
        pending_value_read.emplace_back(
            ValueRead{
                .item = nullptr,
                .value_buf = nullptr,
                .lock_buf = nullptr,
                .cvt_buf = nullptr,
                .attr_pos = nullptr,
                .old_attr_pos = nullptr,
                .cont = Content::kDelete_Vcell});
        item_ptr->is_delete_no_read_value = true;
        return true;
      }

      char* must_read_attrs_buf = thread_rdma_buffer_alloc->Alloc(attr_len);
      attr_pos->local_attr_buf = must_read_attrs_buf;

      auto doorbell = std::make_shared<DeleteRead>();
      doorbell->SetReadValueReq(fv_buff, val_off, fv_size);
      doorbell->SetReadAttrReq(must_read_attrs_buf,
                               fetched_cvt->vcell[read_pos].attri_so + fetched_cvt->header.remote_attribute_offset,
                               attr_len);

      doorbell->SendReqs(coro_sched, qp, coro_id);

      // CheckAddr(val_off, fv_size, "ReadValueRW:ReadOld:Delete:SetReadValueReq");
      // CheckAddr(fetched_cvt->vcell[read_pos].attri_so + fetched_cvt->header.remote_attribute_offset, attr_len, "ReadValueRW:ReadOld:Delete:SetReadAttrReq");

      pending_value_read.emplace_back(
          ValueRead{
              .item = item_ptr,
              .value_buf = fv_buff,
              .lock_buf = nullptr,
              .cvt_buf = nullptr,
              .attr_pos = attr_pos,
              .old_attr_pos = nullptr,
              .cont = Content::kDelete_Value_Attr});
    }
  }

  return true;
}

bool TXN::LockReadValueRW(RCQP* qp,
                          node_id_t remote_node,
                          CVT* fetched_cvt,
                          DataSetItem* item_ptr,
                          int read_pos,
                          std::vector<ValueRead>& pending_value_read,
                          int item_idx,
                          bool is_read_newest) {
  table_id_t table_id = item_ptr->header.table_id;

  char* lock_buff = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
  *(lock_t*)lock_buff = 0xdeadbeaf;

  char* cvt_buff = thread_rdma_buffer_alloc->Alloc(CVTSize);

  size_t fv_size = TABLE_VALUE_SIZE[table_id] + sizeof(anchor_t) * 2;
  char* fv_buff = thread_rdma_buffer_alloc->Alloc(fv_size);

  RecordLockKey(remote_node, item_ptr->GetRemoteLockAddr());

  if (is_read_newest) {
    if (item_ptr->user_op == UserOP::kUpdate) {
      // event_counter.RegEvent(t_id, txn_name, "LockReadValueRW:ReadNewest:Update");
      // 1) Lock cvt, re-read cvt, read full value

      auto doorbell = std::make_shared<LockReadTwoBatch>();
      doorbell->SetLockReq(lock_buff, item_ptr->GetRemoteLockAddr(), STATE_UNLOCKED, tx_id);
      doorbell->SetReadCVTReq(cvt_buff, item_ptr->header.remote_offset, CVTSize);  // Re-read the cvt
      doorbell->SetReadValueReq(fv_buff, item_ptr->header.remote_full_value_offset, fv_size);
      doorbell->SendReqs(coro_sched, qp, coro_id);

      // CheckAddr(item_ptr->GetRemoteLockAddr(), 8, "LockReadValueRW:ReadNewFV:Update:SetLockReq");
      // CheckAddr(item_ptr->header.remote_offset, CVTSize, "LockReadValueRW:ReadNewFV:Update:SetReadCVTReq");
      // CheckAddr(item_ptr->header.remote_full_value_offset, fv_size, "LockReadValueRW:ReadNewFV:Update:SetReadValueReq");

      pending_value_read.emplace_back(
          ValueRead{
              .item = item_ptr,
              .value_buf = fv_buff,
              .lock_buf = lock_buff,
              .cvt_buf = cvt_buff,
              .attr_pos = nullptr,
              .old_attr_pos = nullptr,
              .cont = Content::kValue_LockCVT});
    } else if (item_ptr->user_op == UserOP::kDelete) {
      // 1) Lock cvt, re-read cvt, read full value, read attr

      if (item_ptr->is_delete_all_invalid) {
        auto doorbell = std::make_shared<DeleteLock>();
        doorbell->SetLockReq(lock_buff, item_ptr->GetRemoteLockAddr(), STATE_UNLOCKED, tx_id);
        doorbell->SetReadCVTReq(cvt_buff, item_ptr->header.remote_offset, CVTSize);  // Re-read the cvt
        doorbell->SendReqs(coro_sched, qp, coro_id);

        // CheckAddr(item_ptr->GetRemoteLockAddr(), 8, "LockReadValueRW:ReadNew:Delete:SetLockReq:NoReadValue");
        // CheckAddr(item_ptr->header.remote_offset, CVTSize, "LockReadValueRW:ReadNew:Delete:SetReadCVTReq:NoReadValue");

        pending_value_read.emplace_back(
            ValueRead{
                .item = item_ptr,
                .value_buf = nullptr,
                .lock_buf = lock_buff,
                .cvt_buf = cvt_buff,
                .attr_pos = nullptr,
                .old_attr_pos = nullptr,
                .cont = Content::kDelete_AllInvalid_LockCVT});
        item_ptr->is_delete_no_read_value = true;
        
        locked_rw_set.emplace_back(item_idx);
        return true;
      }

      AttrPos* attr_pos = new AttrPos();

      auto attr_len = CollectDeleteNewestAttr(attr_pos, fetched_cvt->vcell[read_pos].attri_bitmap, table_id);

      if (attr_len == 0) {
        // event_counter.RegEvent(t_id, txn_name, "LockReadValueRW:ReadNewest:Delete:OnlyOneNewestValue");
        // Delete the init loaded fv
        auto doorbell = std::make_shared<DeleteLock>();
        doorbell->SetLockReq(lock_buff, item_ptr->GetRemoteLockAddr(), STATE_UNLOCKED, tx_id);
        doorbell->SetReadCVTReq(cvt_buff, item_ptr->header.remote_offset, CVTSize);  // Re-read the cvt
        doorbell->SendReqs(coro_sched, qp, coro_id);

        // CheckAddr(item_ptr->GetRemoteLockAddr(), 8, "LockReadValueRW:ReadNew:Delete:SetLockReq:NoReadValue");
        // CheckAddr(item_ptr->header.remote_offset, CVTSize, "LockReadValueRW:ReadNew:Delete:SetReadCVTReq:NoReadValue");

        pending_value_read.emplace_back(
            ValueRead{
                .item = item_ptr,
                .value_buf = nullptr,
                .lock_buf = lock_buff,
                .cvt_buf = cvt_buff,
                .attr_pos = nullptr,
                .old_attr_pos = nullptr,
                .cont = Content::kDelete_Vcell_LockCVT});
        item_ptr->is_delete_no_read_value = true;
        
        locked_rw_set.emplace_back(item_idx);
        return true;
      }

      // event_counter.RegEvent(t_id, txn_name, "LockReadValueRW:ReadNewest:Delete:ReadFVAttr");

      char* must_read_attrs_buf = thread_rdma_buffer_alloc->Alloc(attr_len);
      attr_pos->local_attr_buf = must_read_attrs_buf;

      auto doorbell = std::make_shared<DeleteLockRead>();
      doorbell->SetLockReq(lock_buff, item_ptr->GetRemoteLockAddr(), STATE_UNLOCKED, tx_id);
      doorbell->SetReadCVTReq(cvt_buff, item_ptr->header.remote_offset, CVTSize);  // Re-read the cvt
      doorbell->SetReadValueReq(fv_buff, item_ptr->header.remote_full_value_offset, fv_size);
      doorbell->SetReadAttrReq(must_read_attrs_buf,
                               fetched_cvt->vcell[read_pos].attri_so + fetched_cvt->header.remote_attribute_offset,
                               attr_len);
      doorbell->SendReqs(coro_sched, qp, coro_id);

      // CheckAddr(item_ptr->GetRemoteLockAddr(), 8, "LockReadValueRW:ReadNew:Delete:SetLockReq:ReadValue");
      // CheckAddr(item_ptr->header.remote_offset, CVTSize, "LockReadValueRW:ReadNew:Delete:SetReadCVTReq:ReadValue");
      // CheckAddr(item_ptr->header.remote_full_value_offset, fv_size, "LockReadValueRW:ReadNew:Delete:SetReadValueReq:ReadValue");
      // CheckAddr(fetched_cvt->vcell[read_pos].attri_so + fetched_cvt->header.remote_attribute_offset, attr_len, "LockReadValueRW:ReadNew:Delete:SetReadAttrReq:ReadValue");

      pending_value_read.emplace_back(
          ValueRead{
              .item = item_ptr,
              .value_buf = fv_buff,
              .lock_buf = lock_buff,
              .cvt_buf = cvt_buff,
              .attr_pos = attr_pos,
              .old_attr_pos = nullptr,
              .cont = Content::kDelete_Value_Attr_LockCVT});
    }
  } else {
    // 2) Lock cvt, re-read cvt, read full value, read rw attributes

    //                 +--> this version records the modifications of read pos's version
    //                 |
    // [read pos] [next pos]

    if (fetched_cvt->header.remote_attribute_offset <= 0) {
      event_counter.RegEvent(t_id, txn_name, "LockReadValueRW:ReadOld:remote_attribute_offset_not_set");
      return false;
    }

    if (item_ptr->user_op == kUpdate) {
      // event_counter.RegEvent(t_id, txn_name, "LockReadValueRW:NotReadNewest:Update");

      int next_pos;
      do {
        next_pos = (read_pos + 1) % MAX_VCELL_NUM;
      } while (!(fetched_cvt->vcell[next_pos].valid));

      std::vector<AttrRead> attr_read_list;

      AttrPos* attr_pos = new AttrPos();
      std::vector<OldAttrPos>* old_attr_pos = new std::vector<OldAttrPos>;

      CollectAttr(attr_read_list, attr_pos, old_attr_pos, table_id, fetched_cvt, next_pos, item_ptr);

      auto doorbell = std::make_shared<LockReadThreeBatch>(attr_read_list.size());
      doorbell->SetLockReq(lock_buff, item_ptr->GetRemoteLockAddr(), STATE_UNLOCKED, tx_id);
      doorbell->SetReadCVTReq(cvt_buff, item_ptr->header.remote_offset, CVTSize);  // Re-read the cvt
      doorbell->SetReadValueReq(fv_buff, item_ptr->header.remote_full_value_offset, fv_size);
      doorbell->SetReadAttrReq(attr_read_list);
      doorbell->SendReqs(coro_sched, qp, coro_id);

      // CheckAddr(item_ptr->GetRemoteLockAddr(), 8, "LockReadValueRW:ReadOldFV:Update:SetLockReq");
      // CheckAddr(item_ptr->header.remote_offset, CVTSize, "LockReadValueRW:ReadOldFV:Update:SetReadCVTReq");
      // CheckAddr(item_ptr->header.remote_full_value_offset, fv_size, "LockReadValueRW:ReadOldFV:Update:SetReadValueReq");
      // for (int i = 0; i < attr_read_list.size(); i++) {
      //   const std::string desc = "LockReadValueRW:ReadOldFV:Update:attr_read_list:" + std::to_string(i);
      //   CheckAddr(attr_read_list[i].remote_attr_off, attr_read_list[i].attr_size, desc);
      // }

      pending_value_read.emplace_back(
          ValueRead{
              .item = item_ptr,
              .value_buf = fv_buff,
              .lock_buf = lock_buff,
              .cvt_buf = cvt_buff,
              .attr_pos = attr_pos,
              .old_attr_pos = old_attr_pos,
              .cont = Content::kValue_Attr_LockCVT});

    } else if (item_ptr->user_op == UserOP::kDelete) {
      // event_counter.RegEvent(t_id, txn_name, "LockReadValueRW:NotReadNewest:Delete");
      item_ptr->is_delete_newest = false;

      AttrPos* attr_pos = new AttrPos();

      // We should read read_pos's modifications, and these modifications are not modified by newer versions.
      auto attr_len = CollectDeleteMiddleAttr(attr_pos, fetched_cvt, read_pos, table_id);

      if (attr_len == 0) {
        // event_counter.RegEvent(t_id, txn_name, "LockReadValueRW:ReadNewest:Delete:Vcell_LockedCVT");
        // Delete the init loaded fv
        auto doorbell = std::make_shared<DeleteLock>();
        doorbell->SetLockReq(lock_buff, item_ptr->GetRemoteLockAddr(), STATE_UNLOCKED, tx_id);
        doorbell->SetReadCVTReq(cvt_buff, item_ptr->header.remote_offset, CVTSize);  // Re-read the cvt
        doorbell->SendReqs(coro_sched, qp, coro_id);

        // CheckAddr(item_ptr->GetRemoteLockAddr(), 8, "LockReadValueRW:ReadOld:Delete:SetLockReq:NoReadValue");
        // CheckAddr(item_ptr->header.remote_offset, CVTSize, "LockReadValueRW:ReadOld:Delete:SetReadCVTReq:NoReadValue");

        pending_value_read.emplace_back(
            ValueRead{
                .item = item_ptr,
                .value_buf = nullptr,
                .lock_buf = lock_buff,
                .cvt_buf = cvt_buff,
                .attr_pos = nullptr,
                .old_attr_pos = nullptr,
                .cont = Content::kDelete_Vcell_LockCVT});
        item_ptr->is_delete_no_read_value = true;

        locked_rw_set.emplace_back(item_idx);
        return true;
      }

      char* must_read_attrs_buf = thread_rdma_buffer_alloc->Alloc(attr_len);
      attr_pos->local_attr_buf = must_read_attrs_buf;

      auto doorbell = std::make_shared<DeleteLockRead>();
      doorbell->SetLockReq(lock_buff, item_ptr->GetRemoteLockAddr(), STATE_UNLOCKED, tx_id);
      doorbell->SetReadCVTReq(cvt_buff, item_ptr->header.remote_offset, CVTSize);  // Re-read the cvt
      doorbell->SetReadValueReq(fv_buff, item_ptr->header.remote_full_value_offset, fv_size);
      doorbell->SetReadAttrReq(must_read_attrs_buf,
                               fetched_cvt->vcell[read_pos].attri_so + fetched_cvt->header.remote_attribute_offset,
                               attr_len);
      doorbell->SendReqs(coro_sched, qp, coro_id);

      // CheckAddr(item_ptr->GetRemoteLockAddr(), 8, "LockReadValueRW:ReadOld:Delete:SetLockReq:ReadValue");
      // CheckAddr(item_ptr->header.remote_offset, CVTSize, "LockReadValueRW:ReadOld:Delete:SetReadCVTReq:ReadValue");
      // CheckAddr(item_ptr->header.remote_full_value_offset, fv_size, "LockReadValueRW:ReadOld:Delete:SetReadValueReq:ReadValue");
      // CheckAddr(fetched_cvt->vcell[read_pos].attri_so + fetched_cvt->header.remote_attribute_offset, attr_len, "LockReadValueRW:ReadOld:Delete:SetReadAttrReq:ReadValue");

      pending_value_read.emplace_back(
          ValueRead{
              .item = item_ptr,
              .value_buf = fv_buff,
              .lock_buf = lock_buff,
              .cvt_buf = cvt_buff,
              .attr_pos = attr_pos,
              .old_attr_pos = nullptr,
              .cont = Content::kDelete_Value_Attr_LockCVT});
    }
  }

  locked_rw_set.emplace_back(item_idx);

  return true;
}

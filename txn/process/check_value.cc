// Author: Ming Zhang
// Copyright (c) 2023

#include <bitset>

#include "process/txn.h"

// --------------- Checking cvts, values and attributes after reading them---------
bool TXN::CheckValueRO(std::vector<ValueRead>& pending_value_read) {
  for (auto& fetched_it : pending_value_read) {
    char* p = fetched_it.value_buf;

    anchor_t fetched_value_sa = *((anchor_t*)p);
    char* fetched_value = p + sizeof(anchor_t);
    size_t value_size = TABLE_VALUE_SIZE[fetched_it.item->header.table_id];
    p = p + sizeof(anchor_t) + value_size;
    anchor_t fetched_value_ea = *((anchor_t*)p);

    if (fetched_value_sa != fetched_value_ea) {
      event_counter.RegEvent(t_id, txn_name, "CheckValueRO:ValueAnchorMismatch");
      return false;
    }

    // For read-only txns, they do not validate, so they need to check the consistency
    // between the anchors in vcell and vpkg
    if (fetched_value_ea != fetched_it.item->latest_anchor) {
      event_counter.RegEvent(t_id, txn_name, "CheckValueRO:ValueVcellAnchorMismatch");
      return false;
    }

    fetched_it.item->valuepkg.sa = fetched_value_sa;
    fetched_it.item->valuepkg.ea = fetched_value_ea;

    switch (fetched_it.cont) {
      case Content::kValue: {
        // Case 1: only read values
        memcpy((char*)&(fetched_it.item->valuepkg.value), fetched_value, value_size);
        break;
      }
      case Content::kValue_Attr: {
        // Case 2: read values and attributes
        CopyValueAndAttr(fetched_it.item, fetched_value, fetched_it.attr_pos, fetched_it.old_attr_pos, value_size);
        break;
      }
      default: {
        RDMA_LOG(FATAL) << "Error content to check";
      }
    }
  }

  return true;
}

bool TXN::CheckValueRW(std::vector<ValueRead>& pending_value_read,
                       std::vector<LockReadCVT>& pending_cvt_insert) {
  for (auto& fetched_it : pending_value_read) {
    // Not read full value (only for delete)
    if (fetched_it.cont == Content::kDelete_Vcell) {
      continue;
    }

    if (fetched_it.cont == Content::kDelete_Vcell_LockCVT) {
      if (*((lock_t*)fetched_it.lock_buf) != STATE_UNLOCKED) {
        event_counter.RegEvent(t_id, txn_name, "CheckValueRW:kDelete_Vcell_LockCVT:CVTLocked");
        return false;
      }

      if (!ObtainWritePos((CVT*)(fetched_it.cvt_buf), fetched_it.item)) {
        return false;
      }

      continue;
    }

    if (fetched_it.cont == Content::kDelete_AllInvalid_LockCVT) {
      // During cvt-read phase, I dont find a valid version to delete
      // In this value-read phase, I check the pos again
      if (*((lock_t*)fetched_it.lock_buf) != STATE_UNLOCKED) {
        event_counter.RegEvent(t_id, txn_name, "CheckValueRW:kDelete_AllInvalid_LockCVT:CVTLocked");
        return false;
      }

      bool is_all_invalid = true;

      auto new_read_pos = ReCheckReadPosForDelete((CVT*)(fetched_it.cvt_buf), start_time, is_all_invalid);

      if (is_all_invalid) {
        continue;
      } else if (new_read_pos != NO_POS) {
        // regain the deleted position
        fetched_it.item->is_delete_all_invalid = false;
        fetched_it.item->is_delete_no_read_value = true;
        fetched_it.item->target_write_pos = new_read_pos;
      } else if (new_read_pos == NO_POS) {
        event_counter.RegEvent(t_id, txn_name, "CheckValueRW:kDelete_AllInvalid_LockCVT:NewVersionOccurs");
        return false;
      }
    }

    // read full value
    char* p = fetched_it.value_buf;

    anchor_t fetched_value_sa = *((anchor_t*)p);
    char* fetched_value = p + sizeof(anchor_t);
    size_t value_size = TABLE_VALUE_SIZE[fetched_it.item->header.table_id];
    p = p + sizeof(anchor_t) + value_size;
    anchor_t fetched_value_ea = *((anchor_t*)p);

    if (fetched_value_sa != fetched_value_ea) {
      event_counter.RegEvent(t_id, txn_name, "CheckValueRW:ValueAnchorMismatch");
      return false;
    }

    // For read-write txns, they will re-read the CVT to check version for any
    // early abort, so they do not need to check the anchors in vcell and vpkg

    if (fetched_value_ea != fetched_it.item->latest_anchor) {
      event_counter.RegEvent(t_id, txn_name, "CheckValueRW:ValueVcellAnchorMismatch");
      // TLOG(INFO, t_id) << "fetched_value_ea: " << (int)fetched_value_ea << " latest_anchor: " << (int)fetched_it.item->latest_anchor << " table id: " << fetched_it.item->header.table_id << " key: " << fetched_it.item->header.key;
      return false;
    }

    fetched_it.item->valuepkg.sa = fetched_value_sa;
    fetched_it.item->valuepkg.ea = fetched_value_ea;

    switch (fetched_it.cont) {
      case Content::kValue: {
        // Case 1: only read values
        memcpy((char*)&(fetched_it.item->valuepkg.value), fetched_value, value_size);
        break;
      }
      case Content::kValue_Attr: {
        // Case 2: read values and attributes
        CopyValueAndAttr(fetched_it.item, fetched_value, fetched_it.attr_pos, fetched_it.old_attr_pos, value_size);
        break;
      }
      case Content::kValue_LockCVT: {
        // Case 3: lock CVT, read CVT, and read value
        if (*((lock_t*)fetched_it.lock_buf) != STATE_UNLOCKED) {
          event_counter.RegEvent(t_id, txn_name, "CheckValueRW:kValue_LockCVT:CVTLocked");
          return false;
        }

        if (!ObtainWritePos((CVT*)(fetched_it.cvt_buf), fetched_it.item)) {
          return false;
        }

        // Copy value
        memcpy((char*)&(fetched_it.item->valuepkg.value), fetched_value, value_size);
        break;
      }
      case Content::kValue_Attr_LockCVT: {
        // Case 4: lock CVT, read CVT, read value, read attr
        if (*((lock_t*)fetched_it.lock_buf) != STATE_UNLOCKED) {
          event_counter.RegEvent(t_id, txn_name, "CheckValueRW:kValue_Attr_LockCVT:CVTLocked");
          return false;
        }

        if (!ObtainWritePos((CVT*)(fetched_it.cvt_buf), fetched_it.item)) {
          return false;
        }

        CopyValueAndAttr(fetched_it.item, fetched_value, fetched_it.attr_pos, fetched_it.old_attr_pos, value_size);
        break;
      }
      case Content::kDelete_Value_Attr: {
        // Case 5: read value, read one attr
        CopyValueAndAttr(fetched_it.item, fetched_value, fetched_it.attr_pos, fetched_it.old_attr_pos, value_size);
        break;
      }
      case Content::kDelete_Value_Attr_LockCVT: {
        // Case 6: lock cvt, read cvt, read value, read one attr
        if (*((lock_t*)fetched_it.lock_buf) != STATE_UNLOCKED) {
          event_counter.RegEvent(t_id, txn_name, "CheckValueRW:kDelete_Value_Attr_LockCVT:CVTLocked");
          return false;
        }

        if (!ObtainWritePos((CVT*)(fetched_it.cvt_buf), fetched_it.item)) {
          return false;
        }

        CopyValueAndAttr(fetched_it.item, fetched_value, fetched_it.attr_pos, fetched_it.old_attr_pos, value_size);
        break;
      }
    }
  }

  for (auto& fetched_it : pending_cvt_insert) {
    // For insertions, the coordinator does not need to read values in Execution phase
    if (*((lock_t*)fetched_it.lock_buf) != STATE_UNLOCKED) {
      event_counter.RegEvent(t_id, txn_name, "CheckValueRW:Insert:CVTLocked");
      return false;
    }

    CVT* re_read_cvt = (CVT*)fetched_it.cvt_buf;

    if (fetched_it.item->is_insert_all_invalid) {
      // Double check whether the fetched cvt is still empty
      // bool is_all_invalid = true;
      // auto pos = ReGetInsertPos(re_read_cvt, start_time, is_all_invalid);
      // if (is_all_invalid) {
      //   fetched_it.item->target_write_pos = 0;
      // } else if (pos == NO_POS) {
      //   event_counter.RegEvent(t_id, txn_name, "CheckValueRW:Insert:ReGetInsertPosFail");
      //   return false;
      // } else if (pos != NO_POS) {
      //   fetched_it.item->target_write_pos = pos;
      // }

      if (!IsAllInvalid(re_read_cvt)) {
        event_counter.RegEvent(t_id, txn_name, "CheckValueRW:Insert:SlotBecomeValid");
        return false;
      }
    } else {
      if (re_read_cvt->header.value_size) {
        // I and another coordinator (C0) follow the same agreement to occupy the first empty slot.
        // Now I pass the locking check but I find that the re-read cvt has been occupied.
        // That's to say, C0 has finished lock+write+unlock before I successfuly lock the remote slot.
        // Due to the agreement, I have to abort since I cannot overwrite C0's slot.
        // This is a rare case in practice though.
        event_counter.RegEvent(t_id, txn_name, "CheckValueRW:Insert:SlotOccupied");
        return false;
      }
    }

    fetched_it.item->target_write_pos = 0;
  }

  return true;
}

void TXN::CopyValueAndAttr(DataSetItem* item,
                           char* fetched_value,
                           AttrPos* attr_pos,
                           std::vector<OldAttrPos>* old_attr_pos,
                           size_t value_size) {
  // copy old attributes
  if (old_attr_pos) {
    for (size_t i = 0; i < old_attr_pos->size(); i++) {
      memcpy(fetched_value + old_attr_pos->at(i).off_within_struct, old_attr_pos->at(i).local_attr_buf, old_attr_pos->at(i).len);
    }
  }

  char* p = attr_pos->local_attr_buf;
  for (size_t i = 0; i < attr_pos->offs_within_struct.size(); i++) {
    memcpy(fetched_value + attr_pos->offs_within_struct[i], p, attr_pos->lens[i]);
    p += attr_pos->lens[i];
  }

  memcpy((char*)&(item->valuepkg.value), fetched_value, value_size);
}

bool TXN::ObtainWritePos(CVT* re_read_cvt, DataSetItem* item) {
  int new_read_pos = NO_POS;
  int write_pos = NO_POS;
  int max_version_pos = 0;

  if (global_meta_man->iso_level == ISOLATION::SR) {
    // Below are for SR

    if (item->user_op == UserOP::kDelete) {
      // for delete, its target write pos already set in FindMatch
      new_read_pos = ReReadPos(re_read_cvt, start_time);
      if (new_read_pos == NO_POS) {
        event_counter.RegEvent(t_id, txn_name, "CheckValueRW:ObtainWritePos:ReReadPos:NoNewReadPosForDelete");
        return false;
      }
    } else {
      bool is_ea = false;
      write_pos = FindReadWritePos(re_read_cvt, new_read_pos, max_version_pos, is_ea);

      if (is_ea) {
        event_counter.RegEvent(t_id, txn_name, "CheckValueRW:ObtainWritePos:EarlyAbort");
        return false;
      }

      if (write_pos == NO_POS) {
        event_counter.RegEvent(t_id, txn_name, "CheckValueRW:ObtainWritePos:FindReadWritePos:NoWritePosForUpdate");
        return false;
      }
      if (new_read_pos == NO_POS) {
        event_counter.RegEvent(t_id, txn_name, "CheckValueRW:ObtainWritePos:FindReadWritePos:NoNewReadPosForUpdate");
        return false;
      }
      item->remote_so = re_read_cvt->vcell[max_version_pos].attri_so;
      item->remote_bmp = re_read_cvt->vcell[max_version_pos].attri_bitmap;
      item->target_write_pos = write_pos;
    }

    version_t my_old_version = item->vcell.version;
    if (re_read_cvt->vcell[new_read_pos].version != my_old_version) {
      // Another coordinator has updated a newer version
      // I should see it, but I have not, so I abort
      // Note that this is not needed in SI
      event_counter.RegEvent(t_id, txn_name, "CheckValueRW:ObtainWritePos:NewVersionOccurs");
      return false;
    }
  } else {
    // Below are for SI
    if (item->user_op != UserOP::kDelete) {
      write_pos = FindWritePos(re_read_cvt, max_version_pos);

      if (write_pos == NO_POS) {
        event_counter.RegEvent(t_id, txn_name, "[SI] CheckValueRW:ObtainWritePos:FindWritePos:NoWritePos");
        return false;
      }

      item->remote_so = re_read_cvt->vcell[max_version_pos].attri_so;
      item->remote_bmp = re_read_cvt->vcell[max_version_pos].attri_bitmap;
      item->target_write_pos = write_pos;
    }
  }

  return true;
}

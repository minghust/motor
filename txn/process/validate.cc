// Author: Ming Zhang
// Copyright (c) 2023

#include "process/txn.h"

void TXN::IssueValidate(std::vector<ValidateRead>& pending_validate) {
  // For read-only items, we only need to read their versions
  for (auto& set_it : read_only_set) {
    // If reading from backup, using backup's qp to validate the version on backup.
    // Otherwise, the qp mismatches the remote version addr
    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(set_it->read_which_node);
    char* cvt_buf = thread_rdma_buffer_alloc->Alloc(CVTSize);

    pending_validate.emplace_back(ValidateRead{.item = set_it.get(), .cvt_buf = cvt_buf});
    
    coro_sched->RDMARead(coro_id, qp, cvt_buf, set_it->header.remote_offset, CVTSize);
    // CheckAddr(set_it->header.remote_offset, CVTSize, "IssueValidate");
  }
}

// --------------- Details of processing validations -----------------
bool TXN::CheckValidate(std::vector<ValidateRead>& pending_validate) {
  // Requirements for all
  // For those eagerly locked R-W, we have choosen write_pos at read, since the lock succeeds at read
  // For those delayed locked R-W, we have choosen write_pos at CheckValue, since the lock succeeds at CheckValue

  // -> Requirements for SI
  // --- For R-O: Nothing to do
  if (global_meta_man->iso_level == ISOLATION::SI) {
    return true;
  }

  // -> Requirements for SR
  // --- For R-O: Whether the R-O is locked? Using Tcommit to identify version
  for (auto& re : pending_validate) {
    CVT* re_read_cvt = (CVT*)re.cvt_buf;
    if ((*(CVT*)(re.cvt_buf)).header.lock == STATE_LOCKED) {
      event_counter.RegEvent(t_id, txn_name, "CheckValidate:RO is Locked");
      return false;
    }
    int new_read_pos = ReReadPos(re_read_cvt, commit_time);
    if (new_read_pos == NO_POS) {
      event_counter.RegEvent(t_id, txn_name, "CheckValidate:No re-read pos for RO");
      return false;
    }

    version_t my_old_version = re.item->vcell.version;
    if (re_read_cvt->vcell[new_read_pos].version != my_old_version) {
      // Another coordinator has updated a newer version at my commit time
      // I should see it, but I have not, so I abort
      event_counter.RegEvent(t_id, txn_name, "CheckValidate:New RO version occurs");
      return false;
    }
  }
  return true;
}

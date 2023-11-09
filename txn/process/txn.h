// Author: Ming Zhang
// Copyright (c) 2023

#pragma once

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <list>
#include <queue>
#include <random>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "allocator/buffer_allocator.h"
#include "base/common.h"
#include "base/workload.h"
#include "cache/addr_cache.h"
#include "connection/meta_manager.h"
#include "connection/qp_manager.h"
#include "memstore/hash_store.h"
#include "process/doorbell.h"
#include "process/oplog.h"
#include "process/stat.h"
#include "process/structs.h"
#include "util/debug.h"
#include "util/hash.h"
#include "util/json_config.h"

extern std::atomic<uint64_t> tx_id_generator;

extern EventCount event_counter;
extern KeyCount key_counter;

extern uint64_t access_old_version_cnt[MAX_TNUM_PER_CN];
extern uint64_t access_new_version_cnt[MAX_TNUM_PER_CN];

/* One-sided RDMA-enabled distributed transaction processing */
class TXN {
 public:
  /************ Interfaces for applications ************/
  void Begin(tx_id_t txid, TXN_TYPE txn_t, const std::string& name = "default");

  void AddToReadOnlySet(DataSetItemPtr item);

  void AddToReadWriteSet(DataSetItemPtr item);

  bool Execute(coro_yield_t& yield, bool fail_abort = true);

  bool Commit(coro_yield_t& yield);

  // void CheckAddr(offset_t start, size_t len, const std::string desc) {
  //   if ((start < 0) ||
  //       (start + len > (global_meta_man->delta_start_off + global_meta_man->per_thread_delta_size * MAX_CLIENT_NUM_PER_MN))) {
  //     TLOG(ERROR, t_id) << "Error remote access addr. start: " << start << ". start+len: " << start + len << ". desc: " << desc;
  //   }
  // }

  /*****************************************************/

 public:
  void TxAbortReadWrite() { Abort(); }

  void RemoveLastROItem() { read_only_set.pop_back(); }

 public:
  TXN(MetaManager* meta_man,
      QPManager* qp_man,
      t_id_t tid,
      coro_id_t coroid,
      CoroutineScheduler* sched,
      LocalBufferAllocator* rdma_buffer_allocator,
      RemoteDeltaOffsetAllocator* delta_offset_allocator,
      LockedKeyTable* locked_key_table,
      AddrCache* addr_buf) {
    // Transaction setup
    tx_id = 0;
    t_id = tid;
    coro_id = coroid;
    coro_sched = sched;
    global_meta_man = meta_man;
    thread_qp_man = qp_man;
    thread_rdma_buffer_alloc = rdma_buffer_allocator;
    thread_delta_offset_alloc = delta_offset_allocator;
    thread_locked_key_table = locked_key_table;
    addr_cache = addr_buf;
    select_backup = 0;
  }

  ~TXN() {
    Clean();
  }

 private:
  // Internal transaction functions
  bool ExeRO(coro_yield_t& yield);  // Execute read-only transaction

  bool ExeRW(coro_yield_t& yield);  // Execute read-write transaction

  bool Validate(coro_yield_t& yield);  // RDMA read value versions

  void CommitAll();

  void WriteReplica(RCQP* qp,
                    const DataSetItem* item,
                    int write_pos,
                    uint8_t user_op,
                    bool new_attr_bar);

  void HandleDelete(RCQP* qp, const DataSetItem* item, int write_pos);

  void HandleUpdate(RCQP* qp,
                    const DataSetItem* item,
                    int write_pos,
                    bool new_attr_bar);

  void HandleInsert(RCQP* qp,
                    const DataSetItem* item,
                    int write_pos);

  void Abort();

  void RecoverPrimary(table_id_t table_id, PrimaryCrashTime p_crash_time = PrimaryCrashTime::kBeforeCommit);

  void RecoverBackup(table_id_t table_id, node_id_t to_recover_backup_node_id);

  void SendMsgToReplica(node_id_t copy_from, node_id_t copy_to, table_id_t table_id, int is_primary_fail);

  int FindReadPos(CVT* cvt, bool& is_read_newest, int& max_pos, bool& is_ea, bool& is_all_invalid);

  int ReReadPos(CVT* cvt, uint64_t current_time);

  int ReGetInsertPos(CVT* cvt, uint64_t current_time, bool& is_all_invalid);

  bool IsAllInvalid(CVT* cvt);

  int ReCheckReadPosForDelete(CVT* cvt, uint64_t current_time, bool& is_all_invalid);

  int FindCasReadPos(CVT* cvt, int& read_pos, bool& is_read_newest, int& max_pos, bool& is_ea);

  int FindReadWritePos(CVT* cvt, int& new_read_pos, int& max_pos, bool& is_ea);

  int FindWritePos(CVT* cvt, int& max_pos);

  void RecordLockKey(node_id_t n, offset_t o) {
#if HAVE_COORD_CRASH
    int& ne = thread_locked_key_table[coro_id].num_entry;
    thread_locked_key_table[coro_id].entries[ne].remote_node = n;
    thread_locked_key_table[coro_id].entries[ne].remote_off = o;
    ne += 1;
#endif
  }

  bool RDMAWriteRoundTrip(RCQP* qp, char* wt_data, uint64_t remote_offset, size_t size);  // RDMA write wrapper

  bool RDMAReadRoundTrip(RCQP* qp, char* rd_data, uint64_t remote_offset, size_t size);  // RDMA read wrapper

  void Clean();  // Clean data sets after commit/abort

 private:
  // For coroutine issues RDMA requests before yield
  bool IssueReadROCVT(std::vector<DirectRead>& pending_direct_ro,
                      std::vector<HashRead>& pending_hash_read);

  bool IssueReadLockCVT(std::vector<CasRead>& pending_cas_rw,
                        std::vector<HashRead>& pending_hash_read,
                        std::vector<InsertOffRead>& pending_insert_off_rw);

  bool ReadValueRO(RCQP* qp,
                   CVT* fetched_cvt,
                   DataSetItem* item_ptr,
                   int read_pos,
                   std::vector<ValueRead>& pending_value_read,
                   bool is_read_newest);

  bool ReadValueRW(RCQP* qp,
                   CVT* fetched_cvt,
                   DataSetItem* item_ptr,
                   int read_pos,
                   std::vector<ValueRead>& pending_value_read,
                   bool is_read_newest);

  void CollectAttr(std::vector<AttrRead>& attr_read_list,
                   AttrPos* attr_pos,
                   std::vector<OldAttrPos>* old_attr_pos,
                   table_id_t table_id,
                   CVT* cvt,
                   int read_pos,
                   DataSetItem* item_ptr);

  void SearchOldVCells(int attr_idx,
                       uint64_t mask,
                       std::vector<AttrRead>& attr_read_list,
                       std::vector<OldAttrPos>* old_attr_pos,
                       table_id_t table_id,
                       CVT* cvt,
                       int read_pos);

  size_t CollectDeleteNewestAttr(AttrPos* attr_pos,
                                 bitmap_t read_pos_bmp,
                                 table_id_t table_id);

  size_t CollectDeleteMiddleAttr(AttrPos* attr_pos,
                                 CVT* cvt,
                                 int read_pos,
                                 table_id_t table_id);

  bool IsFurtherModified(uint64_t mask, CVT* cvt, int read_pos);

  void IssueValidate(std::vector<ValidateRead>& pending_validate);

  bool ObtainWritePos(CVT* re_read_cvt, DataSetItem* item);

  void CopyValueAndAttr(DataSetItem* item,
                        char* fetched_value,
                        AttrPos* attr_pos,
                        std::vector<OldAttrPos>* old_attr_pos,
                        size_t value_size);

  bool CheckValueRO(std::vector<ValueRead>& pending_value_read);

  bool CheckValueRW(std::vector<ValueRead>& pending_value_read,
                    std::vector<LockReadCVT>& pending_cvt_insert);

  bool CheckDirectROCVT(std::vector<DirectRead>& pending_direct_ro,
                        std::vector<ValueRead>& pending_value_read);

  bool CheckCasReadCVT(std::vector<CasRead>& pending_cas_rw,
                       std::vector<ValueRead>& pending_value_read);

  bool CheckHashReadCVT(std::vector<HashRead>& pending_hash_read,
                        std::vector<ValueRead>& pending_value_read);

  int FindMatch(HashRead& res,
                int& read_pos,
                bool& is_read_newest);

  bool CheckInsertCVT(std::vector<InsertOffRead>& pending_insert_off_rw,
                      std::vector<LockReadCVT>& pending_cvt_insert,
                      std::vector<ValueRead>& pending_value_read);

  int FindInsertOff(InsertOffRead& res,
                    int& read_pos,
                    bool& is_read_newest);

  bool LockReadValueRW(RCQP* qp,
                       node_id_t remote_node,
                       CVT* fetched_cvt,
                       DataSetItem* item_ptr,
                       int read_pos,
                       std::vector<ValueRead>& pending_value_read,
                       int item_idx,
                       bool is_read_newest);

  bool CheckValidate(std::vector<ValidateRead>& pending_validate);

 public:
  tx_id_t tx_id;  // Transaction ID

  tx_id_t start_time;  // Sequencial number as time

  tx_id_t commit_time;  // Sequencial number as time

  t_id_t t_id;  // Thread ID

  coro_id_t coro_id;  // Coroutine ID

  MetaManager* global_meta_man;  // Global metadata manager

 private:
  CoroutineScheduler* coro_sched;  // Thread local coroutine scheduler

  QPManager* thread_qp_man;  // Thread local qp connection manager. Each transaction thread has one

  // Thread local RDMA buffer allocator
  LocalBufferAllocator* thread_rdma_buffer_alloc;

  // Thread local remote delta address assigner
  RemoteDeltaOffsetAllocator* thread_delta_offset_alloc;

  LockedKeyTable* thread_locked_key_table;

  std::vector<DataSetItemPtr> read_only_set;

  std::vector<DataSetItemPtr> read_write_set;

  std::vector<size_t> locked_rw_set;  // For release lock during abort

  AddrCache* addr_cache;

  // For backup-enabled read. Which backup is selected (the backup index, not the backup's machine id)
  size_t select_backup;

  struct pair_hash {
    inline std::size_t operator()(const std::pair<node_id_t, offset_t>& v) const {
      return v.first * 31 + v.second;
    }
  };

  // Avoid inserting to the same slot in one transaction
  std::unordered_set<std::pair<node_id_t, offset_t>, pair_hash> inserted_pos;

  TXN_TYPE txn_type;

  std::string txn_name;
};

/*************************************************************
 *********** Implementations of simple interfaces in TXN ******
 **************************************************************/

ALWAYS_INLINE
void TXN::Begin(tx_id_t txid, TXN_TYPE txn_t, const std::string& name) {
  Clean();  // Clean the last transaction states
  tx_id = txid;
  start_time = txid;
  txn_type = txn_t;
  txn_name = name;

  thread_locked_key_table[coro_id].num_entry = 0;
  thread_locked_key_table[coro_id].tx_id = txid;
}

ALWAYS_INLINE
void TXN::AddToReadOnlySet(DataSetItemPtr item) {
#if OUTPUT_KEY_STAT
  key_counter.RegKey(t_id, KeyType::kKeyRead, txn_name, item->header.table_id, item->header.key);
#endif

  read_only_set.emplace_back(item);
}

ALWAYS_INLINE
void TXN::AddToReadWriteSet(DataSetItemPtr item) {
#if OUTPUT_KEY_STAT
  key_counter.RegKey(t_id, KeyType::kKeyWrite, txn_name, item->header.table_id, item->header.key);
#endif
  read_write_set.emplace_back(item);
}

ALWAYS_INLINE
int TXN::FindReadPos(CVT* cvt, bool& is_read_newest, int& max_pos, bool& is_ea, bool& is_all_invalid) {
  int target_idx = NO_POS;

  int64_t max_ts = -1;

  int64_t target_ts = -1;

  for (int i = 0; i < MAX_VCELL_NUM; i++) {
    if (!cvt->vcell[i].valid) continue;

    is_all_invalid = false;

    int64_t ts = (int64_t)cvt->vcell[i].version;

    if (ts > start_time) {
      is_read_newest = false;

#if EARLY_ABORT
      if ((global_meta_man->iso_level == ISOLATION::SR) &&
          (txn_type == TXN_TYPE::kRWTxn)) {
        // In SR and rw txn, if reading a version larger than start time, I can early abort,
        // since in Validation I must abort by acquiring a more larger Tcommit
        event_counter.RegEvent(t_id, txn_name, "FindReadPos:EarlyAbort");
        is_ea = true;
        // access_old_version_cnt[t_id]++;
        return NO_POS;
      }
#endif
    }

    if (ts > max_ts) {
      max_ts = ts;
      max_pos = i;
    }

    if ((ts <= start_time) && (ts > target_ts)) {
      target_ts = ts;
      target_idx = i;
    }
  }

  // if (is_read_newest) {
  //   access_new_version_cnt[t_id]++;
  // } else {
  //   access_old_version_cnt[t_id]++;
  // }

  return target_idx;
}

// ALWAYS_INLINE
// int TXN::ReGetInsertPos(CVT* cvt, uint64_t current_time, bool& is_all_invalid) {
//   int empty_idx = NO_POS;
//   bool find_empty = false;

//   int min_pos = 0;
//   int64_t min_ts = cvt->vcell[0].version;

//   for (int i = 0; i < MAX_VCELL_NUM; i++) {
//     if (!cvt->vcell[i].valid) {
//       if (!find_empty) {
//         find_empty = true;
//         empty_idx = i;
//       }
//       continue;
//     }

//     is_all_invalid = false;

//     int64_t ts = (int64_t)cvt->vcell[i].version;

//     if (ts < min_ts) {
//       min_ts = ts;
//       min_pos = i;
//     }

//     if (ts > start_time && global_meta_man->iso_level == ISOLATION::SR) {
//       return NO_POS;
//     }
//   }

//   if (empty_idx != NO_POS) {
//     return empty_idx;
//   }

//   // Triggering coordinator-active GC
//   if (start_time >= cvt->vcell[min_pos].version) {
//     return min_pos;
//   }

//   return NO_POS;
// }

ALWAYS_INLINE
bool TXN::IsAllInvalid(CVT* cvt) {
  for (int i = 0; i < MAX_VCELL_NUM; i++) {
    if (!cvt->vcell[i].valid) continue;
    return false;
  }

  return true;
}

ALWAYS_INLINE
int TXN::ReCheckReadPosForDelete(CVT* cvt, uint64_t current_time, bool& is_all_invalid) {
  int target_idx = NO_POS;

  int64_t target_ts = -1;

  for (int i = 0; i < MAX_VCELL_NUM; i++) {
    if (!cvt->vcell[i].valid) continue;

    is_all_invalid = false;

    int64_t ts = (int64_t)cvt->vcell[i].version;
    if ((ts <= current_time) && (ts > target_ts)) {
      target_ts = ts;
      target_idx = i;
    }
  }

  return target_idx;
}

ALWAYS_INLINE
int TXN::ReReadPos(CVT* cvt, uint64_t current_time) {
  int target_idx = NO_POS;

  int64_t target_ts = -1;

  for (int i = 0; i < MAX_VCELL_NUM; i++) {
    if (!cvt->vcell[i].valid) continue;

    int64_t ts = (int64_t)cvt->vcell[i].version;
    if ((ts <= current_time) && (ts > target_ts)) {
      target_ts = ts;
      target_idx = i;
    }
  }
  return target_idx;
}

ALWAYS_INLINE
int TXN::FindCasReadPos(CVT* cvt,
                        int& read_pos,
                        bool& is_read_newest,
                        int& max_pos,
                        bool& is_ea) {
  read_pos = NO_POS;

  int min_pos = 0;
  int64_t min_ts = cvt->vcell[0].version;
  int64_t max_ts = -1;

  int empty_idx = NO_POS;
  bool find_empty = false;

  int64_t target_ts = -1;

  for (int i = 0; i < MAX_VCELL_NUM; i++) {
    int64_t ts = (int64_t)cvt->vcell[i].version;

    if (ts > start_time && cvt->vcell[i].valid) {
      is_read_newest = false;

#if EARLY_ABORT
      if ((global_meta_man->iso_level == ISOLATION::SR) && (txn_type == TXN_TYPE::kRWTxn)) {
        // In SR and rw txn, if reading a version larger than start time, I can early abort,
        // since in Validation I must abort due to acquiring a larger Tcommit
        is_ea = true;
        // access_old_version_cnt[t_id]++;
        return NO_POS;
      }
#endif
    }

    if (!find_empty && !cvt->vcell[i].valid) {
      empty_idx = i;
      find_empty = true;
    }

    // even if this min_ts is invalid, it is OK because we will first
    // use the invalid vcell to write, and then use the min_pos, in the
    // latter case, the min_ts must be valid
    if (ts < min_ts) {
      min_ts = ts;
      min_pos = i;
    }

    if ((ts > max_ts) && cvt->vcell[i].valid) {
      max_ts = ts;
      max_pos = i;
    }

    if ((cvt->vcell[i].valid) && (ts <= start_time) && (ts > target_ts)) {
      target_ts = ts;
      read_pos = i;
    }
  }

  // if (is_read_newest) {
  //   access_new_version_cnt[t_id]++;
  // } else {
  //   access_old_version_cnt[t_id]++;
  // }

  // Return write position

  // Write to an empty slot
  if (empty_idx != NO_POS) {
    return empty_idx;
  }

  // Triggering coordinator-active GC
  if (start_time >= cvt->vcell[min_pos].version) {
    return min_pos;
  }

  return NO_POS;
}

ALWAYS_INLINE
int TXN::FindReadWritePos(CVT* cvt, int& new_read_pos, int& max_pos, bool& is_ea) {
  new_read_pos = NO_POS;

  int min_pos = 0;
  int64_t min_ts = cvt->vcell[0].version;
  int64_t max_ts = -1;

  int empty_idx = NO_POS;
  bool find_empty = false;
  is_ea = false;

  int64_t target_ts = -1;

  for (int i = 0; i < MAX_VCELL_NUM; i++) {
    int64_t ts = (int64_t)cvt->vcell[i].version;

    if (!find_empty && !cvt->vcell[i].valid) {
      empty_idx = i;
      find_empty = true;
    }

    // even if this min_ts is invalid, it is OK because we will first
    // use the invalid vcell to write, and then use the min_pos, in the
    // latter case, the min_ts must be valid
    if (ts < min_ts) {
      min_ts = ts;
      min_pos = i;
    }

    if ((ts > max_ts) && cvt->vcell[i].valid) {
      max_ts = ts;
      max_pos = i;
    }

    if (!is_ea && (cvt->vcell[i].valid) && (ts <= start_time) && (ts > target_ts)) {
      target_ts = ts;
      new_read_pos = i;
    }

    if (!is_ea && (cvt->vcell[i].valid) && (ts > start_time)) {
#if EARLY_ABORT
      if ((global_meta_man->iso_level == ISOLATION::SR) && (txn_type == TXN_TYPE::kRWTxn)) {
        // In SR and rw txn, if reading a version larger than start time, I can early abort,
        // since in Validation I must abort due to acquiring a larger Tcommit
        new_read_pos = NO_POS;
        is_ea = true;
      }
#endif
    }
  }

  // Write to an empty slot
  if (empty_idx != NO_POS) {
    return empty_idx;
  }

  // Triggering coordinator-active GC
  if (start_time >= cvt->vcell[min_pos].version) {
    // RDMA_LOG(DBG) << "Trigger GC. Txn " << tx_id;
    return min_pos;
  }

  return NO_POS;
}

ALWAYS_INLINE
int TXN::FindWritePos(CVT* cvt, int& max_pos) {
  int min_pos = 0;
  int64_t min_ts = cvt->vcell[0].version;
  int64_t max_ts = -1;

  int empty_idx = NO_POS;
  bool find_empty = false;

  int64_t target_ts = -1;

  for (int i = 0; i < MAX_VCELL_NUM; i++) {
    int64_t ts = (int64_t)cvt->vcell[i].version;

    if (!find_empty && !cvt->vcell[i].valid) {
      empty_idx = i;
      find_empty = true;
    }

    // even if this min_ts is invalid, it is OK because we will first
    // use the invalid vcell to write, and then use the min_pos, in the
    // latter case, the min_ts must be valid
    if (ts < min_ts) {
      min_ts = ts;
      min_pos = i;
    }

    if ((ts > max_ts) && cvt->vcell[i].valid) {
      max_ts = ts;
      max_pos = i;
    }
  }

  // Write to an empty slot
  if (empty_idx != NO_POS) {
    return empty_idx;
  }

  // Triggering coordinator-active GC
  if (start_time >= cvt->vcell[min_pos].version) {
    // RDMA_LOG(DBG) << "Trigger GC. Txn " << tx_id;
    return min_pos;
  }

  return NO_POS;
}

// RDMA write `wt_data' with size `size' to remote
ALWAYS_INLINE
bool TXN::RDMAWriteRoundTrip(RCQP* qp, char* wt_data,
                             uint64_t remote_offset,
                             size_t size) {
  auto rc = qp->post_send(IBV_WR_RDMA_WRITE, wt_data, size, remote_offset, 0);
  if (rc != SUCC) {
    return false;
  }
  // wait finish
  // *** For Debug Only ***
  sleep(1);
  return true;
}

// RDMA read value with size `size` from remote mr at offset `remote_offset` to
// rd_data
ALWAYS_INLINE
bool TXN::RDMAReadRoundTrip(RCQP* qp, char* rd_data,
                            uint64_t remote_offset, size_t size) {
  auto rc = qp->post_send(IBV_WR_RDMA_READ, rd_data, size, remote_offset, 0);
  if (rc != SUCC) {
    return false;
  }
  // wait finish
  // *** For Debug Only ***
  sleep(1);
  return true;
}

ALWAYS_INLINE
void TXN::Clean() {
  read_only_set.clear();
  read_write_set.clear();
  locked_rw_set.clear();
  inserted_pos.clear();
}

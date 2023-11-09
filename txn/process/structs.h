// Author: Ming Zhang
// Copyright (c) 2023

#pragma once

#include "memstore/hash_store.h"
#include "rlib/rdma_ctrl.hpp"

enum TXN_SYS : int {
  FaRMv2 = 1,
  Motor = 2,
};

enum ISOLATION : int {
  SI = 1,  // snapshot isolation
  SR = 2,  // serializability
};

enum VersionStructure : int {
  N2O = 1,  // New to old chain
  O2N = 2,  // Old to new chain
};

enum TXN_TYPE : int {
  kRWTxn = 1,
  kROTxn,
};

// Following are stuctures for maintaining coroutine's state in analogy with context switch

struct LockAddr {
  node_id_t node_id;
  uint64_t lock_addr;
};

// For coroutines
struct DirectRead {
  RCQP* qp;
  DataSetItem* item;
  char* buf;
  node_id_t remote_node;
  bool is_ro;  // is read-only or read-write
};

struct HashRead {
  RCQP* qp;
  DataSetItem* item;
  char* buf;
  node_id_t remote_node;
  int item_idx;
  bool is_ro;  // is read-only or read-write
};

struct AttrPos {
  char* local_attr_buf;
  std::vector<offset_t> offs_within_struct;
  std::vector<offset_t> lens;
};

struct OldAttrPos {
  char* local_attr_buf;
  offset_t off_within_struct;
  size_t len;
};

enum Content : int {
  kValue = 1,                  // read value
  kValue_Attr,                 // read value, read attr
  kValue_LockCVT,              // lock cvt, read cvt, read value
  kValue_Attr_LockCVT,         // lock cvt, read cvt, read value, read attr
  kDelete_Value_Attr,          // read value, read attr, for delete to recover old attr to full value
  kDelete_Value_Attr_LockCVT,  // lock cvt, read cvt, read value, read attr, for delete to recover old attr to full value
  kDelete_Vcell,               // delete the only one newest vcell in cvt. no need to recover the full value
  kDelete_Vcell_LockCVT,       // lock cvt, read cvt, delete the only one newest vcell in cvt. no need to recover the full value
  kDelete_AllInvalid_LockCVT   // lock cvt, read cvt. No valid version can be deleted
};

struct ValueRead {
  DataSetItem* item;
  char* value_buf;  // variable length for different tables
  char* lock_buf;
  char* cvt_buf;
  AttrPos* attr_pos;
  std::vector<OldAttrPos>* old_attr_pos;
  Content cont;
};

struct LockReadCVT {
  DataSetItem* item;
  char* lock_buf;
  char* cvt_buf;
};

struct AttrRead {
  char* local_attr_buf;
  offset_t remote_attr_off;
  size_t attr_size;
};

struct ValueRecord {
  RCQP* qp;
  DataSetItem* item;
  Value* recv_value;  // Point to the receiver's space
  offset_t remote_off;
};

struct ChainWalk {
  RCQP* qp;
  int walking_steps;
  uint64_t off;  // Offset of the vcell
  bool must_abort;
};

struct CasRead {
  RCQP* qp;
  DataSetItem* item;
  char* cas_buf;
  char* cvt_buf;
  node_id_t primary_node_id;
};

struct InsertOffRead {
  RCQP* qp;
  DataSetItem* item;
  char* buf;
  node_id_t remote_node;
  int item_idx;
  offset_t bucket_off;
};

struct ValidateRead {
  DataSetItem* item;
  char* cvt_buf;
};

struct Lock {
  RCQP* qp;
  DataSetItem* item;
  char* cas_buf;
  uint64_t lock_off;
};

struct Unlock {
  char* cas_buf;
};

struct Version {
  RCQP* qp;
  DataSetItem* item;
  char* cvt_buf;
  bool is_rw;
  node_id_t remote_node;
};

struct FindRes {
  int sorted_read_pos;
  int original_read_pos;
  int punish_step;

  FindRes() {
    sorted_read_pos = NO_POS;
    original_read_pos = NO_POS;
    punish_step = NO_WALK;
  }
};

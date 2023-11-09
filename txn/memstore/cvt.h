// Author: Ming Zhang
// Copyright (c) 2023

#pragma once

#include <cassert>
#include <cstring>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>

#include "base/common.h"
#include "base/workload.h"
#include "util/debug.h"

struct Header {
  table_id_t table_id;
  lock_t lock;
  itemkey_t key;
  offset_t remote_offset;             // remote offset of the CVT
  offset_t remote_full_value_offset;  // Remote offset of the full value
  offset_t remote_attribute_offset;   // Remote offset of the attribute bar
  size_t value_size;                  // The length of value of each object
  bool user_inserted;
} Aligned8;
constexpr size_t HeaderSize = sizeof(Header);
using HeaderPtr = std::shared_ptr<Header>;

struct VCell {
  anchor_t sa;            // Start anchor of this vcell
  valid_t valid;          // deleted?
  version_t version;      // Timestamp
  in_offset_t attri_so;   // [Fetched] Start offset in the attribute bar
  bitmap_t attri_bitmap;  // [Fetched] Modified attributes represented in bitmap
  anchor_t ea;            // End anchor of this vcell, should be consisted with the anchors in vcell and value
  bool IsWritten() {
    return sa != ea;
  }
} Aligned8;
constexpr size_t VCellSize = sizeof(VCell);
using VCellPtr = std::shared_ptr<VCell>;

struct Value {
  anchor_t sa;                    // Start anchor
  uint8_t value[MAX_VALUE_SIZE];  // A max buffer to receive remote full value
  anchor_t ea;                    // End anchor
  bool IsWritten() {
    return sa != ea;
  }
} __attribute__((packed));
constexpr size_t ValueSize = sizeof(Value);
using ValuePtr = std::shared_ptr<Value>;

// Consecutive Version Tuple
struct CVT {
  Header header;
  VCell vcell[MAX_VCELL_NUM];

  ALWAYS_INLINE
  void Debug() const {
    // For debug usage
    std::cerr << "[CVT debug] (meta) table id: " << this->header.table_id << ", value size: " << this->header.value_size << ", key: " << this->header.key << ", remote offset: " << this->header.remote_offset << ", lock: " << this->header.lock << std::endl;
  }
} Aligned8;
constexpr size_t CVTSize = sizeof(CVT);
using CVTPtr = std::shared_ptr<CVT>;

enum UserOP : uint8_t {
  kRead = 0,
  kUpdate,
  kInsert,
  kDelete
};

// Used for RO and RW sets
struct DataSetItem {
  struct Header header;
  struct VCell vcell;  // Fetched remote target vcell will be copied here
  struct Value valuepkg;
  char* fetched_cvt_ptr;

  bool is_fetched;
  int target_write_pos;          // Which position in the cvt I should update/insert/delete a new one
  node_id_t read_which_node;     // From which node this cvt is read. This is a node id, e.g., 0, 1, 2...
  UserOP user_op;                // Read, Update, Insert, Delete
  bool is_delete_newest;         // Whether delete the newest version
  bool is_delete_no_read_value;  // Whether no read value in delete
  bool is_delete_all_invalid;    // Whether no valid version can be deleted
  bool is_insert_all_invalid;    // Whether insert into an all-invalid-cvt
  int insert_slot_idx;           // Insert into which slot. Useful in write replica for calculating remote value offset

  bitmap_t update_bitmap;
  uint8_t* old_value_ptr;
  int current_p;  // current update position

  in_offset_t remote_so;
  bitmap_t remote_bmp;

  anchor_t latest_anchor;  // store the latest anchor value for comparison

  DataSetItem(table_id_t _table_id, size_t _size, itemkey_t _key, UserOP op) {
    memset((char*)this, 0, sizeof(DataSetItem));
    header.table_id = _table_id;
    header.key = _key;
    header.value_size = _size;

    is_fetched = false;
    target_write_pos = UN_INIT_POS;
    read_which_node = -1;
    user_op = op;
    is_delete_newest = true;
    is_delete_no_read_value = false;
    is_delete_all_invalid = false;
    is_insert_all_invalid = false;
    insert_slot_idx = -1;

    update_bitmap = 0;
    old_value_ptr = new uint8_t[TABLE_VALUE_SIZE[_table_id]];
    current_p = 0;

    remote_so = 0;
    remote_bmp = 0;

    latest_anchor = 0;
  }

  ~DataSetItem() {
    delete[] old_value_ptr;
  }

  void SetUpdate(int bit_pos, void* old_value, size_t len) {
    update_bitmap |= 1UL << bit_pos;  // set the corresponding bit
    memcpy(old_value_ptr + current_p, old_value, len);
    current_p += len;
  }

  uint8_t* Value() {
    return valuepkg.value;
  }

  size_t SizeofValue() {
    return header.value_size;
  }

  bool IsRealInsert() {
    return user_op == UserOP::kInsert;
  }

  void Debug() {
    // For debug usage
    std::cerr << "[OneObj debug] (meta) table id: " << this->header.table_id << ", value size: " << this->header.value_size << ", key: " << this->header.key << ", remote offset: " << this->header.remote_offset << ", lock: " << this->header.lock << std::endl;
    std::cerr << "(data) sa: " << (int)valuepkg.sa << ", valid: " << (int)vcell.valid << ", version: " << vcell.version << ", ea: " << (int)valuepkg.ea << std::endl;
  }

  const uint64_t GetRemoteLockAddr() const {
    return header.remote_offset + offsetof(Header, lock);
  }

  const uint64_t GetRemoteAttrAddr() const {
    return header.remote_offset + offsetof(Header, remote_attribute_offset);
  }

  const uint64_t GetRemoteValidAddr(int i) const {
    return header.remote_offset + sizeof(Header) + sizeof(VCell) * i + offsetof(VCell, valid);
  }

  const uint64_t GetRemoteVCellAddr(int i) const {
    return header.remote_offset + sizeof(Header) + sizeof(VCell) * i;
  }

} Aligned8;
constexpr size_t DataSetItemSize = sizeof(DataSetItem);
using DataSetItemPtr = std::shared_ptr<DataSetItem>;

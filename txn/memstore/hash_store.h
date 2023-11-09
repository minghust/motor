// Author: Ming Zhang
// Copyright (c) 2023

#pragma once

#include <cassert>
#include <functional>

#include "base/workload.h"
#include "memstore/cvt.h"
#include "memstore/mem_store.h"
#include "util/hash.h"

struct HashMeta {
  // To which table this hash store belongs
  table_id_t table_id;

  // Virtual address of the table
  uint64_t table_ptr;

  // Offset of the table, relative to the RDMA local_mr
  offset_t base_off;

  // Total hash buckets
  uint64_t bucket_num;

  // Size of a hash bucket
  size_t bucket_size;

  HashCore hash_core;

  HashMeta(table_id_t table_id,
           uint64_t table_ptr,
           offset_t base_off,
           uint64_t bucket_n,
           size_t bucket_size,
           HashCore core_func)
      : table_id(table_id),
        table_ptr(table_ptr),
        base_off(base_off),
        bucket_num(bucket_n),
        bucket_size(bucket_size),
        hash_core(core_func) {}
  HashMeta() {}
} Aligned8;

// struct HashBucket {
//   CVT cvts[SLOT_NUM_PER_BKT];  // a cvt occupies a slot
// } Aligned8;
// constexpr size_t HashBucketSize = sizeof(HashBucket);

// A hash store for a DB table

// Structure
// ==DB Table1==
// |          |
// |   Index  | <- User-defined bucket number
// |          |
// ------------
// |          |
// | FullValue| <- User-defined initial number of rows
// |          |
// ==DB Table2==
// |          |
// |   Index  |
// |          |
// ------------
// |          |
// | FullValue|
// |          |
// ...

class HashStore {
 public:
  HashStore(table_id_t table_id,
            uint64_t bucket_n,
            MemStoreAllocParam* param,
            HashCore func = HashCore::kDirectFunc)
      : table_id(table_id),
        base_off(0),
        bucket_num(bucket_n),
        table_ptr(nullptr),
        value_ptr(nullptr),
        region_start_ptr(param->mem_region_start),
        hash_core(func),
        init_insert_num(0) {
    assert(bucket_num > 0);

    // Calculate the total size of the hash table and initial full values
    size_t bkt_size = SLOT_NUM[table_id] * CVTSize;
    // size_t hash_table_size = bucket_num * HashBucketSize;
    size_t hash_table_size = bucket_num * bkt_size;
    vpkg_size = TABLE_VALUE_SIZE[table_id] + sizeof(anchor_t) * 2;
    total_size = hash_table_size + (SLOT_NUM[table_id] * bucket_n) * vpkg_size;

    if ((uint64_t)param->hash_store_start + param->alloc_offset + total_size >= (uint64_t)param->mem_store_end) {
      RDMA_LOG(FATAL) << "memory region too small!";
    }

    // Addr of the hash table
    table_ptr = param->hash_store_start + param->alloc_offset;

    // Addr of the initial full value region
    value_ptr = table_ptr + hash_table_size;

    // Move the pointer to next table
    param->alloc_offset += total_size;

    // The offset between this hash table and the MR
    base_off = (uint64_t)table_ptr - (uint64_t)region_start_ptr;

    assert(base_off >= 0);
    assert(table_ptr != nullptr);
    assert(value_ptr != nullptr);

    memset(table_ptr, 0, total_size);

    // std::cerr << "++++++++++++ Table Info +++++++++++++" << std::endl;

    // std::cerr << "Table ID: " << std::dec << table_id << std::endl;
    // std::cerr << "Value size (B): " << TABLE_VALUE_SIZE[table_id] << std::endl;
    // std::cerr << "Num of vcells: " << MAX_VCELL_NUM << std::endl;  // TODO: different tables have differnet # of vcells
    // std::cerr << "CVT size (B): " << CVTSize << std::endl;
    // std::cerr << "Slot num: " << SLOT_NUM[table_id] << std::endl;
    // std::cerr << "HashBucketNum: " << bucket_num << std::endl;
    // std::cerr << "Bucket size (B): " << bkt_size << std::endl;
    // std::cerr << "HashCore: " << (int)func << std::endl;
    // std::cerr << "HT+Vpkg size (MB): " << total_size / 1024 / 1024 << std::endl;
    // std::cerr << "Start address: " << std::hex << "0x" << (uint64_t)table_ptr << std::endl;
    // std::cerr << "Base_off:" << std::hex << "0x" << base_off << std::endl;

    // std::cerr << "++++++++++++++++++++++++++++++++++++" << std::endl;
  }

  table_id_t GetTableID() const {
    return table_id;
  }

  offset_t GetBaseOff() const {
    return base_off;
  }

  uint64_t GetHashBucketSize() const {
    return SLOT_NUM[table_id] * CVTSize;
  }

  uint64_t GetBucketNum() const {
    return bucket_num;
  }

  HashCore GetHashCore() const {
    return hash_core;
  }

  char* GetTablePtr() const {
    return table_ptr;
  }

  size_t GetTotalSize() const {
    return total_size;
  }

  size_t GetHTInitFVSize() const {
    return bucket_num * (SLOT_NUM[table_id] * CVTSize) + init_insert_num * vpkg_size;
  }

  size_t GetHTSize() const {
    return bucket_num * (SLOT_NUM[table_id] * CVTSize);
  }

  size_t GetInitFVSize() const {
    return init_insert_num * vpkg_size;
  }

  size_t GetLoadCVTSize() const {
    size_t header_size = 40;
    size_t vcell_size = 14;
    size_t effective_cvt_size = header_size + vcell_size * MAX_VCELL_NUM;
    return init_insert_num * effective_cvt_size;
    // return init_insert_num * CVTSize;
  }

  uint64_t GetInitInsertNum() const {
    return init_insert_num;
  }

  offset_t GetRemoteOffset(const void* ptr) const {
    return (uint64_t)ptr - (uint64_t)region_start_ptr;
  }

  size_t GetValidCVTSize() {
    size_t header_size = 40;
    size_t vcell_size = 14;
    size_t effective_cvt_size = header_size + vcell_size * MAX_VCELL_NUM;

    size_t valid_cvt_size = 0;
    size_t bkt_size = SLOT_NUM[table_id] * CVTSize;
    for (int bkt_pos = 0; bkt_pos < bucket_num; bkt_pos++) {
      char* cvt_start = bkt_pos * bkt_size + table_ptr;

      for (int slot_pos = 0; slot_pos < SLOT_NUM[table_id]; slot_pos++) {
        CVT* cvt = (CVT*)(cvt_start + slot_pos * CVTSize);
        if (cvt->header.value_size > 0) {
          valid_cvt_size += effective_cvt_size;
        }
      }
    }

    return valid_cvt_size;
  }

  size_t GetMaxOccupySlotNum() {
    size_t max_num = 0;
    size_t bkt_size = SLOT_NUM[table_id] * CVTSize;
    for (int bkt_id = 0; bkt_id < bucket_num; bkt_id++) {
      size_t num = 0;

      char* cvt_start = bkt_id * bkt_size + table_ptr;
      for (int slot_id = 0; slot_id < SLOT_NUM[table_id]; slot_id++) {
        CVT* cvt = (CVT*)(cvt_start + slot_id * CVTSize);
        if (cvt->header.value_size > 0) {
          num++;
        }
      }

      if (num > max_num) {
        max_num = num;
      }
    }

    return max_num;
  }

  void LocalInsertTuple(itemkey_t key, char* value, size_t value_size);

 private:
  // To which table this hash store belongs
  table_id_t table_id;

  // The offset in the RDMA region
  offset_t base_off;

  // Total hash buckets
  uint64_t bucket_num;

  // The pointer to the hash table
  char* table_ptr;

  // The pointer to the raw data values
  char* value_ptr;

  // Start of the memory region address
  char* region_start_ptr;

  // Which hash function this table uses
  HashCore hash_core;

  // Number of initial insertions
  uint64_t init_insert_num;

  // The size of a value package containing a data value and two anchors
  size_t vpkg_size;

  // The size of the entire hash tabletup.
  size_t total_size;
};

ALWAYS_INLINE
void HashStore::LocalInsertTuple(itemkey_t key, char* value, size_t value_size) {
  uint64_t bkt_pos = GetHash(key, bucket_num, hash_core);

  size_t bkt_size = SLOT_NUM[table_id] * CVTSize;
  char* cvt_start = bkt_pos * bkt_size + table_ptr;

  for (int i = 0; i < SLOT_NUM[table_id]; i++) {
    CVT* cvt = (CVT*)(cvt_start + i * CVTSize);
    if (cvt->header.value_size == 0) {
      char* value_insert_pos = value_ptr;

      cvt->header.table_id = table_id;
      cvt->header.lock = 0;
      cvt->header.key = key;
      cvt->header.remote_offset = GetRemoteOffset(cvt);
      cvt->header.remote_full_value_offset = GetRemoteOffset(value_insert_pos);
      cvt->header.remote_attribute_offset = UN_INIT_POS;
      cvt->header.value_size = value_size;
      cvt->header.user_inserted = false;

      cvt->vcell[0].sa = 0;
      cvt->vcell[0].valid = 1;
      cvt->vcell[0].version = 1;
      cvt->vcell[0].attri_so = 0;
      cvt->vcell[0].attri_bitmap = 0;
      cvt->vcell[0].ea = 0;

      Value* p = (Value*)value_insert_pos;
      p->sa = 0;
      memcpy(p->value, value, value_size);
      p->ea = 0;

      // Move the value pointer forward
      value_ptr += vpkg_size;

      init_insert_num++;

      return;
    }
  }

  RDMA_LOG(FATAL) << "Table " << table_id << " alloc a new bucket for key: " << key << ". Current slotnum per bucket: " << SLOT_NUM[table_id];
}

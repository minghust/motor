// Author: Ming Zhang
// Copyright (c) 2023

#pragma once

#include "allocator/region_allocator.h"
#include "base/common.h"

// Alloc registered RDMA buffer for each thread
class LocalBufferAllocator {
 public:
  LocalBufferAllocator(char* s, char* e) : start(s), end(e), cur_offset(0) {}

  ALWAYS_INLINE
  char* Alloc(int64_t size) {
    // When the thread local region is exhausted, the region
    // can be re-used (i.e., overwritten) at the front offset, i.e., 0. This is almost always true,
    // because the local region is typically GB-scale, and hence the front
    // allocated buffer has already finished serving for RDMA requests and replies, or has already aborted.
    // As such, our Allocator is extremely fast due to simply moving the pointer.
    // If anyone relies on a more reliable allocator, you can just re-implement this Alloc interface
    // using other standard allocators, e.g., ptmalloc/jemalloc/tcmalloc.

    assert(size > 0);

    if (unlikely(start + cur_offset + size > end)) {
      cur_offset = 0;
    }
    char* ret = start + cur_offset;
    cur_offset += size;
    return ret;
  }

  ALWAYS_INLINE
  void Free(void* p) {
    // As the memory region can be safely reused, we do not need to
    // explicitly deallocate the previously allocated memory region buffer.
  }

 private:
  // Each thread has a local RDMA region to temporarily alloc a small buffer.
  // This local region has an address range: [start, end)
  char* start;
  char* end;
  uint64_t cur_offset;
};

// Used for a thread to allocate a remote offset to append a full value or attributes into remote delta region

// When writing remote replicas, a coordinator should use the offset provided
// by NextDeltaOffset plus the remote delta region's start address to form
// a writable address. The remote delta region's start address is recorded in the meta manager
class RemoteDeltaOffsetAllocator {
 public:
  RemoteDeltaOffsetAllocator(const std::unordered_map<node_id_t, DeltaRange>& thread_delta_region) {
    for (int i = 0; i < MAX_REMOTE_NODE_NUM; i++) {
      starts[i] = 0;
      ends[i] = 0;
      cur_offsets[i] = 0;
    }

    cur_offset = 0;

    for (const auto& node : thread_delta_region) {
      starts[node.first] = node.second.start;
      ends[node.first] = node.second.end;

      start = node.second.start;
      end = node.second.end;
    }
  }

  ALWAYS_INLINE
  uintptr_t NextDeltaOffset(node_id_t mn_id, size_t write_size) {
    if (unlikely(starts[mn_id] + cur_offsets[mn_id] + write_size > ends[mn_id])) {
      RDMA_LOG(FATAL) << "Delta buffer not enough for this thread!";
    }

    uintptr_t ret = starts[mn_id] + cur_offsets[mn_id];
    cur_offsets[mn_id] += write_size;

    return ret;
  }

  // Write to all MNs the same way
  ALWAYS_INLINE
  uintptr_t NextDeltaOffset(size_t write_size) {
    if (unlikely(start + cur_offset + write_size > end)) {
      RDMA_LOG(FATAL) << "Delta buffer not enough for this thread! Current usage: " << (double)(cur_offset + write_size) / 1024 / 1024 << " MB delta space";
    }

    uintptr_t ret = start + cur_offset;
    cur_offset += write_size;

    return ret;
  }

  ALWAYS_INLINE
  double GetDeltaUsage() {
    return (double)cur_offset / 1024 / 1024;
  }

 private:
  uintptr_t starts[MAX_REMOTE_NODE_NUM];
  uintptr_t ends[MAX_REMOTE_NODE_NUM];
  uintptr_t cur_offsets[MAX_REMOTE_NODE_NUM];

  uintptr_t start;
  uintptr_t end;
  size_t cur_offset;
};

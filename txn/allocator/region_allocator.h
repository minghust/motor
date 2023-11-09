// Author: Ming Zhang
// Copyright (c) 2023

#pragma once

#include "connection/meta_manager.h"

const uint64_t PER_THREAD_ALLOC_SIZE = (size_t)200 * 1024 * 1024;

// This allocator is a global one which manages all the RDMA regions in this machine

// |                   | <- t1 start
// |                   |
// |       LOCAL       |
// |       RMDA        |
// |       REGION      | <- t1 end. t2 start
// |                   |
// |                   |
// |                   |
// |                   | <- t2 end. t3 start

class LocalRegionAllocator {
 public:
  LocalRegionAllocator(MetaManager* global_meta_man, t_id_t thread_num_per_machine) {
    size_t global_mr_size = (size_t)thread_num_per_machine * PER_THREAD_ALLOC_SIZE;
    // Register a buffer to the previous opened device. It's DRAM in compute pools
    global_mr = (char*)malloc(global_mr_size);
    thread_num = thread_num_per_machine;
    memset(global_mr, 0, global_mr_size);
    RDMA_ASSERT(global_meta_man->global_rdma_ctrl->register_memory(CLIENT_MR_ID, global_mr, global_mr_size, global_meta_man->opened_rnic));
  }

  ~LocalRegionAllocator() {
    if (global_mr) free(global_mr);
  }

  ALWAYS_INLINE
  std::pair<char*, char*> GetThreadLocalRegion(t_id_t tid) {
    assert(tid < thread_num);
    return std::make_pair(global_mr + tid * PER_THREAD_ALLOC_SIZE, global_mr + (tid + 1) * PER_THREAD_ALLOC_SIZE);
  }

 private:
  char* global_mr;  // memory region
  t_id_t thread_num;
};

// This allocator assigns a remote delta region to each global thread
// |                   | <- t1 start
// |                   |
// |      REMOTE       |
// |      DELTA        |
// |      REGION       | <- t1 end. t2 start
// |                   |
// |                   |
// |                   |
// |                   | <- t2 end. t3 start

struct DeltaRange {
  uintptr_t start;
  uintptr_t end;
};

class RemoteDeltaRegionAllocator {
 public:
  RemoteDeltaRegionAllocator(const MetaManager* global_meta_man,
                             const std::vector<RemoteNode>& mem_nodes) {
    for (const auto& node : mem_nodes) {
      mem_node_ids.push_back(node.node_id);
    }

    delta_start_off = global_meta_man->GetDeltaStartOffset();

    per_thread_delta_size = global_meta_man->GetPerThreadDeltaSize();
  }

  ALWAYS_INLINE
  void GetThreadDeltaRegion(t_id_t global_tid,
                            std::unordered_map<node_id_t, DeltaRange>& thread_delta_region) {
    if (global_tid >= MAX_CLIENT_NUM_PER_MN) {
      RDMA_LOG(FATAL) << "Exceeding max number of clients of per memory node."
                      << " Current thread id: " << global_tid
                      << " Max number: " << MAX_CLIENT_NUM_PER_MN;
    }

    for (auto id : mem_node_ids) {
      thread_delta_region[id] = DeltaRange{
          .start = delta_start_off + global_tid * per_thread_delta_size,
          .end = delta_start_off + (global_tid + 1) * per_thread_delta_size};
    }
  }

 private:
  std::vector<node_id_t> mem_node_ids;
  offset_t delta_start_off;
  size_t per_thread_delta_size;
};

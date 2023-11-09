// Author: Ming Zhang
// Copyright (c) 2023

#pragma once

#include <string>

#include "base/common.h"

enum class MemStoreType {
  kHash = 0,
  kBPlusTree,
};

struct MemStoreAllocParam {
  // The start of the registered memory region for storing memory stores
  char* mem_region_start;

  // The start of the hash store space
  char* hash_store_start;

  // The start offset of each memory store type
  offset_t alloc_offset;

  // The end of the whole memory store space (e.g., Hash Store Space)
  char* mem_store_end;

  MemStoreAllocParam(char* region_start, char* store_start, offset_t start_off, char* store_end)
      : mem_region_start(region_start),
        hash_store_start(store_start),
        alloc_offset(start_off),
        mem_store_end(store_end) {}
};

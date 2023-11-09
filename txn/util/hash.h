// Author: Ming Zhang
// Copyright (c) 2023

#pragma once

#include "base/common.h"
#include "util/latency.h"
#include "util/timer.h"

enum class HashCore : int {
  kDirectFunc = 0,
  kMurmurFunc,
};

// 64-bit hash for 64-bit platforms
ALWAYS_INLINE
static uint64_t
MurmurHash64A(uint64_t key, unsigned int seed) {
  const uint64_t m = 0xc6a4a7935bd1e995;
  const int r = 47;
  uint64_t h = seed ^ (8 * m);
  const uint64_t* data = &key;
  const uint64_t* end = data + 1;

  while (data != end) {
    uint64_t k = *data++;
    k *= m;
    k ^= k >> r;
    k *= m;
    h ^= k;
    h *= m;
  }

  h ^= h >> r;
  h *= m;
  h ^= h >> r;

  return h;
}

ALWAYS_INLINE
static uint64_t
GetHash(itemkey_t key, size_t bucket_num, HashCore hash_core) {
  return hash_core == HashCore::kDirectFunc ? (key % bucket_num) : MurmurHash64A(key, 0xdeadbeef) % bucket_num;
}

// Author: Ming Zhang
// Copyright (c) 2023

#pragma once

#include "base/common.h"

#if WORKLOAD_TPCC
#define MAX_LOCKED_KEY_NUM 200

#elif WORKLOAD_TATP
#define MAX_LOCKED_KEY_NUM 10

#elif WORKLOAD_SmallBank
#define MAX_LOCKED_KEY_NUM 10

#elif WORKLOAD_MICRO
#define MAX_LOCKED_KEY_NUM 10
#endif

struct LockedKeyEntry {
  node_id_t remote_node;
  offset_t remote_off;
};

// For each coordinator, i.e., coroutine
struct LockedKeyTable {
  tx_id_t tx_id;
  int num_entry;
  LockedKeyEntry entries[MAX_LOCKED_KEY_NUM];
};

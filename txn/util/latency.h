// Author: Ming Zhang
// Adapted from mica
// Copyright (c) 2023

#pragma once

#include <algorithm>
#include <cstdio>

// Test ibv_poll_cq
static inline unsigned long GetCPUCycle() {
  unsigned a, d;
  __asm __volatile("rdtsc"
                   : "=a"(a), "=d"(d));
  return ((unsigned long)a) | (((unsigned long)d) << 32);
}

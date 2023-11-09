// Author: Ming Zhang
// Copyright (c) 2023

#pragma once

#include <string>

#include "base/common.h"
#include "config/table_type.h"

union micro_key_t {
  uint64_t micro_id;
  uint64_t item_key;

  micro_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(micro_key_t) == sizeof(uint64_t), "");

enum micro_val_bitmap : int {
  d1 = 0,
  d2,
  d3,
  d4,
  d5,
};

struct micro_val_t {
  // 40 bytes, consistent with FaSST
  uint64_t d1;
  uint64_t d2;
  uint64_t d3;
  uint64_t d4;
  uint64_t d5;
} __attribute__((packed));

constexpr size_t micro_val_t_size = sizeof(micro_val_t);

// static_assert(sizeof(micro_val_t) == 40, "");

// Magic numbers for debugging. These are unused in the spec.
#define Micro_MAGIC 10 /* Some magic number <= 255 */
#define micro_magic (Micro_MAGIC)

// Helpers for generating workload
#define MICRO_TX_TYPES 2

enum MicroTxType : int {
  kUpdateOne,
  kReadOne
};

// #define MICRO_TX_TYPES 1
// enum MicroTxType : int {
//   kRWOne,
// };

// enum MicroTxType : int {
//   kTxTest1 = 1,
//   kTxTest2,
//   kTxTest3,
//   kTxTest4,
//   kTxTest5,
//   kTxTest6,
//   kTxTest7,
//   kTxTest8,
//   kTxTest9,
//   kTxTest10,
//   kTxTest11,
//   kTxTest12,
//   kTxTest100,
//   kTxTest101,
//   kRWOne,
// };

// const std::string MICRO_TX_NAME[MICRO_TX_TYPES] = {"RWOne"};

const std::string MICRO_TX_NAME[MICRO_TX_TYPES] = {"RWUpdateOne", "RWReadOne"};

// Table id
enum MicroTableType : uint64_t {
  kMicroTable = TABLE_MICRO,
};
const int MICRO_TOTAL_TABLES = 1;

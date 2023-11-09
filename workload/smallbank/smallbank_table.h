// Author: Ming Zhang
// Copyright (c) 2023
#pragma once

#include <string>

#include "base/common.h"
#include "config/table_type.h"

/* STORED PROCEDURE EXECUTION FREQUENCIES (0-100) */
#define FREQUENCY_AMALGAMATE 15
#define FREQUENCY_BALANCE 15
#define FREQUENCY_DEPOSIT_CHECKING 15
#define FREQUENCY_SEND_PAYMENT 25
#define FREQUENCY_TRANSACT_SAVINGS 15
#define FREQUENCY_WRITE_CHECK 15

#define TX_HOT 90 /* Percentage of txns that use accounts from hotspot */

// Smallbank table keys and values
// All keys have been sized to 8 bytes
// All values have been sized to the next multiple of 8 bytes

/*
 * SAVINGS table.
 */
union smallbank_savings_key_t {
  uint64_t acct_id;
  uint64_t item_key;

  smallbank_savings_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(smallbank_savings_key_t) == sizeof(uint64_t), "");

enum smallbank_savings_val_bitmap : int {
  sbal = 0
};

struct smallbank_savings_val_t {
  float bal;
  uint32_t magic;
  // 1 attribute
} __attribute__((packed));

constexpr size_t smallbank_savings_val_t_size = sizeof(smallbank_savings_val_t);

// static_assert(sizeof(smallbank_savings_val_t) == sizeof(uint64_t), "");

/*
 * CHECKING table
 */
union smallbank_checking_key_t {
  uint64_t acct_id;
  uint64_t item_key;

  smallbank_checking_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(smallbank_checking_key_t) == sizeof(uint64_t), "");

enum smallbank_checking_val_bitmap : int {
  cbal = 0
};

struct smallbank_checking_val_t {
  float bal;
  uint32_t magic;
  // 1 attribute
} __attribute__((packed));

constexpr size_t smallbank_checking_val_t_size = sizeof(smallbank_checking_val_t);

// static_assert(sizeof(smallbank_checking_val_t) == sizeof(uint64_t), "");

// Magic numbers for debugging. These are unused in the spec.
#define SmallBank_MAGIC 97 /* Some magic number <= 255 */
#define smallbank_savings_magic (SmallBank_MAGIC)
#define smallbank_checking_magic (SmallBank_MAGIC + 1)

// Helpers for generating workload
#define SmallBank_TX_TYPES 6
enum class SmallBankTxType : int {
  kAmalgamate,
  kBalance,
  kDepositChecking,
  kSendPayment,
  kTransactSaving,
  kWriteCheck,
};

const std::string SmallBank_TX_NAME[SmallBank_TX_TYPES] =
    {"Amalgamate", "Balance", "DepositChecking",
     "SendPayment", "TransactSaving", "WriteCheck"};

// Table id
enum SmallBankTableType : uint64_t {
  kSavingsTable = TABLE_SMALLBANK,
  kCheckingTable,
};
const int SmallBank_TOTAL_TABLES = 2;

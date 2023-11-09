// Author: Ming Zhang
// Copyright (c) 2023
#pragma once

#include <cassert>
#include <vector>

#include "memstore/hash_store.h"
#include "smallbank/smallbank_table.h"
#include "util/fast_random.h"
#include "util/json_config.h"

class SmallBank {
 public:
  std::string bench_name;

  uint32_t num_accounts_global, num_hot_global;

  /* Tables */
  HashStore* savings_table;

  HashStore* checking_table;

  std::vector<HashStore*> primary_table_ptrs;

  std::vector<HashStore*> backup_table_ptrs;

  // For server usage: Provide interfaces to servers for loading tables
  // Also for client usage: Provide interfaces to clients for generating ids during tests
  SmallBank() {
    bench_name = "SmallBank";
    // Used for populate table (line num) and get account
    std::string config_filepath = "../../../config/smallbank_config.json";
    auto json_config = JsonConfig::load_file(config_filepath);
    auto conf = json_config.get("smallbank");
    num_accounts_global = conf.get("num_accounts").get_uint64();
    num_hot_global = conf.get("num_hot_accounts").get_uint64();

    /* Up to 2 billion accounts */
    assert(num_accounts_global <= 2ull * 1024 * 1024 * 1024);

    savings_table = nullptr;
    checking_table = nullptr;
  }

  ~SmallBank() {
    if (savings_table) delete savings_table;
    if (checking_table) delete checking_table;
  }

  SmallBankTxType* CreateWorkgenArray() {
    SmallBankTxType* workgen_arr = new SmallBankTxType[100];

    int i = 0, j = 0;

    j += FREQUENCY_AMALGAMATE;
    for (; i < j; i++) workgen_arr[i] = SmallBankTxType::kAmalgamate;

    j += FREQUENCY_BALANCE;
    for (; i < j; i++) workgen_arr[i] = SmallBankTxType::kBalance;

    j += FREQUENCY_DEPOSIT_CHECKING;
    for (; i < j; i++) workgen_arr[i] = SmallBankTxType::kDepositChecking;

    j += FREQUENCY_SEND_PAYMENT;
    for (; i < j; i++) workgen_arr[i] = SmallBankTxType::kSendPayment;

    j += FREQUENCY_TRANSACT_SAVINGS;
    for (; i < j; i++) workgen_arr[i] = SmallBankTxType::kTransactSaving;

    j += FREQUENCY_WRITE_CHECK;
    for (; i < j; i++) workgen_arr[i] = SmallBankTxType::kWriteCheck;

    assert(i == 100 && j == 100);
    return workgen_arr;
  }

  /*
   * Generators for new account IDs. Called once per transaction because
   * we need to decide hot-or-not per transaction, not per account.
   */
  inline void get_account(uint64_t* seed, uint64_t* acct_id) const {
    if (FastRand(seed) % 100 < TX_HOT) {
      *acct_id = FastRand(seed) % num_hot_global;
    } else {
      *acct_id = FastRand(seed) % num_accounts_global;
    }
  }

  inline void get_two_accounts(uint64_t* seed, uint64_t* acct_id_0, uint64_t* acct_id_1) const {
    if (FastRand(seed) % 100 < TX_HOT) {
      *acct_id_0 = FastRand(seed) % num_hot_global;
      *acct_id_1 = FastRand(seed) % num_hot_global;
      while (*acct_id_1 == *acct_id_0) {
        *acct_id_1 = FastRand(seed) % num_hot_global;
      }
    } else {
      *acct_id_0 = FastRand(seed) % num_accounts_global;
      *acct_id_1 = FastRand(seed) % num_accounts_global;
      while (*acct_id_1 == *acct_id_0) {
        *acct_id_1 = FastRand(seed) % num_accounts_global;
      }
    }
  }

  void LoadTable(node_id_t node_id,
                 node_id_t num_server,
                 MemStoreAllocParam* mem_store_alloc_param,
                 size_t& total_size,
                 size_t& ht_loadfv_size,
                 size_t& ht_size,
                 size_t& initfv_size,
                 size_t& real_cvt_size);

  void PopulateSavingsTable();

  void PopulateCheckingTable();

  void LoadRecord(HashStore* table,
                  itemkey_t item_key,
                  void* val_ptr,
                  size_t val_size,
                  table_id_t table_id);

  ALWAYS_INLINE
  std::vector<HashStore*> GetPrimaryHashStore() {
    return primary_table_ptrs;
  }

  ALWAYS_INLINE
  std::vector<HashStore*> GetBackupHashStore() {
    return backup_table_ptrs;
  }
};

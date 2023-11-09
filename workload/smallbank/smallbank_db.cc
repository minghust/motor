// Author: Ming Zhang
// Copyright (c) 2023

#include "smallbank_db.h"

#include "unistd.h"
#include "util/json_config.h"

/* Called by main. Only initialize here. The worker threads will populate. */
void SmallBank::LoadTable(node_id_t node_id,
                          node_id_t num_server,
                          MemStoreAllocParam* mem_store_alloc_param,
                          size_t& total_size,
                          size_t& ht_loadfv_size,
                          size_t& ht_size,
                          size_t& initfv_size,
                          size_t& real_cvt_size) {
  std::string config_filepath = "../../../config/smallbank_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);
  auto table_config = json_config.get("smallbank");

  {
    RDMA_LOG(DBG) << "Loading SAVINGS table";
    savings_table = new HashStore((table_id_t)SmallBankTableType::kSavingsTable,
                                  table_config.get("num_accounts").get_uint64(),
                                  mem_store_alloc_param);
    PopulateSavingsTable();
    total_size += savings_table->GetTotalSize();
    ht_loadfv_size += savings_table->GetHTInitFVSize();
    ht_size += savings_table->GetHTSize();
    initfv_size += savings_table->GetInitFVSize();
    real_cvt_size += savings_table->GetLoadCVTSize();
  }

  {
    RDMA_LOG(DBG) << "Loading CHECKING table";
    checking_table = new HashStore((table_id_t)SmallBankTableType::kCheckingTable,
                                   table_config.get("num_accounts").get_uint64(),
                                   mem_store_alloc_param);
    PopulateCheckingTable();
    total_size += checking_table->GetTotalSize();
    ht_loadfv_size += checking_table->GetHTInitFVSize();
    ht_size += checking_table->GetHTSize();
    initfv_size += checking_table->GetInitFVSize();
    real_cvt_size += checking_table->GetLoadCVTSize();
  }

  std::cout << "----------------------------------------------------------" << std::endl;

  // Assign primary
  if ((node_id_t)SmallBankTableType::kSavingsTable % num_server == node_id) {
    RDMA_LOG(EMPH) << "[Primary] SAVINGS table ID: " << (node_id_t)SmallBankTableType::kSavingsTable;
    std::cerr << "Number of initial records: " << std::dec << savings_table->GetInitInsertNum() << std::endl;
    primary_table_ptrs.push_back(savings_table);
  }

  if ((node_id_t)SmallBankTableType::kCheckingTable % num_server == node_id) {
    RDMA_LOG(EMPH) << "[Primary] CHECKING table ID: " << (node_id_t)SmallBankTableType::kCheckingTable;
    std::cerr << "Number of initial records: " << std::dec << checking_table->GetInitInsertNum() << std::endl;
    primary_table_ptrs.push_back(checking_table);
  }

  std::cout << "----------------------------------------------------------" << std::endl;
  // Assign backup
  if (BACKUP_NUM < num_server) {
    for (node_id_t i = 1; i <= BACKUP_NUM; i++) {
      if ((node_id_t)SmallBankTableType::kSavingsTable % num_server == (node_id - i + num_server) % num_server) {
        RDMA_LOG(DBG) << "[Backup] SAVINGS table ID: " << (node_id_t)SmallBankTableType::kSavingsTable;
        std::cerr << "Number of initial records: " << std::dec << savings_table->GetInitInsertNum() << std::endl;
        backup_table_ptrs.push_back(savings_table);
      }

      if ((node_id_t)SmallBankTableType::kCheckingTable % num_server == (node_id - i + num_server) % num_server) {
        RDMA_LOG(DBG) << "[Backup] CHECKING table ID: " << (node_id_t)SmallBankTableType::kSavingsTable;
        std::cerr << "Number of initial records: " << std::dec << checking_table->GetInitInsertNum() << std::endl;
        backup_table_ptrs.push_back(checking_table);
      }
    }
  }
}

void SmallBank::LoadRecord(HashStore* table,
                           itemkey_t item_key,
                           void* val_ptr,
                           size_t val_size,
                           table_id_t table_id) {
  assert(val_size <= MAX_VALUE_SIZE);
  /* Insert into HashStore */
  table->LocalInsertTuple(item_key, (char*)val_ptr, val_size);
}

void SmallBank::PopulateSavingsTable() {
  /* All threads must execute the loop below deterministically */

  /* Populate the tables */
  for (uint32_t acct_id = 0; acct_id < num_accounts_global; acct_id++) {
    // Savings
    smallbank_savings_key_t savings_key;
    savings_key.acct_id = (uint64_t)acct_id;

    smallbank_savings_val_t savings_val;
    savings_val.magic = smallbank_savings_magic;
    savings_val.bal = 1000000000ull;
    LoadRecord(savings_table, savings_key.item_key,
               (void*)&savings_val, sizeof(smallbank_savings_val_t),
               (table_id_t)SmallBankTableType::kSavingsTable);
  }
}

void SmallBank::PopulateCheckingTable() {
  /* All threads must execute the loop below deterministically */

  /* Populate the tables */
  for (uint32_t acct_id = 0; acct_id < num_accounts_global; acct_id++) {
    // Checking
    smallbank_checking_key_t checking_key;
    checking_key.acct_id = (uint64_t)acct_id;

    smallbank_checking_val_t checking_val;
    checking_val.magic = smallbank_checking_magic;
    checking_val.bal = 1000000000ull;

    LoadRecord(checking_table, checking_key.item_key,
               (void*)&checking_val, sizeof(smallbank_checking_val_t),
               (table_id_t)SmallBankTableType::kCheckingTable);
  }
}

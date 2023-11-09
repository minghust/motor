// Author: Ming Zhang
// Copyright (c) 2023

#include "micro/micro_db.h"

#include "unistd.h"
#include "util/json_config.h"

/* Called by main. Only initialize here. The worker threads will populate. */
void MICRO::LoadTable(node_id_t node_id,
                      node_id_t num_server,
                      MemStoreAllocParam* mem_store_alloc_param,
                      size_t& total_size,
                      size_t& ht_loadfv_size,
                      size_t& ht_size,
                      size_t& initfv_size,
                      size_t& real_cvt_size) {
  // Initiate + Populate table for primary role
  {
    RDMA_LOG(DBG) << "Loading MICRO table";
    std::string config_filepath = "../../../config/micro_config.json";
    auto json_config = JsonConfig::load_file(config_filepath);
    auto table_config = json_config.get("micro");
    micro_table = new HashStore((table_id_t)MicroTableType::kMicroTable,
                                table_config.get("num_keys").get_uint64(),
                                mem_store_alloc_param);
    PopulateMicroTable();
    total_size += micro_table->GetTotalSize();
    ht_loadfv_size += micro_table->GetHTInitFVSize();
    ht_size += micro_table->GetHTSize();
    initfv_size += micro_table->GetInitFVSize();
    real_cvt_size += micro_table->GetLoadCVTSize();
  }

  std::cout << "----------------------------------------------------------" << std::endl;
  // Assign primary

  if ((node_id_t)MicroTableType::kMicroTable % num_server == node_id) {
    RDMA_LOG(EMPH) << "[Primary] MICRO table ID: " << (node_id_t)MicroTableType::kMicroTable;
    std::cerr << "Number of initial records: " << std::dec << micro_table->GetInitInsertNum() << std::endl;
    primary_table_ptrs.push_back(micro_table);
  }

  std::cout << "----------------------------------------------------------" << std::endl;
  // Assign backup

  if (BACKUP_NUM < num_server) {
    for (node_id_t i = 1; i <= BACKUP_NUM; i++) {
      if ((node_id_t)MicroTableType::kMicroTable % num_server == (node_id - i + num_server) % num_server) {
        RDMA_LOG(EMPH) << "[Backup] MICRO table ID: " << (node_id_t)MicroTableType::kMicroTable;
        std::cerr << "Number of initial records: " << std::dec << micro_table->GetInitInsertNum() << std::endl;
        backup_table_ptrs.push_back(micro_table);
      }
    }
  }
}

void MICRO::PopulateMicroTable() {
  RDMA_LOG(DBG) << "NUM KEYS TOTAL: " << num_keys_global;
  for (uint64_t id = 0; id < num_keys_global; id++) {
    micro_key_t micro_key;
    micro_key.micro_id = (uint64_t)id;

    micro_val_t micro_val;
    micro_val.d1 = micro_magic + 1;
    micro_val.d2 = micro_magic + 2;
    micro_val.d3 = micro_magic + 3;
    micro_val.d4 = micro_magic + 4;
    micro_val.d5 = micro_magic + 5;

    LoadRecord(micro_table, micro_key.item_key,
               (void*)&micro_val, sizeof(micro_val_t),
               (table_id_t)MicroTableType::kMicroTable);
  }
}

void MICRO::LoadRecord(HashStore* table,
                       itemkey_t item_key,
                       void* val_ptr,
                       size_t val_size,
                       table_id_t table_id) {
  assert(val_size <= MAX_VALUE_SIZE);
  table->LocalInsertTuple(item_key, (char*)val_ptr, val_size);
}

// Author: Ming Zhang
// Copyright (c) 2023

#include "tatp_db.h"

#include "unistd.h"
#include "util/json_config.h"

/* Only initialize here. The worker threads will populate. */
void TATP::LoadTable(node_id_t node_id,
                     node_id_t num_server,
                     MemStoreAllocParam* mem_store_alloc_param,
                     size_t& total_size,
                     size_t& ht_loadfv_size,
                     size_t& ht_size,
                     size_t& initfv_size,
                     size_t& real_cvt_size) {
  // Initiate + Populate table for primary role
  std::string config_filepath = "../../../config/tatp_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);
  auto table_config = json_config.get("tatp");

  {
    RDMA_LOG(DBG) << "Loading SUBSCRIBER table";
    subscriber_table = new HashStore((table_id_t)TATPTableType::kSubscriberTable,
                                     table_config.get("num_subscriber").get_uint64(),
                                     mem_store_alloc_param);
    PopulateSubscriberTable();
    total_size += subscriber_table->GetTotalSize();
    ht_loadfv_size += subscriber_table->GetHTInitFVSize();
    ht_size += subscriber_table->GetHTSize();
    initfv_size += subscriber_table->GetInitFVSize();
    real_cvt_size += subscriber_table->GetLoadCVTSize();

    std::cerr << "SUBSCRIBER max occupy slot num: " << subscriber_table->GetMaxOccupySlotNum() << std::endl;
  }

  {
    RDMA_LOG(DBG) << "Loading SECONDARY SUBSCRIBER table";
    sec_subscriber_table = new HashStore((table_id_t)TATPTableType::kSecSubscriberTable,
                                         table_config.get("sec_sub_bkt_num").get_uint64(),
                                         mem_store_alloc_param);
    PopulateSecondarySubscriberTable();
    total_size += sec_subscriber_table->GetTotalSize();
    ht_loadfv_size += sec_subscriber_table->GetHTInitFVSize();
    ht_size += sec_subscriber_table->GetHTSize();
    initfv_size += sec_subscriber_table->GetInitFVSize();
    real_cvt_size += sec_subscriber_table->GetLoadCVTSize();

    std::cerr << "SECONDARY SUBSCRIBER max occupy slot num: " << sec_subscriber_table->GetMaxOccupySlotNum() << std::endl;
  }

  {
    RDMA_LOG(DBG) << "Loading ACCESS INFO table";
    access_info_table = new HashStore((table_id_t)TATPTableType::kAccessInfoTable,
                                      table_config.get("access_info_bkt_num").get_uint64(),
                                      mem_store_alloc_param);
    PopulateAccessInfoTable();
    total_size += access_info_table->GetTotalSize();
    ht_loadfv_size += access_info_table->GetHTInitFVSize();
    ht_size += access_info_table->GetHTSize();
    initfv_size += access_info_table->GetInitFVSize();
    real_cvt_size += access_info_table->GetLoadCVTSize();

    std::cerr << "ACCESS INFO max occupy slot num: " << access_info_table->GetMaxOccupySlotNum() << std::endl;
  }

  {
    RDMA_LOG(DBG) << "Loading SPECIAL FACILITY+CALL FORWARDING table";
    special_facility_table = new HashStore((table_id_t)TATPTableType::kSpecialFacilityTable,
                                           table_config.get("spec_fac_bkt_num").get_uint64(),
                                           mem_store_alloc_param);

    call_forwarding_table = new HashStore((table_id_t)TATPTableType::kCallForwardingTable,
                                          table_config.get("call_fwd_bkt_num").get_uint64(),
                                          mem_store_alloc_param);
    PopulateSpecfacAndCallfwdTable();
    total_size += special_facility_table->GetTotalSize();
    ht_loadfv_size += special_facility_table->GetHTInitFVSize();
    ht_size += special_facility_table->GetHTSize();
    initfv_size += special_facility_table->GetInitFVSize();
    real_cvt_size += special_facility_table->GetLoadCVTSize();

    total_size += call_forwarding_table->GetTotalSize();
    ht_loadfv_size += call_forwarding_table->GetHTInitFVSize();
    ht_size += call_forwarding_table->GetHTSize();
    initfv_size += call_forwarding_table->GetInitFVSize();
    real_cvt_size += call_forwarding_table->GetLoadCVTSize();

    std::cerr << "SPECIAL FACILITY max occupy slot num: " << special_facility_table->GetMaxOccupySlotNum() << std::endl;
    std::cerr << "CALL FORWARDING max occupy slot num: " << call_forwarding_table->GetMaxOccupySlotNum() << std::endl;
  }

  std::cout << "----------------------------------------------------------" << std::endl;
  // Assign primary
  if ((node_id_t)TATPTableType::kSubscriberTable % num_server == node_id) {
    RDMA_LOG(EMPH) << "[Primary] SUBSCRIBER table ID: " << (node_id_t)TATPTableType::kSubscriberTable;
    std::cerr << "Number of initial records: " << std::dec << subscriber_table->GetInitInsertNum() << std::endl;
    primary_table_ptrs.push_back(subscriber_table);
  }

  if ((node_id_t)TATPTableType::kSecSubscriberTable % num_server == node_id) {
    RDMA_LOG(EMPH) << "[Primary] SECONDARY SUBSCRIBER table ID: " << (node_id_t)TATPTableType::kSecSubscriberTable;
    std::cerr << "Number of initial records: " << std::dec << sec_subscriber_table->GetInitInsertNum() << std::endl;
    primary_table_ptrs.push_back(sec_subscriber_table);
  }

  if ((node_id_t)TATPTableType::kAccessInfoTable % num_server == node_id) {
    RDMA_LOG(EMPH) << "[Primary] ACCESS INFO table ID: " << (node_id_t)TATPTableType::kAccessInfoTable;
    std::cerr << "Number of initial records: " << std::dec << access_info_table->GetInitInsertNum() << std::endl;
    primary_table_ptrs.push_back(access_info_table);
  }

  if ((node_id_t)TATPTableType::kSpecialFacilityTable % num_server == node_id) {
    RDMA_LOG(EMPH) << "[Primary] SPECIAL FACILITY+CALL FORWARDING table IDs: " << (node_id_t)TATPTableType::kSpecialFacilityTable << " + " << (node_id_t)TATPTableType::kCallForwardingTable;
    std::cerr << "Number of initial records: " << std::dec << special_facility_table->GetInitInsertNum() << std::endl;
    std::cerr << "Number of initial records: " << std::dec << call_forwarding_table->GetInitInsertNum() << std::endl;
    primary_table_ptrs.push_back(special_facility_table);
    primary_table_ptrs.push_back(call_forwarding_table);
  }

  std::cout << "----------------------------------------------------------" << std::endl;
  // Assign backup

  if (BACKUP_NUM < num_server) {
    for (node_id_t i = 1; i <= BACKUP_NUM; i++) {
      if ((node_id_t)TATPTableType::kSubscriberTable % num_server == (node_id - i + num_server) % num_server) {
        // Meaning: I (current node_id) am the backup-SubscriberTable of my primary. My primary-SubscriberTable
        // resides on a node, whose id is TATPTableType::kSubscriberTable % num_server
        // A possible layout: | P (My primary) | B1 (I'm here) | B2 (Or I'm here) |
        RDMA_LOG(DBG) << "[Backup] SUBSCRIBER table ID: " << (node_id_t)TATPTableType::kSubscriberTable;
        std::cerr << "Number of initial records: " << std::dec << subscriber_table->GetInitInsertNum() << std::endl;
        backup_table_ptrs.push_back(subscriber_table);
      }

      if ((node_id_t)TATPTableType::kSecSubscriberTable % num_server == (node_id - i + num_server) % num_server) {
        RDMA_LOG(DBG) << "[Backup] SECONDARY SUBSCRIBER table ID: " << (node_id_t)TATPTableType::kSecSubscriberTable;
        std::cerr << "Number of initial records: " << std::dec << sec_subscriber_table->GetInitInsertNum() << std::endl;
        backup_table_ptrs.push_back(sec_subscriber_table);
      }

      if ((node_id_t)TATPTableType::kAccessInfoTable % num_server == (node_id - i + num_server) % num_server) {
        RDMA_LOG(DBG) << "[Backup] ACCESS INFO table ID: " << (node_id_t)TATPTableType::kAccessInfoTable;
        std::cerr << "Number of initial records: " << std::dec << access_info_table->GetInitInsertNum() << std::endl;
        backup_table_ptrs.push_back(access_info_table);
      }

      if ((node_id_t)TATPTableType::kSpecialFacilityTable % num_server == (node_id - i + num_server) % num_server) {
        RDMA_LOG(DBG) << "[Backup] SPECIAL FACILITY+CALL FORWARDING table IDs: " << (node_id_t)TATPTableType::kSpecialFacilityTable << " + " << (node_id_t)TATPTableType::kCallForwardingTable;
        std::cerr << "Number of initial records: " << std::dec << special_facility_table->GetInitInsertNum() << std::endl;
        std::cerr << "Number of initial records: " << std::dec << call_forwarding_table->GetInitInsertNum() << std::endl;
        backup_table_ptrs.push_back(special_facility_table);
        backup_table_ptrs.push_back(call_forwarding_table);
      }
    }
  }
}

void TATP::PopulateSubscriberTable() {
  /* All threads must execute the loop below deterministically */
  uint64_t tmp_seed = 0xdeadbeef; /* Temporary seed for this function only */

  /* Populate the table */
  for (uint32_t s_id = 0; s_id < subscriber_size; s_id++) {
    tatp_sub_key_t key;
    key.s_id = s_id;

    /* Initialize the subscriber payload */
    tatp_sub_val_t sub_val;
    sub_val.sub_number = SimpleGetSubscribeNumFromSubscribeID(s_id);

    for (int i = 0; i < 5; i++) {
      sub_val.hex[i] = FastRand(&tmp_seed);
    }

    for (int i = 0; i < 10; i++) {
      sub_val.bytes[i] = FastRand(&tmp_seed);
    }

    sub_val.bits = FastRand(&tmp_seed);
    sub_val.msc_location = tatp_sub_msc_location_magic; /* Debug */
    sub_val.vlr_location = FastRand(&tmp_seed);

    LoadRecord(subscriber_table,
               key.item_key,
               (void*)&sub_val,
               sizeof(tatp_sub_val_t),
               (table_id_t)TATPTableType::kSubscriberTable);
  }
}

void TATP::PopulateSecondarySubscriberTable() {
  /* Populate the tables */
  for (uint32_t s_id = 0; s_id < subscriber_size; s_id++) {
    tatp_sec_sub_key_t key;
    key.sub_number = SimpleGetSubscribeNumFromSubscribeID(s_id);

    /* Initialize the subscriber payload */
    tatp_sec_sub_val_t sec_sub_val;
    sec_sub_val.s_id = s_id;
    sec_sub_val.magic = tatp_sec_sub_magic;

    LoadRecord(sec_subscriber_table, key.item_key,
               (void*)&sec_sub_val, sizeof(tatp_sec_sub_val_t),
               (table_id_t)TATPTableType::kSecSubscriberTable);
  }
}

void TATP::PopulateAccessInfoTable() {
  std::vector<uint8_t> ai_type_values = {1, 2, 3, 4};

  /* All threads must execute the loop below deterministically */
  uint64_t tmp_seed = 0xdeadbeef; /* Temporary seed for this function only */

  /* Populate the table */
  for (uint32_t s_id = 0; s_id < subscriber_size; s_id++) {
    std::vector<uint8_t> ai_type_vec = SelectUniqueItem(&tmp_seed, ai_type_values, 1, 4);
    for (uint8_t ai_type : ai_type_vec) {
      /* Insert access info record */
      tatp_accinf_key_t key;
      key.s_id = s_id;
      key.ai_type = ai_type;

      tatp_accinf_val_t accinf_val;
      accinf_val.data1 = tatp_accinf_data1_magic;

      /* Insert into table if I am replica number repl_i for key */
      LoadRecord(access_info_table, key.item_key,
                 (void*)&accinf_val, sizeof(tatp_accinf_val_t),
                 (table_id_t)TATPTableType::kAccessInfoTable);
    }
  }
}

/*
 * Which rows are inserted into the CALL FORWARDING table depends on which
 * rows get inserted into the SPECIAL FACILITY, so process these two jointly.
 */
void TATP::PopulateSpecfacAndCallfwdTable() {
  std::vector<uint8_t> sf_type_values = {1, 2, 3, 4};
  std::vector<uint8_t> start_time_values = {0, 8, 16};

  /* All threads must execute the loop below deterministically */
  uint64_t tmp_seed = 0xdeadbeef; /* Temporary seed for this function only */

  /* Populate the tables */
  for (uint32_t s_id = 0; s_id < subscriber_size; s_id++) {
    std::vector<uint8_t> sf_type_vec = SelectUniqueItem(
        &tmp_seed, sf_type_values, 1, 4);

    for (uint8_t sf_type : sf_type_vec) {
      /* Insert the special facility record */
      tatp_specfac_key_t key;
      key.s_id = s_id;
      key.sf_type = sf_type;

      tatp_specfac_val_t specfac_val;
      specfac_val.data_b[0] = tatp_specfac_data_b0_magic;
      specfac_val.is_active = (FastRand(&tmp_seed) % 100 < 85) ? 1 : 0;
      LoadRecord(special_facility_table, key.item_key,
                 (void*)&specfac_val, sizeof(tatp_specfac_val_t),
                 (table_id_t)TATPTableType::kSpecialFacilityTable);
      /*
       * The TATP spec requires a different initial probability
       * distribution of Call Forwarding records (see README). Here, we
       * populate the table using the steady state distribution.
       */
      for (size_t start_time = 0; start_time <= 16; start_time += 8) {
        /*
         * At steady state, each @start_time for <s_id, sf_type> is
         * equally likely to be present or absent.
         */
        if (FastRand(&tmp_seed) % 2 == 0) {
          continue;
        }

        /* Insert the call forwarding record */
        tatp_callfwd_key_t key;
        key.s_id = s_id;
        key.sf_type = sf_type;
        key.start_time = start_time;

        tatp_callfwd_val_t callfwd_val;
        callfwd_val.numberx[0] = tatp_callfwd_numberx0_magic;
        /* At steady state, @end_time is unrelated to @start_time */
        callfwd_val.end_time = (FastRand(&tmp_seed) % 24) + 1;
        LoadRecord(call_forwarding_table, key.item_key,
                   (void*)&callfwd_val, sizeof(tatp_callfwd_val_t),
                   (table_id_t)TATPTableType::kCallForwardingTable);

      } /* End loop start_time */
    }   /* End loop sf_type */
  }     /* End loop s_id */
}

void TATP::LoadRecord(HashStore* table,
                      itemkey_t item_key,
                      void* val_ptr,
                      size_t val_size,
                      table_id_t table_id) {
  assert(val_size <= MAX_VALUE_SIZE);
  /* Insert into HashStore */
  table->LocalInsertTuple(item_key, (char*)val_ptr, val_size);
}

/*
 * Select between N and M unique items from the values vector. The number
 * of values to be selected, and the actual values are chosen at random.
 */
std::vector<uint8_t> TATP::SelectUniqueItem(uint64_t* tmp_seed, std::vector<uint8_t> values, unsigned N, unsigned M) {
  assert(M >= N);
  assert(M >= values.size());

  std::vector<uint8_t> ret;

  int used[32];
  memset(used, 0, 32 * sizeof(int));

  int to_select = (FastRand(tmp_seed) % (M - N + 1)) + N;
  for (int i = 0; i < to_select; i++) {
    int index = FastRand(tmp_seed) % values.size();
    uint8_t value = values[index];
    assert(value < 32);

    if (used[value] == 1) {
      i--;
      continue;
    }

    used[value] = 1;
    ret.push_back(value);
  }
  return ret;
}

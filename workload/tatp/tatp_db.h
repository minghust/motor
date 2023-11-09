// Author: Ming Zhang
// Copyright (c) 2023

#pragma once

#include <cassert>
#include <vector>

#include "memstore/hash_store.h"
#include "tatp/tatp_table.h"
#include "util/fast_random.h"
#include "util/json_config.h"

class TATP {
 public:
  std::string bench_name;

  /* Map 0--999 to 12b, 4b/digit decimal representation */
  uint16_t* map_1000;

  uint32_t subscriber_size;

  /* TATP spec parameter for non-uniform random generation */
  uint32_t A;

  /* Tables */
  HashStore* subscriber_table;

  HashStore* sec_subscriber_table;

  HashStore* special_facility_table;

  HashStore* access_info_table;

  HashStore* call_forwarding_table;

  std::vector<HashStore*> primary_table_ptrs;

  std::vector<HashStore*> backup_table_ptrs;

  // For server and client usage: Provide interfaces to servers for loading tables
  TATP() {
    bench_name = "TATP";
    /* Init the precomputed decimal map */
    map_1000 = (uint16_t*)malloc(1000 * sizeof(uint16_t));
    for (size_t i = 0; i < 1000; i++) {
      uint32_t dig_1 = (i / 1) % 10;
      uint32_t dig_2 = (i / 10) % 10;
      uint32_t dig_3 = (i / 100) % 10;
      map_1000[i] = (dig_3 << 8) | (dig_2 << 4) | dig_1;
    }

    std::string config_filepath = "../../../config/tatp_config.json";
    auto json_config = JsonConfig::load_file(config_filepath);
    auto conf = json_config.get("tatp");
    subscriber_size = conf.get("num_subscriber").get_uint64();

    assert(subscriber_size <= TATP_MAX_SUBSCRIBERS);
    /* Compute the "A" parameter for nurand distribution as per spec */
    if (subscriber_size <= 1000000) {
      A = 65535;
    } else if (subscriber_size <= 10000000) {
      A = 1048575;
    } else {
      A = 2097151;
    }

    subscriber_table = nullptr;
    sec_subscriber_table = nullptr;
    special_facility_table = nullptr;
    access_info_table = nullptr;
    call_forwarding_table = nullptr;
  }

  ~TATP() {
    if (subscriber_table) delete subscriber_table;
    if (sec_subscriber_table) delete sec_subscriber_table;
    if (special_facility_table) delete special_facility_table;
    if (access_info_table) delete access_info_table;
    if (call_forwarding_table) delete call_forwarding_table;
  }

  /* create workload generation array for benchmarking */
  ALWAYS_INLINE
  TATPTxType* CreateWorkgenArray() {
    TATPTxType* workgen_arr = new TATPTxType[100];

    int i = 0, j = 0;

    j += FREQUENCY_GET_SUBSCRIBER_DATA;
    for (; i < j; i++) workgen_arr[i] = TATPTxType::kGetSubsciberData;

    j += FREQUENCY_GET_ACCESS_DATA;
    for (; i < j; i++) workgen_arr[i] = TATPTxType::kGetAccessData;

    j += FREQUENCY_GET_NEW_DESTINATION;
    for (; i < j; i++) workgen_arr[i] = TATPTxType::kGetNewDestination;

    j += FREQUENCY_UPDATE_SUBSCRIBER_DATA;
    for (; i < j; i++) workgen_arr[i] = TATPTxType::kUpdateSubscriberData;

    j += FREQUENCY_UPDATE_LOCATION;
    for (; i < j; i++) workgen_arr[i] = TATPTxType::kUpdateLocation;

    j += FREQUENCY_INSERT_CALL_FORWARDING;
    for (; i < j; i++) workgen_arr[i] = TATPTxType::kInsertCallForwarding;

    j += FREQUENCY_DELETE_CALL_FORWARDING;
    for (; i < j; i++) workgen_arr[i] = TATPTxType::kDeleteCallForwarding;

    assert(i == 100 && j == 100);
    return workgen_arr;
  }

  /*
   * Get a non-uniform-random distributed subscriber ID according to spec.
   * To get a non-uniformly random number between 0 and y:
   * NURand(A, 0, y) = (get_random(0, A) | get_random(0, y)) % (y + 1)
   */
  ALWAYS_INLINE
  uint32_t GetNonUniformRandomSubscriber(uint64_t* thread_local_seed) const {
    return ((FastRand(thread_local_seed) % subscriber_size) |
            (FastRand(thread_local_seed) & A)) %
           subscriber_size;
  }

  /* Get a subscriber number from a subscriber ID, fast */
  ALWAYS_INLINE
  tatp_sub_number_t FastGetSubscribeNumFromSubscribeID(uint32_t s_id) const {
    tatp_sub_number_t sub_number;
    sub_number.item_key = 0;
    sub_number.dec_0_1_2 = map_1000[s_id % 1000];
    s_id /= 1000;
    sub_number.dec_3_4_5 = map_1000[s_id % 1000];
    s_id /= 1000;
    sub_number.dec_6_7_8 = map_1000[s_id % 1000];

    return sub_number;
  }

  /* Get a subscriber number from a subscriber ID, simple */
  tatp_sub_number_t SimpleGetSubscribeNumFromSubscribeID(uint32_t s_id) {
#define update_sid()     \
  do {                   \
    s_id = s_id / 10;    \
    if (s_id == 0) {     \
      return sub_number; \
    }                    \
  } while (false)

    tatp_sub_number_t sub_number;
    sub_number.item_key = 0; /* Zero out all digits */

    sub_number.dec_0 = s_id % 10;
    update_sid();

    sub_number.dec_1 = s_id % 10;
    update_sid();

    sub_number.dec_2 = s_id % 10;
    update_sid();

    sub_number.dec_3 = s_id % 10;
    update_sid();

    sub_number.dec_4 = s_id % 10;
    update_sid();

    sub_number.dec_5 = s_id % 10;
    update_sid();

    sub_number.dec_6 = s_id % 10;
    update_sid();

    sub_number.dec_7 = s_id % 10;
    update_sid();

    sub_number.dec_8 = s_id % 10;
    update_sid();

    sub_number.dec_9 = s_id % 10;
    update_sid();

    sub_number.dec_10 = s_id % 10;
    update_sid();

    assert(s_id == 0);
    return sub_number;
  }

  // For server-side usage
  void LoadTable(node_id_t node_id,
                 node_id_t num_server,
                 MemStoreAllocParam* mem_store_alloc_param,
                 size_t& total_size,
                 size_t& ht_loadfv_size,
                 size_t& ht_size,
                 size_t& initfv_size,
                 size_t& real_cvt_size);

  void PopulateSubscriberTable();

  void PopulateSecondarySubscriberTable();

  void PopulateAccessInfoTable();

  void PopulateSpecfacAndCallfwdTable();

  void LoadRecord(HashStore* table,
                  itemkey_t item_key,
                  void* val_ptr,
                  size_t val_size,
                  table_id_t table_id);

  std::vector<uint8_t> SelectUniqueItem(uint64_t* tmp_seed, std::vector<uint8_t> values, unsigned N, unsigned M);

  ALWAYS_INLINE
  std::vector<HashStore*> GetPrimaryHashStore() {
    return primary_table_ptrs;
  }

  ALWAYS_INLINE
  std::vector<HashStore*> GetBackupHashStore() {
    return backup_table_ptrs;
  }
};

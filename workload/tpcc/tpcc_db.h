// Author: Ming Zhang
// Copyright (c) 2023

#pragma once

#include <cassert>
#include <set>
#include <vector>

#include "memstore/hash_store.h"
#include "tpcc/tpcc_table.h"
#include "util/fast_random.h"
#include "util/json_config.h"

class TPCC {
 public:
  std::string bench_name;

  // Pre-defined constants, which will be modified for tests
  uint32_t num_warehouse = 3000;

  uint32_t num_district_per_warehouse = 10;

  uint32_t num_customer_per_district = 3000;

  uint32_t num_item = 100000;

  uint32_t num_stock_per_warehouse = 100000;

  /* Tables */
  HashStore* warehouse_table = nullptr;

  HashStore* district_table = nullptr;

  HashStore* customer_table = nullptr;

  HashStore* history_table = nullptr;

  HashStore* new_order_table = nullptr;

  HashStore* order_table = nullptr;

  HashStore* order_line_table = nullptr;

  HashStore* item_table = nullptr;

  HashStore* stock_table = nullptr;

  HashStore* customer_index_table = nullptr;

  HashStore* order_index_table = nullptr;

  std::vector<HashStore*> primary_table_ptrs;

  std::vector<HashStore*> backup_table_ptrs;

  // For server and client usage: Provide interfaces to servers for loading tables
  TPCC() {
    bench_name = "TPCC";
    std::string config_filepath = "../../../config/tpcc_config.json";
    auto json_config = JsonConfig::load_file(config_filepath);
    auto table_config = json_config.get("tpcc");

    num_warehouse = table_config.get("num_warehouse").get_uint64();
    num_district_per_warehouse = table_config.get("num_district_per_warehouse").get_uint64();
    num_customer_per_district = table_config.get("num_customer_per_district").get_uint64();
    num_item = table_config.get("num_item").get_uint64();
    num_stock_per_warehouse = table_config.get("num_stock_per_warehouse").get_uint64();
  }

  ~TPCC() {
    delete warehouse_table;
    delete customer_table;
    delete history_table;
    delete new_order_table;
    delete order_table;
    delete order_line_table;
    delete item_table;
    delete stock_table;
  }

  /* create workload generation array for benchmarking */
  ALWAYS_INLINE
  TPCCTxType* CreateWorkgenArray() {
    TPCCTxType* workgen_arr = new TPCCTxType[100];

    int i = 0, j = 0;

    j += FREQUENCY_NEW_ORDER;
    for (; i < j; i++) workgen_arr[i] = TPCCTxType::kNewOrder;

    j += FREQUENCY_PAYMENT;
    for (; i < j; i++) workgen_arr[i] = TPCCTxType::kPayment;

    j += FREQUENCY_ORDER_STATUS;
    for (; i < j; i++) workgen_arr[i] = TPCCTxType::kOrderStatus;

    j += FREQUENCY_DELIVERY;
    for (; i < j; i++) workgen_arr[i] = TPCCTxType::kDelivery;

    j += FREQUENCY_STOCK_LEVEL;
    for (; i < j; i++) workgen_arr[i] = TPCCTxType::kStockLevel;

    assert(i == 100 && j == 100);
    return workgen_arr;
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

  void Populate_Warehouse_Table(unsigned long seed);

  void Populate_District_Table(unsigned long seed);

  void Populate_Customer_CustomerIndex_History_Table(unsigned long seed);

  void Populate_Order_OrderIndex_NewOrder_OrderLine_Table(unsigned long seed);

  void Populate_Item_Table(unsigned long seed);

  void Populate_Stock_Table(unsigned long seed);

  void LoadRecord(HashStore* table,
                  itemkey_t item_key,
                  void* val_ptr,
                  size_t val_size,
                  table_id_t table_id);

  ALWAYS_INLINE
  std::vector<HashStore*>& GetPrimaryHashStore() {
    return primary_table_ptrs;
  }

  ALWAYS_INLINE
  std::vector<HashStore*>& GetBackupHashStore() {
    return backup_table_ptrs;
  }

  /* Followng pieces of codes mainly comes from Silo */
  ALWAYS_INLINE
  uint32_t GetCurrentTimeMillis() {
    // implement a scalable GetCurrentTimeMillis()
    // for now, we just give each core an increasing number
    static __thread uint32_t tl_hack = 0;
    return ++tl_hack;
  }

  // utils for generating random #s and strings
  ALWAYS_INLINE
  int CheckBetweenInclusive(int v, int lower, int upper) {
    assert(v >= lower);
    assert(v <= upper);
    return v;
  }

  ALWAYS_INLINE
  int RandomNumber(FastRandom& r, int min, int max) {
    return CheckBetweenInclusive((int)(r.NextUniform() * (max - min + 1) + min), min, max);
  }

  ALWAYS_INLINE
  int NonUniformRandom(FastRandom& r, int A, int C, int min, int max) {
    return (((RandomNumber(r, 0, A) | RandomNumber(r, min, max)) + C) % (max - min + 1)) + min;
  }

  ALWAYS_INLINE
  int64_t GetItemId(FastRandom& r) {
    return CheckBetweenInclusive(g_uniform_item_dist ? RandomNumber(r, 1, num_item) : NonUniformRandom(r, 8191, 7911, 1, num_item), 1, num_item);
  }

  ALWAYS_INLINE
  int GetCustomerId(FastRandom& r) {
    return CheckBetweenInclusive(NonUniformRandom(r, 1023, 259, 1, num_customer_per_district), 1, num_customer_per_district);
  }

  // pick a number between [start, end)
  ALWAYS_INLINE
  unsigned PickWarehouseId(FastRandom& r, unsigned start, unsigned end) {
    assert(start < end);
    const unsigned diff = end - start;
    if (diff == 1)
      return start;
    return (r.Next() % diff) + start;
  }

  inline size_t GetCustomerLastName(uint8_t* buf, FastRandom& r, int num) {
    const std::string& s0 = NameTokens[num / 100];
    const std::string& s1 = NameTokens[(num / 10) % 10];
    const std::string& s2 = NameTokens[num % 10];
    uint8_t* const begin = buf;
    const size_t s0_sz = s0.size();
    const size_t s1_sz = s1.size();
    const size_t s2_sz = s2.size();
    memcpy(buf, s0.data(), s0_sz);
    buf += s0_sz;
    memcpy(buf, s1.data(), s1_sz);
    buf += s1_sz;
    memcpy(buf, s2.data(), s2_sz);
    buf += s2_sz;
    return buf - begin;
  }

  ALWAYS_INLINE
  size_t GetCustomerLastName(char* buf, FastRandom& r, int num) {
    return GetCustomerLastName((uint8_t*)buf, r, num);
  }

  inline std::string GetCustomerLastName(FastRandom& r, int num) {
    std::string ret;
    ret.resize(CustomerLastNameMaxSize);
    ret.resize(GetCustomerLastName((uint8_t*)&ret[0], r, num));
    return ret;
  }

  ALWAYS_INLINE
  std::string GetNonUniformCustomerLastNameLoad(FastRandom& r) {
    return GetCustomerLastName(r, NonUniformRandom(r, 255, 157, 0, 999));
  }

  ALWAYS_INLINE
  size_t GetNonUniformCustomerLastNameRun(uint8_t* buf, FastRandom& r) {
    return GetCustomerLastName(buf, r, NonUniformRandom(r, 255, 223, 0, 999));
  }

  ALWAYS_INLINE
  size_t GetNonUniformCustomerLastNameRun(char* buf, FastRandom& r) {
    return GetNonUniformCustomerLastNameRun((uint8_t*)buf, r);
  }

  ALWAYS_INLINE
  std::string GetNonUniformCustomerLastNameRun(FastRandom& r) {
    return GetCustomerLastName(r, NonUniformRandom(r, 255, 223, 0, 999));
  }

  ALWAYS_INLINE
  std::string RandomStr(FastRandom& r, uint64_t len) {
    // this is a property of the oltpbench implementation...
    if (!len)
      return "";

    uint64_t i = 0;
    std::string buf(len, 0);
    while (i < (len)) {
      const char c = (char)r.NextChar();
      // oltpbench uses java's Character.isLetter(), which
      // is a less restrictive filter than isalnum()
      if (!isalnum(c))
        continue;
      buf[i++] = c;
    }
    return buf;
  }

  // RandomNStr() actually produces a string of length len
  ALWAYS_INLINE
  std::string RandomNStr(FastRandom& r, uint64_t len) {
    const char base = '0';
    std::string buf(len, 0);
    for (uint64_t i = 0; i < len; i++)
      buf[i] = (char)(base + (r.Next() % 10));
    return buf;
  }

  ALWAYS_INLINE
  int64_t MakeDistrictKey(int32_t w_id, int32_t d_id) {
    int32_t did = d_id + (w_id * num_district_per_warehouse);
    int64_t id = static_cast<int64_t>(did);
    // assert(districtKeyToWare(id) == w_id);
    return id;
  }

  ALWAYS_INLINE
  int64_t MakeCustomerKey(int32_t w_id, int32_t d_id, int32_t c_id) {
    int32_t upper_id = w_id * num_district_per_warehouse + d_id;
    int64_t id = static_cast<int64_t>(upper_id) << 32 | static_cast<int64_t>(c_id);
    // assert(customerKeyToWare(id) == w_id);
    return id;
  }

  // only used for customer index, maybe some problems when used.
  ALWAYS_INLINE
  void ConvertString(char* newstring, const char* oldstring, int size) {
    for (int i = 0; i < 8; i++)
      if (i < size)
        newstring[7 - i] = oldstring[i];
      else
        newstring[7 - i] = '\0';

    for (int i = 8; i < 16; i++)
      if (i < size)
        newstring[23 - i] = oldstring[i];
      else
        newstring[23 - i] = '\0';
  }

  ALWAYS_INLINE
  uint64_t MakeCustomerIndexKey(int32_t w_id, int32_t d_id, std::string s_last, std::string s_first) {
    uint64_t* seckey = new uint64_t[5];
    int32_t did = d_id + (w_id * num_district_per_warehouse);
    seckey[0] = did;
    ConvertString((char*)(&seckey[1]), s_last.data(), s_last.size());
    ConvertString((char*)(&seckey[3]), s_first.data(), s_first.size());
    return (uint64_t)seckey;
  }

  ALWAYS_INLINE
  int64_t MakeHistoryKey(int32_t h_w_id, int32_t h_d_id, int32_t h_c_w_id, int32_t h_c_d_id, int32_t h_c_id) {
    int32_t cid = (h_c_w_id * num_district_per_warehouse + h_c_d_id) * num_customer_per_district + h_c_id;
    int32_t did = h_d_id + (h_w_id * num_district_per_warehouse);
    int64_t id = static_cast<int64_t>(cid) << 20 | static_cast<int64_t>(did);
    return id;
  }

  ALWAYS_INLINE
  int64_t MakeNewOrderKey(int32_t w_id, int32_t d_id, int32_t o_id) {
    int32_t upper_id = w_id * num_district_per_warehouse + d_id;
    int64_t id = static_cast<int64_t>(upper_id) << 32 | static_cast<int64_t>(o_id);
    return id;
  }

  ALWAYS_INLINE
  int64_t MakeOrderKey(int32_t w_id, int32_t d_id, int32_t o_id) {
    int32_t upper_id = w_id * num_district_per_warehouse + d_id;
    int64_t id = static_cast<int64_t>(upper_id) << 32 | static_cast<int64_t>(o_id);
    // assert(orderKeyToWare(id) == w_id);
    return id;
  }

  ALWAYS_INLINE
  int64_t MakeOrderIndexKey(int32_t w_id, int32_t d_id, int32_t c_id, int32_t o_id) {
    int32_t upper_id = (w_id * num_district_per_warehouse + d_id) * num_customer_per_district + c_id;
    int64_t id = static_cast<int64_t>(upper_id) << 32 | static_cast<int64_t>(o_id);
    return id;
  }

  ALWAYS_INLINE
  int64_t MakeOrderLineKey(int32_t w_id, int32_t d_id, int32_t o_id, int32_t number) {
    int32_t upper_id = w_id * num_district_per_warehouse + d_id;
    // 10000000 is the MAX ORDER ID
    int64_t oid = static_cast<int64_t>(upper_id) * 10000000 + static_cast<int64_t>(o_id);
    int64_t olid = oid * 15 + number;
    int64_t id = static_cast<int64_t>(olid);
    // assert(orderLineKeyToWare(id) == w_id);
    return id;
  }

  ALWAYS_INLINE
  int64_t MakeStockKey(int32_t w_id, int32_t i_id) {
    int32_t item_id = i_id + (w_id * num_stock_per_warehouse);
    int64_t s_id = static_cast<int64_t>(item_id);
    // assert(stockKeyToWare(id) == w_id);
    return s_id;
  }
};

// consistency Requirements
//
// 1. Entries in the WAREHOUSE and DISTRICT tables must satisfy the relationship:
//        W_YTD = sum(D_YTD)
//    for each warehouse defined by (W_ID = D_W_ID).

// 2. Entries in the DISTRICT, ORDER, and NEW-ORDER tables must satisfy the relationship:
//         D_NEXT_O_ID - 1 = max(O_ID) = max(NO_O_ID)
//     for each district defined by (D_W_ID = O_W_ID = NO_W_ID) and (D_ID = O_D_ID = NO_D_ID).
//     This condition does not apply to the NEW-ORDER table for any districts which have no outstanding new orders
//     (i.e., the numbe r of rows is zero).

// 3. Entries in the NEW-ORDER table must satisfy the relationship:
//         max(NO_O_ID) - min(NO_O_ID) + 1 = [number of rows in the NEW-ORDER table for this district]
//     for each district defined by NO_W_ID and NO_D_ID.
//     This condition does not apply to any districts which have no outstanding new orders (i.e., the number of rows is zero).

// 4. Entries in the ORDER and ORDER-LINE tables must satisfy the relationship:
//         sum(O_OL_CNT) = [number of rows in the ORDER-LINE table for this district]
//     for each district defined by (O_W_ID = OL_W_ID) and (O_D_ID = OL_D_ID).

// 5. For any row in the ORDER table, O_CARRIER_ID is set to a null value if and only if
//     there is a corresponding row in the NEW-ORDER table defined by (O_W_ID, O_D_ID, O_ID) = (NO_W_ID, NO_D_ID, NO_O_ID).

// 6. For any row in the ORDER table, O_OL_CNT must equal the number of rows in the ORDER-LINE table
//     for the corresponding order defined by (O_W_ID, O_D_ID, O_ID) = (OL_W_ID, OL_D_ID, OL_O_ID).

// 7. For any row in the ORDER-LINE table, OL_DELIVERY_D is set to a null date/ time if and only if
//     the corresponding row in the ORDER table defined by
//     (O_W_ID, O_D_ID, O_ID) = (OL_W_ID, OL_D_ID, OL_O_ID) has O_CARRIER_ID set to a null value.

// 8. Entries in the WAREHOUSE and HISTORY tables must satisfy the relationship: W_YTD = sum(H_AMOUNT)
//     for each warehouse defined by (W_ID = H_W_ID). 3.3.2.9

// 9. Entries in the DISTRICT and HISTORY tables must satisfy the relationship: D_YTD = sum(H_AMOUNT)
//     for each district defined by (D_W_ID, D_ID) = (H_W_ID, H_D_ID). 3.3.2.10

// 10. Entries in the CUSTOMER, HISTORY, ORDER, and ORDER-LINE tables must satisfy the relationship:
//     C_BALANCE = sum(OL_AMOUNT) - sum(H_AMOUNT)
//     where: H_AMOUNT is selected by (C_W_ID, C_D_ID, C_ID) = (H_C_W_ID, H_C_D_ID, H_C_ID) and
//     OL_AMOUNT is selected by: (OL_W_ID, OL_D_ID, OL_O_ID) = (O_W_ID, O_D_ID, O_ID) and
//     (O_W_ID, O_D_ID, O_C_ID) = (C_W_ID, C_D_ID, C_ID) and (OL_DELIVERY_D is not a null value)

// 11. Entries in the CUSTOMER, ORDER and NEW-ORDER tables must satisfy the relationship:
//     (count(*) from ORDER) - (count(*) from NEW-ORDER) = 2100
//     for each district defined by (O_W_ID, O_D_ID) = (NO_W_ID, NO_D_ID) = (C_W_ID, C_D_ID). 3.3.2.12

// 12. Entries in the CUSTOMER and ORDER-LINE tables must satisfy the relationship:
//     C_BALANCE + C_YTD_PAYMENT = sum(OL_AMOUNT)
//     for any randomly selected customers and where OL_DELIVERY_D is not set to a null date/ time.
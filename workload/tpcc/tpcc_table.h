// Author: Ming Zhang
// Copyright (c) 2023

#pragma once

#include <string>

#include "base/common.h"
#include "config/table_type.h"

// YYYY-MM-DD HH:MM:SS This is supposed to be a date/time field from Jan 1st 1900 -
// Dec 31st 2100 with a resolution of 1 second. See TPC-C 5.11.0.
// static const int DATETIME_SIZE = 14;
// Use uint32 for data and time
static int g_uniform_item_dist = 0;

static int g_new_order_remote_item_pct = 1;

static int g_mico_dist_num = 20;

static const size_t CustomerLastNameMaxSize = 16;

static std::string NameTokens[10] = {
    std::string("BAR"),
    std::string("OUGHT"),
    std::string("ABLE"),
    std::string("PRI"),
    std::string("PRES"),
    std::string("ESE"),
    std::string("ANTI"),
    std::string("CALLY"),
    std::string("ATION"),
    std::string("EING"),
};

const char GOOD_CREDIT[] = "GC";

const char BAD_CREDIT[] = "BC";

static const int DUMMY_SIZE = 12;

static const int DIST = 24;

static const int NUM_DISTRICT_PER_WAREHOUSE = 10;

// Constants
struct Address {
  static const int MIN_STREET = 10;  // W_STREET_1 random a-string [10 .. 20] W_STREET_2 random a-string [10 .. 20]
  static const int MAX_STREET = 20;
  static const int MIN_CITY = 10;  // W_CITY random a-string [10 .. 20]
  static const int MAX_CITY = 20;
  static const int STATE = 2;  // W_STATE random a-string of 2 letters
  static const int ZIP = 9;    // ZIP a-string of 9 letters
};

/******************** TPCC table definitions (Schemas of key and value) start **********************/
/*
 * Warehouse table
 * Primary key: <int32_t w_id>
 */

union tpcc_warehouse_key_t {
  struct {
    int32_t w_id;
    uint8_t unused[4];
  };
  itemkey_t item_key;

  tpcc_warehouse_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tpcc_warehouse_key_t) == sizeof(itemkey_t), "");

enum tpcc_warehouse_val_bitmap : int {
  w_tax = 0,
  w_ytd,
  w_name,
  w_street_1,
  w_street_2,
  w_city,
  w_state,
  w_zip
};

struct tpcc_warehouse_val_t {
  static const int MIN_NAME = 6;
  static const int MAX_NAME = 10;

  float w_tax;
  float w_ytd;
  char w_name[MAX_NAME + 1];
  char w_street_1[Address::MAX_STREET + 1];
  char w_street_2[Address::MAX_STREET + 1];
  char w_city[Address::MAX_CITY + 1];
  char w_state[Address::STATE + 1];
  char w_zip[Address::ZIP + 1];
  // 8 attributes
} __attribute__((packed));

constexpr size_t tpcc_warehouse_val_t_size = sizeof(tpcc_warehouse_val_t);

// static_assert(sizeof(tpcc_warehouse_val_t) == 96, "");

/*
 * District table
 * Primary key: <int32_t d_id, int32_t d_w_id>
 */

union tpcc_district_key_t {
  struct {
    int64_t d_id;
  };
  itemkey_t item_key;

  tpcc_district_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tpcc_district_key_t) == sizeof(itemkey_t), "");

enum tpcc_district_val_bitmap : int {
  d_tax = 0,
  d_ytd,
  d_next_o_id,
  d_name,
  d_street_1,
  d_street_2,
  d_city,
  d_state,
  d_zip,
};

struct tpcc_district_val_t {
  static const int MIN_NAME = 6;
  static const int MAX_NAME = 10;

  float d_tax;
  float d_ytd;
  int32_t d_next_o_id;
  char d_name[MAX_NAME + 1];
  char d_street_1[Address::MAX_STREET + 1];
  char d_street_2[Address::MAX_STREET + 1];
  char d_city[Address::MAX_CITY + 1];
  char d_state[Address::STATE + 1];
  char d_zip[Address::ZIP + 1];
  // 9 attributes
} __attribute__((packed));

constexpr size_t tpcc_district_val_t_size = sizeof(tpcc_district_val_t);

// static_assert(sizeof(tpcc_district_val_t) == 100, "");

/*
 * Customer table
 * Primary key: <int32_t c_id, int32_t c_d_id, int32_t c_w_id>
 */

union tpcc_customer_key_t {
  int64_t c_id;
  itemkey_t item_key;

  tpcc_customer_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tpcc_customer_key_t) == sizeof(itemkey_t), "");

enum tpcc_customer_val_bitmap : int {
  c_credit_lim = 0,
  c_data,
  c_discount,
  c_balance,
  c_ytd_payment,
  c_payment_cnt,
  c_delivery_cnt,
  c_first,
  c_middle,
  c_last,
  c_street_1,
  c_street_2,
  c_city,
  c_state,
  c_zip,
  c_phone,
  c_since,
  c_credit,
};

struct tpcc_customer_val_t {
  static const int MIN_FIRST = 8;  // C_FIRST random a-string [8 .. 16]
  static const int MAX_FIRST = 16;
  static const int MIDDLE = 2;
  static const int MAX_LAST = 16;
  static const int PHONE = 16;  // C_PHONE random n-string of 16 numbers
  static const int CREDIT = 2;
  static const int MIN_DATA = 300;  // C_DATA random a-string [300 .. 500]
  static const int MAX_DATA = 500;

  float c_credit_lim;
  char c_data[MAX_DATA + 1];
  float c_discount;
  float c_balance;
  float c_ytd_payment;
  int32_t c_payment_cnt;
  int32_t c_delivery_cnt;
  char c_first[MAX_FIRST + 1];
  char c_middle[MIDDLE + 1];
  char c_last[MAX_LAST + 1];
  char c_street_1[Address::MAX_STREET + 1];
  char c_street_2[Address::MAX_STREET + 1];
  char c_city[Address::MAX_CITY + 1];
  char c_state[Address::STATE + 1];
  char c_zip[Address::ZIP + 1];
  char c_phone[PHONE + 1];
  uint32_t c_since;
  char c_credit[CREDIT + 1];
  // 18 attributes
} __attribute__((packed));

constexpr size_t tpcc_customer_val_t_size = sizeof(tpcc_customer_val_t);

// static_assert(sizeof(tpcc_customer_val_t) == 664, "");

union tpcc_customer_index_key_t {
  struct {
    uint64_t c_index_id;
  };
  itemkey_t item_key;

  tpcc_customer_index_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tpcc_customer_index_key_t) == sizeof(itemkey_t), "");

enum tpcc_customer_index_val_bitmap : int {
  c_id = 0
};

struct tpcc_customer_index_val_t {
  int64_t c_id;
  int64_t debug_magic;
} __attribute__((packed));

constexpr size_t tpcc_customer_index_val_t_size = sizeof(tpcc_customer_index_val_t);

// static_assert(sizeof(tpcc_customer_index_val_t) == 16, "");  // add debug magic

/*
 * History table
 * Primary key: none
 */

union tpcc_history_key_t {
  int64_t h_id;
  itemkey_t item_key;

  tpcc_history_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tpcc_history_key_t) == sizeof(itemkey_t), "");

enum tpcc_history_val_bitmap : int {
  h_amount = 0,
  h_date,
  h_data
};

struct tpcc_history_val_t {
  static const int MIN_DATA = 12;  // H_DATA random a-string [12 .. 24] from TPCC documents 5.11
  static const int MAX_DATA = 24;

  float h_amount;
  uint32_t h_date;
  char h_data[MAX_DATA + 1];
  // 3 attributes
} __attribute__((packed));

constexpr size_t tpcc_history_val_t_size = sizeof(tpcc_history_val_t);

// static_assert(sizeof(tpcc_history_val_t) == 36, "");

/*
 * NewOrder table
 * Primary key: <int32_t no_w_id, int32_t no_d_id, int32_t no_o_id>
 */
union tpcc_new_order_key_t {
  int64_t no_id;
  itemkey_t item_key;

  tpcc_new_order_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tpcc_new_order_key_t) == sizeof(itemkey_t), "");

enum tpcc_new_order_val_bitmap : int {
  no_dummy = 0
};

struct tpcc_new_order_val_t {
  static constexpr double SCALE_CONSTANT_BETWEEN_NEWORDER_ORDER = 0.7;

  char no_dummy[DUMMY_SIZE + 1];
  int64_t debug_magic;
  // 1 attribute
} __attribute__((packed));

constexpr size_t tpcc_new_order_val_t_size = sizeof(tpcc_new_order_val_t);

// static_assert(sizeof(tpcc_new_order_val_t) == 24, "");  // add debug magic

/*
 * Order table
 * Primary key: <int32_t o_w_id, int32_t o_d_id, int32_t o_id>
 */
union tpcc_order_key_t {
  int64_t o_id;
  itemkey_t item_key;

  tpcc_order_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tpcc_order_key_t) == sizeof(itemkey_t), "");

enum tpcc_order_val_bitmap : int {
  o_c_id = 0,
  o_carrier_id,
  o_ol_cnt,
  o_all_local,
  o_entry_d
};

struct tpcc_order_val_t {
  static const int MIN_CARRIER_ID = 1;
  static const int MAX_CARRIER_ID = 10;  // number of distinct per warehouse

  int32_t o_c_id;
  int32_t o_carrier_id;
  int32_t o_ol_cnt;
  int32_t o_all_local;
  uint32_t o_entry_d;
  // 5 attributes
} __attribute__((packed));

constexpr size_t tpcc_order_val_t_size = sizeof(tpcc_order_val_t);

// static_assert(sizeof(tpcc_order_val_t) == 20, "");

union tpcc_order_index_key_t {
  int64_t o_index_id;
  itemkey_t item_key;

  tpcc_order_index_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tpcc_order_index_key_t) == sizeof(itemkey_t), "");

enum tpcc_order_index_val_bitmap : int {
  o_id = 0
};

struct tpcc_order_index_val_t {
  uint64_t o_id;
  int64_t debug_magic;
  // 1 attribute
} __attribute__((packed));

constexpr size_t tpcc_order_index_val_t_size = sizeof(tpcc_order_index_val_t);

// static_assert(sizeof(tpcc_order_index_val_t) == 16, "");  // add debug magic

/*
 * OrderLine table
 * Primary key: <int32_t ol_o_id, int32_t ol_d_id, int32_t ol_w_id, int32_t ol_number>
 */

union tpcc_order_line_key_t {
  int64_t ol_id;
  itemkey_t item_key;

  tpcc_order_line_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tpcc_order_line_key_t) == sizeof(itemkey_t), "");

enum tpcc_order_line_val_bitmap : int {
  ol_i_id = 0,
  ol_supply_w_id,
  ol_quantity,
  ol_amount,
  ol_delivery_d,
  ol_dist_info
};

struct tpcc_order_line_val_t {
  static const int MIN_OL_CNT = 5;
  static const int MAX_OL_CNT = 15;

  int32_t ol_i_id;
  int32_t ol_supply_w_id;
  int32_t ol_quantity;
  float ol_amount;
  uint32_t ol_delivery_d;
  char ol_dist_info[DIST + 1];
  int64_t debug_magic;
  // 6 attributes
} __attribute__((packed));

constexpr size_t tpcc_order_line_val_t_size = sizeof(tpcc_order_line_val_t);

// static_assert(sizeof(tpcc_order_line_val_t) == 56, "");  // add debug magic

/*
 * Item table
 * Primary key: <int32_t i_id>
 */

union tpcc_item_key_t {
  struct {
    int64_t i_id;
  };
  itemkey_t item_key;

  tpcc_item_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tpcc_item_key_t) == sizeof(itemkey_t), "");

enum tpcc_item_val_bitmap : int {
  i_im_id = 0,
  i_price,
  i_name,
  i_data
};

struct tpcc_item_val_t {
  static const int MIN_NAME = 14;  // I_NAME random a-string [14 .. 24]
  static const int MAX_NAME = 24;
  static const int MIN_DATA = 26;
  static const int MAX_DATA = 50;  // I_DATA random a-string [26 .. 50]

  static const int MIN_IM = 1;
  static const int MAX_IM = 10000;

  int32_t i_im_id;
  float i_price;
  char i_name[MAX_NAME + 1];
  char i_data[MAX_DATA + 1];
  int64_t debug_magic;
  // 4 attributes
} __attribute__((packed));

constexpr size_t tpcc_item_val_t_size = sizeof(tpcc_item_val_t);

// static_assert(sizeof(tpcc_item_val_t) == 96, "");  // add debug magic

/*
 * Stock table
 * Primary key: <int32_t s_i_id, int32_t s_w_id>
 */

union tpcc_stock_key_t {
  struct {
    int64_t s_id;
  };
  itemkey_t item_key;

  tpcc_stock_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tpcc_stock_key_t) == sizeof(itemkey_t), "");

enum tpcc_stock_val_bitmap : int {
  s_quantity = 0,
  s_ytd,
  s_order_cnt,
  s_remote_cnt,
  s_dist,
  s_data
};

struct tpcc_stock_val_t {
  static const int MIN_DATA = 26;
  static const int MAX_DATA = 50;
  static const int32_t MIN_STOCK_LEVEL_THRESHOLD = 10;
  static const int32_t MAX_STOCK_LEVEL_THRESHOLD = 20;
  static const int STOCK_LEVEL_ORDERS = 20;

  int32_t s_quantity;
  int32_t s_ytd;
  int32_t s_order_cnt;
  int32_t s_remote_cnt;
  char s_dist[NUM_DISTRICT_PER_WAREHOUSE][DIST + 1];
  char s_data[MAX_DATA + 1];
  int64_t debug_magic;
  // 6 attributes
} __attribute__((packed));

constexpr size_t tpcc_stock_val_t_size = sizeof(tpcc_stock_val_t);

// static_assert(sizeof(tpcc_stock_val_t) == 328, "");  // add debug magic

#if 0
// TPCC schema references: https://github.com/evanj/tpccbench
struct Warehouse {
    static constexpr float MIN_TAX = 0;
    static constexpr float MAX_TAX = 0.2000f;
    static constexpr float INITIAL_YTD = 300000.00f;
    static const int MIN_NAME = 6;
    static const int MAX_NAME = 10;
    // TPC-C 1.3.1 (page 11) requires 2*W. This permits testing up to 50 warehouses. This is an
    // arbitrary limit created to pack ids into integers.
    static const int MAX_WAREHOUSE_ID = 100;

    int32_t w_id;
    float w_tax;
    float w_ytd;
    char w_name[MAX_NAME + 1];
    char w_street_1[Address::MAX_STREET + 1];
    char w_street_2[Address::MAX_STREET + 1];
    char w_city[Address::MAX_CITY + 1];
    char w_state[Address::STATE + 1];
    char w_zip[Address::ZIP + 1];
};

struct District {
    static constexpr float MIN_TAX = 0;
    static constexpr float MAX_TAX = 0.2000f;
    static constexpr float INITIAL_YTD = 30000.00;  // different from Warehouse
    static const int INITIAL_NEXT_O_ID = 3001;
    static const int MIN_NAME = 6;
    static const int MAX_NAME = 10;
    static const int NUM_PER_WAREHOUSE = 10;

    int32_t d_id;
    int32_t d_w_id;
    float d_tax;
    float d_ytd;
    int32_t d_next_o_id;
    char d_name[MAX_NAME + 1];
    char d_street_1[Address::MAX_STREET + 1];
    char d_street_2[Address::MAX_STREET + 1];
    char d_city[Address::MAX_CITY + 1];
    char d_state[Address::STATE + 1];
    char d_zip[Address::ZIP + 1];
};

struct Customer {
    static constexpr float INITIAL_CREDIT_LIM = 50000.00;
    static constexpr float MIN_DISCOUNT = 0.0000;
    static constexpr float MAX_DISCOUNT = 0.5000;
    static constexpr float INITIAL_BALANCE = -10.00;
    static constexpr float INITIAL_YTD_PAYMENT = 10.00;
    static const int INITIAL_PAYMENT_CNT = 1;
    static const int INITIAL_DELIVERY_CNT = 0;
    static const int MIN_FIRST = 6;
    static const int MAX_FIRST = 10;
    static const int MIDDLE = 2;
    static const int MAX_LAST = 16;
    static const int PHONE = 16;
    static const int CREDIT = 2;
    static const int MIN_DATA = 300;
    static const int MAX_DATA = 500;
    static const int NUM_PER_DISTRICT = 3000;
    static const char GOOD_CREDIT[];
    static const char BAD_CREDIT[];

    int32_t c_id;
    int32_t c_d_id;
    int32_t c_w_id;
    float c_credit_lim;
    float c_discount;
    float c_balance;
    float c_ytd_payment;
    int32_t c_payment_cnt;
    int32_t c_delivery_cnt;
    char c_first[MAX_FIRST + 1];
    char c_middle[MIDDLE + 1];
    char c_last[MAX_LAST + 1];
    char c_street_1[Address::MAX_STREET + 1];
    char c_street_2[Address::MAX_STREET + 1];
    char c_city[Address::MAX_CITY + 1];
    char c_state[Address::STATE + 1];
    char c_zip[Address::ZIP + 1];
    char c_phone[PHONE + 1];
    char c_since[DATETIME_SIZE + 1];
    char c_credit[CREDIT + 1];
    char c_data[MAX_DATA + 1];
};

struct History {
    static const int MIN_DATA = 12;
    static const int MAX_DATA = 24;
    static constexpr float INITIAL_AMOUNT = 10.00f;

    int32_t h_c_id;
    int32_t h_c_d_id;
    int32_t h_c_w_id;
    int32_t h_d_id;
    int32_t h_w_id;
    float h_amount;
    char h_date[DATETIME_SIZE + 1];
    char h_data[MAX_DATA + 1];
};


struct NewOrder {
    static const int INITIAL_NUM_PER_DISTRICT = 900;

    int32_t no_w_id;
    int32_t no_d_id;
    int32_t no_o_id;
};

struct Order {
    static const int MIN_CARRIER_ID = 1;
    static const int MAX_CARRIER_ID = 10;
    // HACK: This is not strictly correct, but it works
    static const int NULL_CARRIER_ID = 0;
    // Less than this value, carrier != null, >= -> carrier == null
    static const int NULL_CARRIER_LOWER_BOUND = 2101;
    static const int MIN_OL_CNT = 5;
    static const int MAX_OL_CNT = 15;
    static const int INITIAL_ALL_LOCAL = 1;
    static const int INITIAL_ORDERS_PER_DISTRICT = 3000;
    // See TPC-C 1.3.1 (page 15)
    static const int MAX_ORDER_ID = 10000000;

    int32_t o_id;
    int32_t o_c_id;
    int32_t o_d_id;
    int32_t o_w_id;
    int32_t o_carrier_id;
    int32_t o_ol_cnt;
    int32_t o_all_local;
    char o_entry_d[DATETIME_SIZE+1];
};

struct OrderLine {
    static const int MIN_I_ID = 1;
    static const int MAX_I_ID = 100000;  // Item::NUM_ITEMS
    static const int INITIAL_QUANTITY = 5;
    static constexpr float MIN_AMOUNT = 0.01f;
    static constexpr float MAX_AMOUNT = 9999.99f;
    // new order has 10/1000 probability of selecting a remote warehouse for ol_supply_w_id
    static const int REMOTE_PROBABILITY_MILLIS = 10;

    int32_t ol_o_id;
    int32_t ol_d_id;
    int32_t ol_w_id;
    int32_t ol_number;
    int32_t ol_i_id;
    int32_t ol_supply_w_id;
    int32_t ol_quantity;
    float ol_amount;
    char ol_delivery_d[DATETIME_SIZE+1];
    char ol_dist_info[Stock::DIST+1];
};

struct Item {
    static const int MIN_IM = 1;
    static const int MAX_IM = 10000;
    static constexpr float MIN_PRICE = 1.00;
    static constexpr float MAX_PRICE = 100.00;
    static const int MIN_NAME = 14;
    static const int MAX_NAME = 24;
    static const int MIN_DATA = 26;
    static const int MAX_DATA = 50;
    static const int NUM_ITEMS = 100000;

    int32_t i_id;
    int32_t i_im_id;
    float i_price;
    char i_name[MAX_NAME+1];
    char i_data[MAX_DATA+1];
};

struct Stock {
    static const int MIN_QUANTITY = 10;
    static const int MAX_QUANTITY = 100;
    static const int DIST = 24;
    static const int MIN_DATA = 26;
    static const int MAX_DATA = 50;
    static const int NUM_STOCK_PER_WAREHOUSE = 100000;

    int32_t s_i_id;
    int32_t s_w_id;
    int32_t s_quantity;
    int32_t s_ytd;
    int32_t s_order_cnt;
    int32_t s_remote_cnt;
    char s_dist[District::NUM_PER_WAREHOUSE][DIST+1];
    char s_data[MAX_DATA+1];
};
#endif

/******************** TPCC table definitions (Schemas of key and value) end **********************/

// Magic numbers for debugging. These are unused in the spec.
const std::string tpcc_zip_magic("123456789");  // warehouse, district
const uint32_t tpcc_no_time_magic = 0;          // customer, history, order
const int64_t tpcc_add_magic = 818;             // customer_index, order_index, new_order, order_line, item, stock

#define TATP_MAGIC 97 /* Some magic number <= 255 */
#define tatp_sub_msc_location_magic (TATP_MAGIC)
#define tatp_sec_sub_magic (TATP_MAGIC + 1)
#define tatp_accinf_data1_magic (TATP_MAGIC + 2)
#define tatp_specfac_data_b0_magic (TATP_MAGIC + 3)
#define tatp_callfwd_numberx0_magic (TATP_MAGIC + 4)

/* STORED PROCEDURE EXECUTION FREQUENCIES (0-100) */
#define FREQUENCY_NEW_ORDER 45
#define FREQUENCY_PAYMENT 43
#define FREQUENCY_ORDER_STATUS 4
#define FREQUENCY_DELIVERY 4
#define FREQUENCY_STOCK_LEVEL 4

// Transaction workload type
#define TPCC_TX_TYPES 5
enum class TPCCTxType {
  kNewOrder = 0,
  kPayment,
  kDelivery,
  kOrderStatus,
  kStockLevel,
};

const std::string TPCC_TX_NAME[TPCC_TX_TYPES] = {"NewOrder", "Payment", "Delivery",
                                                 "OrderStatus", "StockLevel"};

// Table id
enum TPCCTableType : uint64_t {
  kWarehouseTable = TABLE_TPCC,
  kDistrictTable,
  kCustomerTable,
  kHistoryTable,
  kNewOrderTable,
  kOrderTable,
  kOrderLineTable,
  kItemTable,
  kStockTable,
  kCustomerIndexTable,
  kOrderIndexTable,
};
const int TPCC_TOTAL_TABLES = 11;

// Author: Ming Zhang
// Copyright (c) 2023

#pragma once

#include <string>

#include "base/common.h"
#include "config/table_type.h"

/*
 * Up to 1 billion subscribers so that FastGetSubscribeNumFromSubscribeID() requires
 * only 3 modulo operations.
 */
#define TATP_MAX_SUBSCRIBERS 1000000000

/* STORED PROCEDURE EXECUTION FREQUENCIES (0-100) */
#define FREQUENCY_GET_SUBSCRIBER_DATA 35    // Single
#define FREQUENCY_GET_ACCESS_DATA 35        // Single
#define FREQUENCY_GET_NEW_DESTINATION 10    // Multi
#define FREQUENCY_UPDATE_SUBSCRIBER_DATA 2  // Single
#define FREQUENCY_UPDATE_LOCATION 14        // Multi
#define FREQUENCY_INSERT_CALL_FORWARDING 2  // Multi
#define FREQUENCY_DELETE_CALL_FORWARDING 2  // Multi

/******************** TATP table definitions (Schemas of key and value) start **********************/
// All keys have been sized to 8 bytes
// All values have been sized to the next multiple of 8 bytes

/* A 64-bit encoding for 15-character decimal strings. */
union tatp_sub_number_t {
  struct {
    uint32_t dec_0 : 4;
    uint32_t dec_1 : 4;
    uint32_t dec_2 : 4;
    uint32_t dec_3 : 4;
    uint32_t dec_4 : 4;
    uint32_t dec_5 : 4;
    uint32_t dec_6 : 4;
    uint32_t dec_7 : 4;
    uint32_t dec_8 : 4;
    uint32_t dec_9 : 4;
    uint32_t dec_10 : 4;
    uint32_t dec_11 : 4;
    uint32_t dec_12 : 4;
    uint32_t dec_13 : 4;
    uint32_t dec_14 : 4;
    uint32_t dec_15 : 4;
  };

  struct {
    uint64_t dec_0_1_2 : 12;
    uint64_t dec_3_4_5 : 12;
    uint64_t dec_6_7_8 : 12;
    uint64_t dec_9_10_11 : 12;
    uint64_t unused : 16;
  };

  itemkey_t item_key;
};
static_assert(sizeof(tatp_sub_number_t) == sizeof(itemkey_t), "");

/*
 * SUBSCRIBER table
 * Primary key: <uint32_t s_id>
 * Value size: 40 bytes. Full value read in GET_SUBSCRIBER_DATA.
 */
union tatp_sub_key_t {
  struct {
    uint32_t s_id;
    uint8_t unused[4];
  };
  itemkey_t item_key;

  tatp_sub_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tatp_sub_key_t) == sizeof(itemkey_t), "");

enum tatp_sub_val_bitmap : int {
  sub_number = 0,
  sub_number_unused,
  hex,
  bytes,
  bits,
  msc_location,
  vlr_location
};

struct tatp_sub_val_t {
  tatp_sub_number_t sub_number;
  char sub_number_unused[7]; /* sub_number should be 15 bytes. We used 8 above. */
  char hex[5];
  char bytes[10];
  short bits;
  uint32_t msc_location;
  uint32_t vlr_location;
  // 7 attributes
} __attribute__((packed));

constexpr size_t tatp_sub_val_t_size = sizeof(tatp_sub_val_t);

// static_assert(sizeof(tatp_sub_val_t) == 40, "");

/*
 * Secondary SUBSCRIBER table
 * Key: <tatp_sub_number_t>
 * Value size: 8 bytes
 */
union tatp_sec_sub_key_t {
  tatp_sub_number_t sub_number;
  itemkey_t item_key;

  tatp_sec_sub_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tatp_sec_sub_key_t) == sizeof(itemkey_t), "");

enum tatp_sec_sub_val_bitmap : int {
  s_id = 0
};

struct tatp_sec_sub_val_t {
  uint32_t s_id;
  uint8_t magic;
  uint8_t unused[3];
  // 1 attributes
} __attribute__((packed));

constexpr size_t tatp_sec_sub_val_t_size = sizeof(tatp_sec_sub_val_t);

// static_assert(sizeof(tatp_sec_sub_val_t) == 8, "");

/*
 * ACCESS INFO table
 * Primary key: <uint32_t s_id, uint8_t ai_type>
 * Value size: 16 bytes
 */
union tatp_accinf_key_t {
  struct {
    uint32_t s_id;
    uint8_t ai_type;
    uint8_t unused[3];
  };
  itemkey_t item_key;

  tatp_accinf_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tatp_accinf_key_t) == sizeof(itemkey_t), "");

enum tatp_accinf_val_bitmap : int {
  data1 = 0,
  data2,
  data3,
  data4
};

struct tatp_accinf_val_t {
  char data1;
  char data2;
  char data3[3];
  char data4[5];
  uint8_t unused[6];
  // 4 attributes
} __attribute__((packed));

constexpr size_t tatp_accinf_val_t_size = sizeof(tatp_accinf_val_t);

// static_assert(sizeof(tatp_accinf_val_t) == 16, "");

/*
 * SPECIAL FACILITY table
 * Primary key: <uint32_t s_id, uint8_t sf_type>
 * Value size: 8 bytes
 */
union tatp_specfac_key_t {
  struct {
    uint32_t s_id;
    uint8_t sf_type;
    uint8_t unused[3];
  };
  itemkey_t item_key;

  tatp_specfac_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tatp_specfac_key_t) == sizeof(itemkey_t), "");

enum tatp_specfac_val_bitmap : int {
  is_active = 0,
  error_cntl,
  data_a,
  data_b
};

struct tatp_specfac_val_t {
  char is_active;
  char error_cntl;
  char data_a;
  char data_b[5];
  // 4 attributes
} __attribute__((packed));

constexpr size_t tatp_specfac_val_t_size = sizeof(tatp_specfac_val_t);

// static_assert(sizeof(tatp_specfac_val_t) == 8, "");

/*
 * CALL FORWARDING table
 * Primary key: <uint32_t s_id, uint8_t sf_type, uint8_t start_time>
 * Value size: 16 bytes
 */
union tatp_callfwd_key_t {
  struct {
    uint32_t s_id;
    uint8_t sf_type;
    uint8_t start_time;
    uint8_t unused[2];
  };
  itemkey_t item_key;

  tatp_callfwd_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tatp_callfwd_key_t) == sizeof(itemkey_t), "");

enum tatp_callfwd_val_bitmap : int {
  end_time = 0,
  numberx
};

struct tatp_callfwd_val_t {
  uint8_t end_time;
  char numberx[15];
  // 2 attributes
} __attribute__((packed));

constexpr size_t tatp_callfwd_val_t_size = sizeof(tatp_callfwd_val_t);

// static_assert(sizeof(tatp_callfwd_val_t) == 16, "");

/******************** TATP table definitions (Schemas of key and value) end **********************/

// Magic numbers for debugging. These are unused in the spec.
#define TATP_MAGIC 97 /* Some magic number <= 255 */
#define tatp_sub_msc_location_magic (TATP_MAGIC)
#define tatp_sec_sub_magic (TATP_MAGIC + 1)
#define tatp_accinf_data1_magic (TATP_MAGIC + 2)
#define tatp_specfac_data_b0_magic (TATP_MAGIC + 3)
#define tatp_callfwd_numberx0_magic (TATP_MAGIC + 4)

// Transaction workload type
#define TATP_TX_TYPES 7
enum class TATPTxType : uint64_t {
  kGetSubsciberData = 0,
  kGetAccessData,
  kGetNewDestination,
  kUpdateSubscriberData,
  kUpdateLocation,
  kInsertCallForwarding,
  kDeleteCallForwarding,
};

const std::string TATP_TX_NAME[TATP_TX_TYPES] = {"GetSubsciberData", "GetAccessData", "GetNewDestination",
                                                 "UpdateSubscriberData", "UpdateLocation", "InsertCallForwarding", "DeleteCallForwarding"};

// Table id
enum TATPTableType : uint64_t {
  kSubscriberTable = TABLE_TATP,
  kSecSubscriberTable,
  kSpecialFacilityTable,
  kAccessInfoTable,
  kCallForwardingTable,
};
const int TATP_TOTAL_TABLES = 5;

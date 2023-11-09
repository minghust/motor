// Author: Ming Zhang
// Copyright (c) 2023

#pragma once

#include <cstddef>  // For size_t
#include <cstdint>  // For uintxx_t

#include "flags.h"

// Global specification
using tx_id_t = uint64_t;     // Transaction id type
using t_id_t = uint32_t;      // Thread id type
using coro_id_t = int;        // Coroutine id type
using node_id_t = int;        // Machine id type
using mr_id_t = int;          // Memory region id type
using table_id_t = uint64_t;  // Table id type
using itemkey_t = uint64_t;   // Data item key type, used in DB tables
using offset_t = int64_t;     // Offset type. Usually used in remote offset for RDMA
using version_t = uint64_t;   // Version type, used in version checking
using lock_t = uint64_t;      // Lock type, used in remote locking
using anchor_t = uint8_t;     // Anchor type, used in update
using valid_t = uint8_t;      // Valid type
using in_offset_t = int16_t;  // Internel offset type

// Memory region ids for server's hash store buffer
const mr_id_t SERVER_HASH_BUFF_ID = 97;

// Memory region ids for client's local_mr
const mr_id_t CLIENT_MR_ID = 100;

// Indicating that memory store metas have been transmitted
const uint64_t MEM_STORE_META_END = 0xE0FF0E0F;

#define NO_POS -1  // No position for reading a proper versioned data
#define NOT_FOUND -2
#define UN_INIT_POS -3
#define NO_WALK -4
#define SLOT_NOT_FOUND -5
#define FOUND 1

// Data state
#define STATE_LOCKED 1  // Data cannot be written. Used for serializing transactions
#define STATE_UNLOCKED 0
#define STATE_INVALID 0
#define STATE_VALID 1

// Alias
#define Aligned8 __attribute__((aligned(8)))
#define PACKED __attribute__((packed))
#define ALWAYS_INLINE inline __attribute__((always_inline))
#define TID (std::this_thread::get_id())

// Helpful for improving condition prediction hit rate
#define unlikely(x) __builtin_expect(!!(x), 0)
#define likely(x) __builtin_expect(!!(x), 1)

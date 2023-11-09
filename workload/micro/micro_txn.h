// Author: Ming Zhang
// Copyright (c) 2023

#pragma once

#include <memory>

#include "micro/micro_db.h"
#include "process/txn.h"
#include "util/zipf.h"

/******************** The business logic (Transaction) start ********************/

bool TxReadOne(coro_yield_t& yield,
               tx_id_t tx_id,
               TXN* txn,
               itemkey_t key);

bool TxUpdateOne(coro_yield_t& yield,
                 tx_id_t tx_id,
                 TXN* txn,
                 itemkey_t key);

bool TxRWOne(ZipfGen* zipf_gen,
             uint64_t* seed,
             coro_yield_t& yield,
             tx_id_t tx_id,
             TXN* txn,
             bool is_skewed,
             uint64_t data_set_size,
             uint64_t num_keys_global,
             uint64_t write_ratio);

bool TxTest1(ZipfGen* zipf_gen,
             uint64_t* seed,
             coro_yield_t& yield,
             tx_id_t tx_id,
             TXN* txn,
             bool is_skewed,
             uint64_t data_set_size,
             uint64_t num_keys_global,
             uint64_t write_ratio);

bool TxTest2(ZipfGen* zipf_gen,
             uint64_t* seed,
             coro_yield_t& yield,
             tx_id_t tx_id,
             TXN* txn,
             bool is_skewed,
             uint64_t data_set_size,
             uint64_t num_keys_global,
             uint64_t write_ratio);

bool TxTest3(ZipfGen* zipf_gen,
             uint64_t* seed,
             coro_yield_t& yield,
             tx_id_t tx_id,
             TXN* txn,
             bool is_skewed,
             uint64_t data_set_size,
             uint64_t num_keys_global,
             uint64_t write_ratio);

bool TxTest4(ZipfGen* zipf_gen,
             uint64_t* seed,
             coro_yield_t& yield,
             tx_id_t tx_id,
             TXN* txn,
             bool is_skewed,
             uint64_t data_set_size,
             uint64_t num_keys_global,
             uint64_t write_ratio);

bool TxTest5(ZipfGen* zipf_gen,
             uint64_t* seed,
             coro_yield_t& yield,
             tx_id_t tx_id,
             TXN* txn,
             bool is_skewed,
             uint64_t data_set_size,
             uint64_t num_keys_global,
             uint64_t write_ratio);

bool TxTest6(ZipfGen* zipf_gen,
             uint64_t* seed,
             coro_yield_t& yield,
             tx_id_t tx_id,
             TXN* txn,
             bool is_skewed,
             uint64_t data_set_size,
             uint64_t num_keys_global,
             uint64_t write_ratio);

bool TxTest7(ZipfGen* zipf_gen,
             uint64_t* seed,
             coro_yield_t& yield,
             tx_id_t tx_id,
             TXN* txn,
             bool is_skewed,
             uint64_t data_set_size,
             uint64_t num_keys_global,
             uint64_t write_ratio);

bool TxTest8(ZipfGen* zipf_gen,
             uint64_t* seed,
             coro_yield_t& yield,
             tx_id_t tx_id,
             TXN* txn,
             bool is_skewed,
             uint64_t data_set_size,
             uint64_t num_keys_global,
             uint64_t write_ratio);

bool TxTest9(ZipfGen* zipf_gen,
             uint64_t* seed,
             coro_yield_t& yield,
             tx_id_t tx_id,
             TXN* txn,
             bool is_skewed,
             uint64_t data_set_size,
             uint64_t num_keys_global,
             uint64_t write_ratio);

bool TxTest10(ZipfGen* zipf_gen,
              uint64_t* seed,
              coro_yield_t& yield,
              tx_id_t tx_id,
              TXN* txn,
              bool is_skewed,
              uint64_t data_set_size,
              uint64_t num_keys_global,
              uint64_t write_ratio);

bool TxTest11(ZipfGen* zipf_gen,
              uint64_t* seed,
              coro_yield_t& yield,
              tx_id_t tx_id,
              TXN* txn,
              bool is_skewed,
              uint64_t data_set_size,
              uint64_t num_keys_global,
              uint64_t write_ratio);

bool TxTest12(ZipfGen* zipf_gen,
              uint64_t* seed,
              coro_yield_t& yield,
              tx_id_t tx_id,
              TXN* txn,
              bool is_skewed,
              uint64_t data_set_size,
              uint64_t num_keys_global,
              uint64_t write_ratio);

bool TxTest100(ZipfGen* zipf_gen,
               uint64_t* seed,
               coro_yield_t& yield,
               tx_id_t tx_id,
               TXN* txn,
               bool is_skewed,
               uint64_t data_set_size,
               uint64_t num_keys_global,
               uint64_t write_ratio);

bool TxTest101(ZipfGen* zipf_gen,
               uint64_t* seed,
               coro_yield_t& yield,
               tx_id_t tx_id,
               TXN* txn,
               bool is_skewed,
               uint64_t data_set_size,
               uint64_t num_keys_global,
               uint64_t write_ratio);

/******************** The business logic (Transaction) end ********************/
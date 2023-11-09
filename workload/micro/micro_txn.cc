// Author: Ming Zhang
// Copyright (c) 2023

#include "micro/micro_txn.h"

#include <set>

bool TxReadOne(coro_yield_t& yield,
               tx_id_t tx_id,
               TXN* txn,
               itemkey_t key) {
  txn->Begin(tx_id, TXN_TYPE::kROTxn);

  micro_key_t micro_key;
  micro_key.item_key = key;

  DataSetItemPtr micro_record = std::make_shared<DataSetItem>((table_id_t)MicroTableType::kMicroTable,
                                                              micro_val_t_size,
                                                              micro_key.item_key,
                                                              UserOP::kRead);
  txn->AddToReadOnlySet(micro_record);

  if (!txn->Execute(yield)) {
    return false;
  }

  micro_val_t* micro_val = (micro_val_t*)micro_record->Value();
  if (micro_val->d1 != micro_magic + 1) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << txn->t_id << "-" << txn->coro_id << "-" << tx_id;
  }

  bool commit_status = txn->Commit(yield);
  return commit_status;
}

bool TxUpdateOne(coro_yield_t& yield,
                 tx_id_t tx_id,
                 TXN* txn,
                 itemkey_t key) {
  txn->Begin(tx_id, TXN_TYPE::kRWTxn);

  micro_key_t micro_key;
  micro_key.item_key = key;

  DataSetItemPtr micro_record = std::make_shared<DataSetItem>((table_id_t)MicroTableType::kMicroTable,
                                                              micro_val_t_size,
                                                              micro_key.item_key,
                                                              UserOP::kUpdate);
  txn->AddToReadWriteSet(micro_record);

  if (!txn->Execute(yield)) {
    return false;
  }

  micro_val_t* micro_val = (micro_val_t*)micro_record->Value();
  if (micro_val->d1 != micro_magic + 1) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << txn->t_id << "-" << txn->coro_id << "-" << tx_id;
  }

  micro_record->SetUpdate(micro_val_bitmap::d2, &micro_val->d2, sizeof(micro_val->d2));
  micro_val->d2 = micro_magic * 2;

  bool commit_status = txn->Commit(yield);
  return commit_status;
}

bool TxRWOne(ZipfGen* zipf_gen,
             uint64_t* seed,
             coro_yield_t& yield,
             tx_id_t tx_id,
             TXN* txn,
             bool is_skewed,
             uint64_t data_set_size,
             uint64_t num_keys_global,
             uint64_t write_ratio) {
  txn->Begin(tx_id, TXN_TYPE::kRWTxn);
  bool is_write[data_set_size];
  DataSetItemPtr micro_records[data_set_size];

  for (uint64_t i = 0; i < data_set_size; i++) {
    micro_key_t micro_key;
    if (is_skewed) {
      // Skewed distribution
      micro_key.micro_id = (itemkey_t)(zipf_gen->next());
    } else {
      // Uniformed distribution
      micro_key.micro_id = (itemkey_t)FastRand(seed) & (num_keys_global - 1);
    }

    assert(micro_key.item_key >= 0 && micro_key.item_key < num_keys_global);

    if (FastRand(seed) % 100 < write_ratio) {
      micro_records[i] = std::make_shared<DataSetItem>((table_id_t)MicroTableType::kMicroTable,
                                                       micro_val_t_size,
                                                       micro_key.item_key,
                                                       UserOP::kUpdate);
      txn->AddToReadWriteSet(micro_records[i]);
      is_write[i] = true;
    } else {
      micro_records[i] = std::make_shared<DataSetItem>((table_id_t)MicroTableType::kMicroTable,
                                                       micro_val_t_size,
                                                       micro_key.item_key,
                                                       UserOP::kRead);
      txn->AddToReadOnlySet(micro_records[i]);
      is_write[i] = false;
    }
  }

  if (!txn->Execute(yield)) {
    return false;
  }

  for (uint64_t i = 0; i < data_set_size; i++) {
    micro_val_t* micro_val = (micro_val_t*)micro_records[i]->Value();
    if (micro_val->d1 != micro_magic + 1) {
      RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << txn->t_id << "-" << txn->coro_id << "-" << tx_id;
    }
    if (is_write[i]) {
      micro_records[i]->SetUpdate(micro_val_bitmap::d2, &micro_val->d2, sizeof(micro_val->d2));
      micro_val->d2 = micro_magic * 2;
    }
  }

  bool commit_status = txn->Commit(yield);
  return commit_status;
}

/******************** The business logic (Transaction) start ********************/
bool TxTest100(ZipfGen* zipf_gen,
               uint64_t* seed,
               coro_yield_t& yield,
               tx_id_t tx_id,
               TXN* txn,
               bool is_skewed,
               uint64_t data_set_size,
               uint64_t num_keys_global,
               uint64_t write_ratio) {
  txn->Begin(tx_id, TXN_TYPE::kRWTxn);

  micro_key_t micro_key;
  micro_key.micro_id = 10;

  auto micro_record = std::make_shared<DataSetItem>((table_id_t)MicroTableType::kMicroTable,
                                                    micro_val_t_size,
                                                    micro_key.item_key,
                                                    UserOP::kUpdate);
  txn->AddToReadWriteSet(micro_record);

  if (!txn->Execute(yield)) {
    return false;
  }

  micro_val_t* micro_val = (micro_val_t*)micro_record->Value();

  if (tx_id == 3) {
    micro_record->SetUpdate(micro_val_bitmap::d1, &micro_val->d1, sizeof(micro_val->d1));
    micro_val->d1 = micro_magic * 2 + 1;
    RDMA_LOG(INFO) << "tx " << tx_id << " updates d1 size " << sizeof(micro_val->d1);
  } else if (tx_id == 6) {
    micro_record->SetUpdate(micro_val_bitmap::d2, &micro_val->d2, sizeof(micro_val->d2));
    micro_val->d2 = micro_magic * 2 + 2;
    RDMA_LOG(INFO) << "tx " << tx_id << " updates d2 size " << sizeof(micro_val->d2);
  } else if (tx_id == 9) {
    micro_record->SetUpdate(micro_val_bitmap::d3, &micro_val->d3, sizeof(micro_val->d3));
    micro_val->d3 = micro_magic * 2 + 3;
    RDMA_LOG(INFO) << "tx " << tx_id << " updates d3 size " << sizeof(micro_val->d3);
  } else if (tx_id == 12) {
    micro_record->SetUpdate(micro_val_bitmap::d4, &micro_val->d4, sizeof(micro_val->d4));
    micro_val->d4 = micro_magic * 2 + 4;
    RDMA_LOG(INFO) << "tx " << tx_id << " updates d4 size " << sizeof(micro_val->d4);
  } else if (tx_id == 15) {
    micro_record->SetUpdate(micro_val_bitmap::d5, &micro_val->d5, sizeof(micro_val->d5));
    micro_val->d5 = micro_magic * 2 + 5;
    RDMA_LOG(INFO) << "tx " << tx_id << " updates d5 size " << sizeof(micro_val->d5);
  } else if (tx_id == 18) {
    micro_record->SetUpdate(micro_val_bitmap::d2, &micro_val->d2, sizeof(micro_val->d2));
    micro_val->d2 = micro_magic * 2 + 6;
    RDMA_LOG(INFO) << "tx " << tx_id << " updates d2 size " << sizeof(micro_val->d2);
  } else if (tx_id == 21) {
    micro_record->SetUpdate(micro_val_bitmap::d3, &micro_val->d3, sizeof(micro_val->d3));
    micro_val->d3 = micro_magic * 2 + 7;
    RDMA_LOG(INFO) << "tx " << tx_id << " updates d3 size " << sizeof(micro_val->d3);
  } else if (tx_id == 24) {
    micro_record->SetUpdate(micro_val_bitmap::d5, &micro_val->d5, sizeof(micro_val->d5));
    micro_val->d5 = micro_magic * 2 + 8;
    RDMA_LOG(INFO) << "tx " << tx_id << " updates d5 size " << sizeof(micro_val->d5);
  } else if (tx_id == 27) {
    micro_record->SetUpdate(micro_val_bitmap::d1, &micro_val->d1, sizeof(micro_val->d1));
    micro_val->d1 = micro_magic * 2 + 9;
    RDMA_LOG(INFO) << "tx " << tx_id << " updates d1 size " << sizeof(micro_val->d1);
  } else if (tx_id == 30) {
    micro_record->SetUpdate(micro_val_bitmap::d2, &micro_val->d2, sizeof(micro_val->d2));
    micro_val->d2 = micro_magic * 2 + 10;
    RDMA_LOG(INFO) << "tx " << tx_id << " updates d2 size " << sizeof(micro_val->d2);
  }

  bool commit_status = txn->Commit(yield);
  return commit_status;
}

bool TxTest101(ZipfGen* zipf_gen,
               uint64_t* seed,
               coro_yield_t& yield,
               tx_id_t tx_id,
               TXN* txn,
               bool is_skewed,
               uint64_t data_set_size,
               uint64_t num_keys_global,
               uint64_t write_ratio) {
  if (tx_id == 33) {
    tx_id = 20;
  }
  if (tx_id == 34) {
    tx_id = 23;
  }
  if (tx_id == 35) {
    tx_id = 26;
  }
  if (tx_id == 36) {
    tx_id = 29;
  }
  if (tx_id == 37) {
    tx_id = 40;
  }
  txn->Begin(tx_id, TXN_TYPE::kROTxn);

  micro_key_t micro_key;
  micro_key.micro_id = 10;

  auto micro_record = std::make_shared<DataSetItem>((table_id_t)MicroTableType::kMicroTable,
                                                    micro_val_t_size,
                                                    micro_key.item_key,
                                                    UserOP::kRead);
  txn->AddToReadOnlySet(micro_record);

  if (!txn->Execute(yield)) {
    return false;
  }

  micro_val_t* micro_val = (micro_val_t*)micro_record->Value();

  std::cout << "txid: " << tx_id
            << " read d1: " << (int)micro_val->d1
            << " d2: " << (int)micro_val->d2
            << " d3: " << (int)micro_val->d3
            << " d4: " << (int)micro_val->d4
            << " d5: " << (int)micro_val->d5 << std::endl;

  bool commit_status = txn->Commit(yield);
  return commit_status;
}

// updates d2
bool TxTest1(ZipfGen* zipf_gen,
             uint64_t* seed,
             coro_yield_t& yield,
             tx_id_t tx_id,
             TXN* txn,
             bool is_skewed,
             uint64_t data_set_size,
             uint64_t num_keys_global,
             uint64_t write_ratio) {
  txn->Begin(tx_id, TXN_TYPE::kRWTxn);

  micro_key_t micro_key;
  micro_key.micro_id = 10;

  assert(micro_key.item_key >= 0 && micro_key.item_key < num_keys_global);

  auto micro_record = std::make_shared<DataSetItem>((table_id_t)MicroTableType::kMicroTable,
                                                    micro_val_t_size,
                                                    micro_key.item_key,
                                                    UserOP::kUpdate);
  txn->AddToReadWriteSet(micro_record);

  if (!txn->Execute(yield)) {
    return false;
  }

  micro_val_t* micro_val = (micro_val_t*)micro_record->Value();
  if (micro_val->d1 != micro_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << txn->t_id << "-" << txn->coro_id << "-" << tx_id;
  }

  RDMA_LOG(DBG) << "txid: " << tx_id
                << " read d1: " << micro_val->d1
                << " d2: " << micro_val->d2
                << " d3: " << micro_val->d3
                << " d4: " << micro_val->d4
                << " d5: " << micro_val->d5;

  micro_record->SetUpdate(micro_val_bitmap::d2, &micro_val->d2, sizeof(micro_val->d2));
  micro_val->d2 = micro_magic * 2;

  bool commit_status = txn->Commit(yield);
  return commit_status;
}

// updates d2 d3
bool TxTest2(ZipfGen* zipf_gen,
             uint64_t* seed,
             coro_yield_t& yield,
             tx_id_t tx_id,
             TXN* txn,
             bool is_skewed,
             uint64_t data_set_size,
             uint64_t num_keys_global,
             uint64_t write_ratio) {
  txn->Begin(tx_id, TXN_TYPE::kRWTxn);

  micro_key_t micro_key;
  micro_key.micro_id = 10;

  assert(micro_key.item_key >= 0 && micro_key.item_key < num_keys_global);

  auto micro_record = std::make_shared<DataSetItem>((table_id_t)MicroTableType::kMicroTable,
                                                    micro_val_t_size,
                                                    micro_key.item_key,
                                                    UserOP::kUpdate);
  txn->AddToReadWriteSet(micro_record);

  if (!txn->Execute(yield)) {
    return false;
  }

  micro_val_t* micro_val = (micro_val_t*)micro_record->Value();
  if (micro_val->d1 != micro_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << txn->t_id << "-" << txn->coro_id << "-" << tx_id;
  }

  RDMA_LOG(DBG) << "txid: " << tx_id
                << " read d1: " << micro_val->d1
                << " d2: " << micro_val->d2
                << " d3: " << micro_val->d3
                << " d4: " << micro_val->d4
                << " d5: " << micro_val->d5;

  if (micro_val->d2 != micro_magic * 2) {
    RDMA_LOG(FATAL) << "micro_val->d2 error: " << micro_val->d2;
  }

  micro_record->SetUpdate(micro_val_bitmap::d2, &micro_val->d2, sizeof(micro_val->d2));
  micro_val->d2 = micro_magic * 3;

  micro_record->SetUpdate(micro_val_bitmap::d3, &micro_val->d3, sizeof(micro_val->d3));
  micro_val->d3 = micro_magic * 4;

  bool commit_status = txn->Commit(yield);
  return commit_status;
}

// updates d4
bool TxTest3(ZipfGen* zipf_gen,
             uint64_t* seed,
             coro_yield_t& yield,
             tx_id_t tx_id,
             TXN* txn,
             bool is_skewed,
             uint64_t data_set_size,
             uint64_t num_keys_global,
             uint64_t write_ratio) {
  txn->Begin(tx_id, TXN_TYPE::kRWTxn);

  micro_key_t micro_key;
  micro_key.micro_id = 10;

  assert(micro_key.item_key >= 0 && micro_key.item_key < num_keys_global);

  auto micro_record = std::make_shared<DataSetItem>((table_id_t)MicroTableType::kMicroTable,
                                                    micro_val_t_size,
                                                    micro_key.item_key,
                                                    UserOP::kUpdate);
  txn->AddToReadWriteSet(micro_record);

  if (!txn->Execute(yield)) {
    return false;
  }

  micro_val_t* micro_val = (micro_val_t*)micro_record->Value();
  if (micro_val->d1 != micro_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << txn->t_id << "-" << txn->coro_id << "-" << tx_id;
  }

  RDMA_LOG(DBG) << "txid: " << tx_id
                << " read d1: " << micro_val->d1
                << " d2: " << micro_val->d2
                << " d3: " << micro_val->d3
                << " d4: " << micro_val->d4
                << " d5: " << micro_val->d5;

  // read current vcell's d2 and d3
  if (micro_val->d2 != micro_magic * 3) {
    RDMA_LOG(FATAL) << "micro_val->d2 error: " << micro_val->d2;
  }

  if (micro_val->d3 != micro_magic * 4) {
    RDMA_LOG(FATAL) << "micro_val->d3 error: " << micro_val->d3;
  }

  micro_record->SetUpdate(micro_val_bitmap::d4, &micro_val->d4, sizeof(micro_val->d4));
  micro_val->d4 = micro_magic * 5;

  bool commit_status = txn->Commit(yield);
  return commit_status;
}

// read key=10
bool TxTest4(ZipfGen* zipf_gen,
             uint64_t* seed,
             coro_yield_t& yield,
             tx_id_t tx_id,
             TXN* txn,
             bool is_skewed,
             uint64_t data_set_size,
             uint64_t num_keys_global,
             uint64_t write_ratio) {
  txn->Begin(tx_id, TXN_TYPE::kROTxn);

  micro_key_t micro_key;
  micro_key.micro_id = 10;

  assert(micro_key.item_key >= 0 && micro_key.item_key < num_keys_global);

  auto micro_record = std::make_shared<DataSetItem>((table_id_t)MicroTableType::kMicroTable,
                                                    micro_val_t_size,
                                                    micro_key.item_key,
                                                    UserOP::kRead);
  txn->AddToReadOnlySet(micro_record);

  if (!txn->Execute(yield)) {
    return false;
  }

  micro_val_t* micro_val = (micro_val_t*)micro_record->Value();
  if (micro_val->d1 != micro_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << txn->t_id << "-" << txn->coro_id << "-" << tx_id;
  }

  RDMA_LOG(DBG) << "txid: " << tx_id
                << " read d1: " << micro_val->d1
                << " d2: " << micro_val->d2
                << " d3: " << micro_val->d3
                << " d4: " << micro_val->d4
                << " d5: " << micro_val->d5;

  // read previous vcell's d2 and d3
  if (micro_val->d2 != micro_magic * 3) {
    RDMA_LOG(FATAL) << "micro_val->d2 error: " << micro_val->d2;
  }

  if (micro_val->d3 != micro_magic * 4) {
    RDMA_LOG(FATAL) << "micro_val->d3 error: " << micro_val->d3;
  }

  // read current vcell's d4
  if (micro_val->d4 != micro_magic * 5) {
    RDMA_LOG(FATAL) << "micro_val->d4 error: " << micro_val->d4;
  }

  bool commit_status = txn->Commit(yield);
  return commit_status;
}

// insert key=20
bool TxTest5(ZipfGen* zipf_gen,
             uint64_t* seed,
             coro_yield_t& yield,
             tx_id_t tx_id,
             TXN* txn,
             bool is_skewed,
             uint64_t data_set_size,
             uint64_t num_keys_global,
             uint64_t write_ratio) {
  txn->Begin(tx_id, TXN_TYPE::kRWTxn);

  micro_key_t micro_key;
  micro_key.micro_id = 20;

  auto micro_record = std::make_shared<DataSetItem>((table_id_t)MicroTableType::kMicroTable,
                                                    sizeof(micro_val_t),  // User Insert
                                                    micro_key.item_key,
                                                    UserOP::kInsert);
  txn->AddToReadWriteSet(micro_record);

  if (!txn->Execute(yield)) {
    return false;
  }

  micro_val_t* micro_val = (micro_val_t*)micro_record->Value();

  micro_val->d1 = 1;
  micro_val->d2 = 2;
  micro_val->d3 = 3;
  micro_val->d4 = 4;
  micro_val->d5 = 5;

  bool commit_status = txn->Commit(yield);
  return commit_status;
}

// read key=20
bool TxTest6(ZipfGen* zipf_gen,
             uint64_t* seed,
             coro_yield_t& yield,
             tx_id_t tx_id,
             TXN* txn,
             bool is_skewed,
             uint64_t data_set_size,
             uint64_t num_keys_global,
             uint64_t write_ratio) {
  txn->Begin(tx_id, TXN_TYPE::kROTxn);

  micro_key_t micro_key;
  micro_key.micro_id = 20;

  auto micro_record = std::make_shared<DataSetItem>((table_id_t)MicroTableType::kMicroTable,
                                                    micro_val_t_size,
                                                    micro_key.item_key,
                                                    UserOP::kRead);
  txn->AddToReadOnlySet(micro_record);

  if (!txn->Execute(yield)) {
    return false;
  }

  micro_val_t* micro_val = (micro_val_t*)micro_record->Value();

  RDMA_LOG(DBG) << "txid: " << tx_id
                << " read d1: " << micro_val->d1
                << " d2: " << micro_val->d2
                << " d3: " << micro_val->d3
                << " d4: " << micro_val->d4
                << " d5: " << micro_val->d5;

  if (micro_val->d1 != 1 ||
      micro_val->d2 != 2 ||
      micro_val->d3 != 3 ||
      micro_val->d4 != 4 ||
      micro_val->d5 != 5) {
    RDMA_LOG(FATAL) << "READ value unmatches";
  }

  bool commit_status = txn->Commit(yield);
  return commit_status;
}

// updates key=20, d1 d5
bool TxTest7(ZipfGen* zipf_gen,
             uint64_t* seed,
             coro_yield_t& yield,
             tx_id_t tx_id,
             TXN* txn,
             bool is_skewed,
             uint64_t data_set_size,
             uint64_t num_keys_global,
             uint64_t write_ratio) {
  txn->Begin(tx_id, TXN_TYPE::kRWTxn);

  micro_key_t micro_key;
  micro_key.micro_id = 20;

  auto micro_record = std::make_shared<DataSetItem>((table_id_t)MicroTableType::kMicroTable,
                                                    micro_val_t_size,
                                                    micro_key.item_key,
                                                    UserOP::kUpdate);
  txn->AddToReadWriteSet(micro_record);

  if (!txn->Execute(yield)) {
    return false;
  }

  micro_val_t* micro_val = (micro_val_t*)micro_record->Value();

  RDMA_LOG(DBG) << "txid: " << tx_id
                << " read d1: " << micro_val->d1
                << " d2: " << micro_val->d2
                << " d3: " << micro_val->d3
                << " d4: " << micro_val->d4
                << " d5: " << micro_val->d5;

  if (micro_val->d1 != 1 ||
      micro_val->d2 != 2 ||
      micro_val->d3 != 3 ||
      micro_val->d4 != 4 ||
      micro_val->d5 != 5) {
    RDMA_LOG(FATAL) << "READ value unmatches";
  }

  micro_record->SetUpdate(micro_val_bitmap::d1, &micro_val->d1, sizeof(micro_val->d1));
  micro_val->d1 = 100;

  micro_record->SetUpdate(micro_val_bitmap::d5, &micro_val->d5, sizeof(micro_val->d5));
  micro_val->d5 = 233;

  bool commit_status = txn->Commit(yield);
  return commit_status;
}

// read key 20
bool TxTest8(ZipfGen* zipf_gen,
             uint64_t* seed,
             coro_yield_t& yield,
             tx_id_t tx_id,
             TXN* txn,
             bool is_skewed,
             uint64_t data_set_size,
             uint64_t num_keys_global,
             uint64_t write_ratio) {
  txn->Begin(tx_id, TXN_TYPE::kROTxn);

  micro_key_t micro_key;
  micro_key.micro_id = 20;

  auto micro_record = std::make_shared<DataSetItem>((table_id_t)MicroTableType::kMicroTable,
                                                    micro_val_t_size,
                                                    micro_key.item_key,
                                                    UserOP::kRead);
  txn->AddToReadOnlySet(micro_record);

  if (!txn->Execute(yield)) {
    return false;
  }

  micro_val_t* micro_val = (micro_val_t*)micro_record->Value();

  RDMA_LOG(DBG) << "txid: " << tx_id
                << " read d1: " << micro_val->d1
                << " d2: " << micro_val->d2
                << " d3: " << micro_val->d3
                << " d4: " << micro_val->d4
                << " d5: " << micro_val->d5;

  if (micro_val->d1 != 100 ||
      micro_val->d2 != 2 ||
      micro_val->d3 != 3 ||
      micro_val->d4 != 4 ||
      micro_val->d5 != 233) {
    RDMA_LOG(FATAL) << "READ value unmatches";
  }

  bool commit_status = txn->Commit(yield);
  return commit_status;
}

// delete key 10
bool TxTest9(ZipfGen* zipf_gen,
             uint64_t* seed,
             coro_yield_t& yield,
             tx_id_t tx_id,
             TXN* txn,
             bool is_skewed,
             uint64_t data_set_size,
             uint64_t num_keys_global,
             uint64_t write_ratio) {
  txn->Begin(tx_id, TXN_TYPE::kRWTxn);

  micro_key_t micro_key;
  micro_key.micro_id = 10;

  auto micro_record = std::make_shared<DataSetItem>((table_id_t)MicroTableType::kMicroTable,
                                                    micro_val_t_size,
                                                    micro_key.item_key,
                                                    UserOP::kDelete);
  txn->AddToReadWriteSet(micro_record);

  if (!txn->Execute(yield)) {
    return false;
  }

  micro_val_t* micro_val = (micro_val_t*)micro_record->Value();

  RDMA_LOG(DBG) << "txid: " << tx_id
                << " read d1: " << micro_val->d1
                << " d2: " << micro_val->d2
                << " d3: " << micro_val->d3
                << " d4: " << micro_val->d4
                << " d5: " << micro_val->d5;

  // if (micro_val->d1 != micro_magic ||
  //     micro_val->d2 != micro_magic * 3 ||
  //     micro_val->d3 != micro_magic * 4 ||
  //     micro_val->d4 != micro_magic * 5 ||
  //     micro_val->d5 != micro_magic + 4) {
  //   RDMA_LOG(FATAL) << "READ value unmatches";
  // }

  bool commit_status = txn->Commit(yield);
  return commit_status;
}

// read key 10
bool TxTest10(ZipfGen* zipf_gen,
              uint64_t* seed,
              coro_yield_t& yield,
              tx_id_t tx_id,
              TXN* txn,
              bool is_skewed,
              uint64_t data_set_size,
              uint64_t num_keys_global,
              uint64_t write_ratio) {
  txn->Begin(tx_id, TXN_TYPE::kROTxn);

  micro_key_t micro_key;
  micro_key.micro_id = 10;

  auto micro_record = std::make_shared<DataSetItem>((table_id_t)MicroTableType::kMicroTable,
                                                    micro_val_t_size,
                                                    micro_key.item_key,
                                                    UserOP::kRead);
  txn->AddToReadOnlySet(micro_record);

  if (!txn->Execute(yield)) {
    return false;
  }

  micro_val_t* micro_val = (micro_val_t*)micro_record->Value();

  RDMA_LOG(DBG) << "txid: " << tx_id
                << " read d1: " << micro_val->d1
                << " d2: " << micro_val->d2
                << " d3: " << micro_val->d3
                << " d4: " << micro_val->d4
                << " d5: " << micro_val->d5;

  // if (micro_val->d1 != micro_magic ||
  //     micro_val->d2 != micro_magic * 3 ||
  //     micro_val->d3 != micro_magic * 4 ||
  //     micro_val->d4 != micro_magic + 3 ||
  //     micro_val->d5 != micro_magic + 4) {
  //   RDMA_LOG(FATAL) << "READ value unmatches";
  // }

  bool commit_status = txn->Commit(yield);
  return commit_status;
}

// updates key 5
bool TxTest11(ZipfGen* zipf_gen,
              uint64_t* seed,
              coro_yield_t& yield,
              tx_id_t tx_id,
              TXN* txn,
              bool is_skewed,
              uint64_t data_set_size,
              uint64_t num_keys_global,
              uint64_t write_ratio) {
  txn->Begin(tx_id, TXN_TYPE::kRWTxn);

  micro_key_t micro_key;
  micro_key.micro_id = 5;

  assert(micro_key.item_key >= 0 && micro_key.item_key < num_keys_global);

  auto micro_record = std::make_shared<DataSetItem>((table_id_t)MicroTableType::kMicroTable,
                                                    micro_val_t_size,
                                                    micro_key.item_key,
                                                    UserOP::kUpdate);
  txn->AddToReadWriteSet(micro_record);

  if (!txn->Execute(yield)) {
    return false;
  }

  micro_val_t* micro_val = (micro_val_t*)micro_record->Value();
  if (micro_val->d1 != micro_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << txn->t_id << "-" << txn->coro_id << "-" << tx_id;
  }

  RDMA_LOG(DBG) << "txid: " << tx_id
                << " read d1: " << micro_val->d1
                << " d2: " << micro_val->d2
                << " d3: " << micro_val->d3
                << " d4: " << micro_val->d4
                << " d5: " << micro_val->d5;

  micro_record->SetUpdate(micro_val_bitmap::d2, &micro_val->d2, sizeof(micro_val->d2));
  micro_val->d2 = tx_id;

  bool commit_status = txn->Commit(yield);
  return commit_status;
}

// read key 5
bool TxTest12(ZipfGen* zipf_gen,
              uint64_t* seed,
              coro_yield_t& yield,
              tx_id_t tx_id,
              TXN* txn,
              bool is_skewed,
              uint64_t data_set_size,
              uint64_t num_keys_global,
              uint64_t write_ratio) {
  tx_id = 38;
  txn->Begin(tx_id, TXN_TYPE::kROTxn);

  micro_key_t micro_key;
  micro_key.micro_id = 5;

  assert(micro_key.item_key >= 0 && micro_key.item_key < num_keys_global);

  auto micro_record = std::make_shared<DataSetItem>((table_id_t)MicroTableType::kMicroTable,
                                                    micro_val_t_size,
                                                    micro_key.item_key,
                                                    UserOP::kUpdate);
  txn->AddToReadOnlySet(micro_record);

  if (!txn->Execute(yield)) {
    return false;
  }

  micro_val_t* micro_val = (micro_val_t*)micro_record->Value();

  RDMA_LOG(DBG) << "txid: " << tx_id
                << " read d1: " << micro_val->d1
                << " d2: " << micro_val->d2
                << " d3: " << micro_val->d3
                << " d4: " << micro_val->d4
                << " d5: " << micro_val->d5;

  bool commit_status = txn->Commit(yield);
  return commit_status;
}

/******************** The business logic (Transaction) end ********************/
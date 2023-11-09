// Author: Ming Zhang
// Copyright (c) 2023

#include "tatp/tatp_txn.h"

#include <atomic>

/******************** The business logic (Transaction) start ********************/

// Read 1 SUBSCRIBER row
bool TxGetSubsciberData(TATP* tatp_client,
                        uint64_t* seed,
                        coro_yield_t& yield,
                        tx_id_t tx_id,
                        TXN* txn) {
  // RDMA_LOG(DBG) << "coro " << txn->coro_id << " executes TxGetSubsciberData, tx_id=" << tx_id;
  txn->Begin(tx_id, TXN_TYPE::kROTxn, "GetSubsciberData");

  // Build key for the database record
  tatp_sub_key_t sub_key;
  sub_key.s_id = tatp_client->GetNonUniformRandomSubscriber(seed);

  // This empty data sub_record will be filled by RDMA reading from remote when running transaction
  auto sub_record = std::make_shared<DataSetItem>((table_id_t)TATPTableType::kSubscriberTable,
                                                  tatp_sub_val_t_size,
                                                  sub_key.item_key,
                                                  UserOP::kRead);
  txn->AddToReadOnlySet(sub_record);

  if (!txn->Execute(yield)) return false;

  // Get value
  auto* value = (tatp_sub_val_t*)sub_record->Value();

  // Use value
  if (value->msc_location != tatp_sub_msc_location_magic) {
    RDMA_LOG(DBG) << "my msc_location: " << value->msc_location << " vs location_magic: " << tatp_sub_msc_location_magic;
    sub_record->Debug();
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << txn->t_id << "-" << txn->coro_id << "-" << tx_id;
  }

  // Commit transaction
  bool commit_status = txn->Commit(yield);
  return commit_status;
}

// 1. Read 1 SPECIAL_FACILITY row
// 2. Read up to 3 CALL_FORWARDING rows
// 3. Validate up to 4 rows
bool TxGetNewDestination(TATP* tatp_client,
                         uint64_t* seed,
                         coro_yield_t& yield,
                         tx_id_t tx_id,
                         TXN* txn) {
  // RDMA_LOG(DBG) << "coro " << txn->coro_id << " executes TxGetNewDestination";
  txn->Begin(tx_id, TXN_TYPE::kROTxn, "GetNewDestination");

  uint32_t s_id = tatp_client->GetNonUniformRandomSubscriber(seed);
  uint8_t sf_type = (FastRand(seed) % 4) + 1;
  uint8_t start_time = (FastRand(seed) % 3) * 8;
  uint8_t end_time = (FastRand(seed) % 24) * 1;

  unsigned cf_to_fetch = (start_time / 8) + 1;
  assert(cf_to_fetch >= 1 && cf_to_fetch <= 3);

  /* Fetch a single special facility record */
  tatp_specfac_key_t specfac_key;

  specfac_key.s_id = s_id;
  specfac_key.sf_type = sf_type;

  auto specfac_record = std::make_shared<DataSetItem>((table_id_t)TATPTableType::kSpecialFacilityTable,
                                                      tatp_specfac_val_t_size,
                                                      specfac_key.item_key,
                                                      UserOP::kRead);
  txn->AddToReadOnlySet(specfac_record);

  if (!txn->Execute(yield)) return false;

  if (specfac_record->SizeofValue() == 0) {
    return false;
  }

  // Need to wait for reading specfac_record from remote
  auto* specfac_val = (tatp_specfac_val_t*)specfac_record->Value();
  if (specfac_val->data_b[0] != tatp_specfac_data_b0_magic) {
    specfac_record->Debug();
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << txn->t_id << "-" << txn->coro_id << "-" << tx_id;
  }
  if (specfac_val->is_active == 0) {
    // is_active is randomly generated at pm node side
    return false;
  }

  /* Fetch possibly multiple call forwarding records. */
  DataSetItemPtr callfwd_record[3];
  tatp_callfwd_key_t callfwd_key[3];

  for (unsigned i = 0; i < cf_to_fetch; i++) {
    callfwd_key[i].s_id = s_id;
    callfwd_key[i].sf_type = sf_type;
    callfwd_key[i].start_time = (i * 8);
    callfwd_record[i] = std::make_shared<DataSetItem>((table_id_t)TATPTableType::kCallForwardingTable,
                                                      tatp_callfwd_val_t_size,
                                                      callfwd_key[i].item_key,
                                                      UserOP::kRead);
    txn->AddToReadOnlySet(callfwd_record[i]);
  }

  if (!txn->Execute(yield)) return false;

  bool callfwd_success = false;

  for (unsigned i = 0; i < cf_to_fetch; i++) {
    if (callfwd_record[i]->SizeofValue() == 0) {
      continue;
    }

    auto* callfwd_val = (tatp_callfwd_val_t*)callfwd_record[i]->Value();
    if (callfwd_val->numberx[0] != tatp_callfwd_numberx0_magic) {
      callfwd_record[i]->Debug();
      RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << txn->t_id << "-" << txn->coro_id << "-" << tx_id;
    }
    if (callfwd_key[i].start_time <= start_time && end_time < callfwd_val->end_time) {
      /* All conditions satisfied */
      callfwd_success = true;
    }
  }

  if (callfwd_success) {
    bool commit_status = txn->Commit(yield);
    return commit_status;
  } else {
    return false;
  }
}

// Read 1 ACCESS_INFO row
bool TxGetAccessData(TATP* tatp_client,
                     uint64_t* seed,
                     coro_yield_t& yield,
                     tx_id_t tx_id,
                     TXN* txn) {
  // RDMA_LOG(DBG) << "coro " << txn->coro_id << " executes TxGetAccessData, tx_id=" << tx_id;
  txn->Begin(tx_id, TXN_TYPE::kROTxn, "GetAccessData");

  tatp_accinf_key_t key;
  key.s_id = tatp_client->GetNonUniformRandomSubscriber(seed);
  key.ai_type = (FastRand(seed) & 3) + 1;

  auto acc_record = std::make_shared<DataSetItem>((table_id_t)TATPTableType::kAccessInfoTable,
                                                  tatp_accinf_val_t_size,
                                                  key.item_key,
                                                  UserOP::kRead);
  txn->AddToReadOnlySet(acc_record);

  if (!txn->Execute(yield)) return false;

  if (acc_record->SizeofValue() > 0) {
    /* The key was found */
    auto* value = (tatp_accinf_val_t*)acc_record->Value();
    if (value->data1 != tatp_accinf_data1_magic) {
      acc_record->Debug();
      RDMA_LOG(DBG) << (int)value->data1;
      RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << txn->t_id << "-" << txn->coro_id << "-" << tx_id;
    }

    bool commit_status = txn->Commit(yield);
    return commit_status;
  } else {
    /* Key not found */
    return false;
  }
}

// Update 1 SUBSCRIBER row and 1 SPECIAL_FACILTY row
bool TxUpdateSubscriberData(TATP* tatp_client,
                            uint64_t* seed,
                            coro_yield_t& yield,
                            tx_id_t tx_id,
                            TXN* txn) {
  // RDMA_LOG(DBG) << "coro " << txn->coro_id << " executes TxUpdateSubscriberData, tx_id=" << tx_id;
  txn->Begin(tx_id, TXN_TYPE::kRWTxn, "UpdateSubscriberData");

  uint32_t s_id = tatp_client->GetNonUniformRandomSubscriber(seed);
  uint8_t sf_type = (FastRand(seed) % 4) + 1;

  /* Read + lock the subscriber record */
  tatp_sub_key_t sub_key;
  sub_key.s_id = s_id;

  auto sub_record = std::make_shared<DataSetItem>((table_id_t)TATPTableType::kSubscriberTable,
                                                  tatp_sub_val_t_size,
                                                  sub_key.item_key,
                                                  UserOP::kUpdate);
  txn->AddToReadWriteSet(sub_record);

  /* Read + lock the special facilty record */
  tatp_specfac_key_t specfac_key;
  specfac_key.s_id = s_id;
  specfac_key.sf_type = sf_type;

  auto specfac_record = std::make_shared<DataSetItem>((table_id_t)TATPTableType::kSpecialFacilityTable,
                                                      tatp_specfac_val_t_size,
                                                      specfac_key.item_key,
                                                      UserOP::kUpdate);
  txn->AddToReadWriteSet(specfac_record);

  if (!txn->Execute(yield)) return false;

  /* If we are here, execution succeeded and we have locks */
  auto* sub_val = (tatp_sub_val_t*)sub_record->Value();
  if (sub_val->msc_location != tatp_sub_msc_location_magic) {
    sub_record->Debug();
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << txn->t_id << "-" << txn->coro_id << "-" << tx_id;
  }
  sub_record->SetUpdate(tatp_sub_val_bitmap::bits, &sub_val->bits, sizeof(sub_val->bits));

  sub_val->bits = FastRand(seed); /* Update */

  auto* specfac_val = (tatp_specfac_val_t*)specfac_record->Value();
  if (specfac_val->data_b[0] != tatp_specfac_data_b0_magic) {
    specfac_record->Debug();
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << txn->t_id << "-" << txn->coro_id << "-" << tx_id;
  }
  specfac_record->SetUpdate(tatp_specfac_val_bitmap::data_a, &specfac_val->data_a, sizeof(specfac_val->data_a));
  specfac_val->data_a = FastRand(seed); /* Update */

  bool commit_status = txn->Commit(yield);
  return commit_status;
}

// 1. Read a SECONDARY_SUBSCRIBER row
// 2. Update a SUBSCRIBER row
bool TxUpdateLocation(TATP* tatp_client,
                      uint64_t* seed,
                      coro_yield_t& yield,
                      tx_id_t tx_id,
                      TXN* txn) {
  txn->Begin(tx_id, TXN_TYPE::kRWTxn, "UpdateLocation");

  uint32_t s_id = tatp_client->GetNonUniformRandomSubscriber(seed);
  uint32_t vlr_location = FastRand(seed);

  /* Read the secondary subscriber record */
  tatp_sec_sub_key_t sec_sub_key;
  sec_sub_key.sub_number = tatp_client->FastGetSubscribeNumFromSubscribeID(s_id);

  auto sec_sub_record = std::make_shared<DataSetItem>((table_id_t)TATPTableType::kSecSubscriberTable,
                                                      tatp_sec_sub_val_t_size,
                                                      sec_sub_key.item_key,
                                                      UserOP::kRead);
  txn->AddToReadOnlySet(sec_sub_record);

  if (!txn->Execute(yield)) return false;

  auto* sec_sub_val = (tatp_sec_sub_val_t*)sec_sub_record->Value();
  if (sec_sub_val->magic != tatp_sec_sub_magic) {
    sec_sub_record->Debug();
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << txn->t_id << "-" << txn->coro_id << "-" << tx_id;
  }
  if (sec_sub_val->s_id != s_id) {
    sec_sub_record->Debug();
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << txn->t_id << "-" << txn->coro_id << "-" << tx_id;
  }

  tatp_sub_key_t sub_key;
  sub_key.s_id = sec_sub_val->s_id;

  auto sub_record = std::make_shared<DataSetItem>((table_id_t)TATPTableType::kSubscriberTable,
                                                  tatp_sub_val_t_size,
                                                  sub_key.item_key,
                                                  UserOP::kUpdate);
  txn->AddToReadWriteSet(sub_record);

  if (!txn->Execute(yield)) return false;

  auto* sub_val = (tatp_sub_val_t*)sub_record->Value();
  if (sub_val->msc_location != tatp_sub_msc_location_magic) {
    RDMA_LOG(DBG) << sub_val->msc_location;
    sub_record->Debug();
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << txn->t_id << "-" << txn->coro_id << "-" << tx_id;
  }
  sub_record->SetUpdate(tatp_sub_val_bitmap::vlr_location, &sub_val->vlr_location, sizeof(sub_val->vlr_location));

  sub_val->vlr_location = vlr_location; /* Update */

  bool commit_status = txn->Commit(yield);
  return commit_status;
}

// 1. Read a SECONDARY_SUBSCRIBER row
// 2. Read a SPECIAL_FACILTY row
// 3. Insert a CALL_FORWARDING row
bool TxInsertCallForwarding(TATP* tatp_client,
                            uint64_t* seed,
                            coro_yield_t& yield,
                            tx_id_t tx_id,
                            TXN* txn) {
  // RDMA_LOG(DBG) << "coro " << txn->coro_id << " executes TxInsertCallForwarding, tx_id=" << tx_id;
  txn->Begin(tx_id, TXN_TYPE::kRWTxn, "InsertCallForwarding");

  uint32_t s_id = tatp_client->GetNonUniformRandomSubscriber(seed);
  uint8_t sf_type = (FastRand(seed) % 4) + 1;
  uint8_t start_time = (FastRand(seed) % 3) * 8;
  uint8_t end_time = (FastRand(seed) % 24) * 1;

  // Read the secondary subscriber record
  tatp_sec_sub_key_t sec_sub_key;
  sec_sub_key.sub_number = tatp_client->FastGetSubscribeNumFromSubscribeID(s_id);

  auto sec_sub_record = std::make_shared<DataSetItem>((table_id_t)TATPTableType::kSecSubscriberTable,
                                                      tatp_sec_sub_val_t_size,
                                                      sec_sub_key.item_key,
                                                      UserOP::kRead);
  txn->AddToReadOnlySet(sec_sub_record);

  if (!txn->Execute(yield)) return false;

  auto* sec_sub_val = (tatp_sec_sub_val_t*)sec_sub_record->Value();
  if (sec_sub_val->magic != tatp_sec_sub_magic) {
    sec_sub_record->Debug();
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << txn->t_id << "-" << txn->coro_id << "-" << tx_id;
  }
  if (sec_sub_val->s_id != s_id) {
    sec_sub_record->Debug();
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << txn->t_id << "-" << txn->coro_id << "-" << tx_id;
  }

  // Read the Special Facility record
  tatp_specfac_key_t specfac_key;
  specfac_key.s_id = s_id;
  specfac_key.sf_type = sf_type;

  auto specfac_record = std::make_shared<DataSetItem>((table_id_t)TATPTableType::kSpecialFacilityTable,
                                                      tatp_specfac_val_t_size,
                                                      specfac_key.item_key,
                                                      UserOP::kRead);
  txn->AddToReadOnlySet(specfac_record);

  if (!txn->Execute(yield)) return false;

  // The Special Facility record exists only 62.5% of the time
  if (specfac_record->SizeofValue() == 0) {
    return false;
  }

  // The Special Facility record exists.
  auto* specfac_val = (tatp_specfac_val_t*)specfac_record->Value();
  if (specfac_val->data_b[0] != tatp_specfac_data_b0_magic) {
    specfac_record->Debug();
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << txn->t_id << "-" << txn->coro_id << "-" << tx_id;
  }

  // Lock the Call Forwarding record
  tatp_callfwd_key_t callfwd_key;
  callfwd_key.s_id = s_id;
  callfwd_key.sf_type = sf_type;
  callfwd_key.start_time = start_time;

  auto callfwd_record = std::make_shared<DataSetItem>((table_id_t)TATPTableType::kCallForwardingTable,
                                                      tatp_callfwd_val_t_size,  // Here Insert
                                                      callfwd_key.item_key,
                                                      UserOP::kInsert);
  // Handle Insert. Only read the remote offset of callfwd_record
  txn->AddToReadWriteSet(callfwd_record);

  if (!txn->Execute(yield)) return false;

  // Fill callfwd_val by user
  auto* callfwd_val = (tatp_callfwd_val_t*)callfwd_record->Value();
  if (!callfwd_record->IsRealInsert()) {
    callfwd_record->SetUpdate(tatp_callfwd_val_bitmap::end_time, &callfwd_val->end_time, sizeof(callfwd_val->end_time));
    callfwd_record->SetUpdate(tatp_callfwd_val_bitmap::numberx, callfwd_val->numberx, sizeof(callfwd_val->numberx));
  }
  callfwd_val->end_time = end_time;
  callfwd_val->numberx[0] = tatp_callfwd_numberx0_magic;

  bool commit_status = txn->Commit(yield);
  return commit_status;
}

// 1. Read a SECONDARY_SUBSCRIBER row
// 2. Delete a CALL_FORWARDING row
bool TxDeleteCallForwarding(TATP* tatp_client,
                            uint64_t* seed,
                            coro_yield_t& yield,
                            tx_id_t tx_id,
                            TXN* txn) {
  // RDMA_LOG(DBG) << "coro " << txn->coro_id << " executes TxDeleteCallForwarding, tx_id=" << tx_id;
  txn->Begin(tx_id, TXN_TYPE::kRWTxn, "DeleteCallForwarding");

  uint32_t s_id = tatp_client->GetNonUniformRandomSubscriber(seed);
  uint8_t sf_type = (FastRand(seed) % 4) + 1;
  uint8_t start_time = (FastRand(seed) % 3) * 8;

  // Read the secondary subscriber record
  tatp_sec_sub_key_t sec_sub_key;
  sec_sub_key.sub_number = tatp_client->FastGetSubscribeNumFromSubscribeID(s_id);
  auto sec_sub_record = std::make_shared<DataSetItem>((table_id_t)TATPTableType::kSecSubscriberTable,
                                                      tatp_sec_sub_val_t_size,
                                                      sec_sub_key.item_key,
                                                      UserOP::kRead);
  txn->AddToReadOnlySet(sec_sub_record);

  if (!txn->Execute(yield)) {
    return false;
  }

  auto* sec_sub_val = (tatp_sec_sub_val_t*)sec_sub_record->Value();
  if (sec_sub_val->magic != tatp_sec_sub_magic) {
    sec_sub_record->Debug();
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << txn->t_id << "-" << txn->coro_id << "-" << tx_id;
  }

  if (sec_sub_val->s_id != s_id) {
    sec_sub_record->Debug();
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << txn->t_id << "-" << txn->coro_id << "-" << tx_id;
  }

  // Delete the Call Forwarding record if it exists
  tatp_callfwd_key_t callfwd_key;
  callfwd_key.s_id = s_id;
  callfwd_key.sf_type = sf_type;
  callfwd_key.start_time = start_time;

  auto callfwd_record = std::make_shared<DataSetItem>((table_id_t)TATPTableType::kCallForwardingTable,
                                                      tatp_callfwd_val_t_size,
                                                      callfwd_key.item_key,
                                                      UserOP::kDelete);
  txn->AddToReadWriteSet(callfwd_record);

  if (!txn->Execute(yield)) return false;

  bool commit_status = txn->Commit(yield);
  return commit_status;
}

/******************** The business logic (Transaction) end ********************/

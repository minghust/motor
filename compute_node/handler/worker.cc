// Author: Ming Zhang
// Copyright (c) 2023

#include "handler/worker.h"

#include <atomic>
#include <cstdio>
#include <fstream>
#include <functional>
#include <memory>

#include "allocator/buffer_allocator.h"
#include "connection/qp_manager.h"
#include "micro/micro_txn.h"
#include "process/txn.h"
#include "smallbank/smallbank_txn.h"
#include "tatp/tatp_txn.h"
#include "tpcc/tpcc_txn.h"
#include "util/latency.h"
#include "util/zipf.h"

using namespace std::placeholders;

///////////// For control and statistics ///////////////
std::mutex mux;

extern std::atomic<uint64_t> tx_id_generator;
extern std::atomic<uint64_t> connected_t_num;
extern std::atomic<uint64_t> connected_recovery_t_num;

extern std::vector<t_id_t> tid_vec;
extern std::vector<double> attemp_tp_vec;
extern std::vector<double> tp_vec;
extern std::vector<double> medianlat_vec;
extern std::vector<double> taillat_vec;
extern std::vector<double> delta_usage;

extern std::vector<uint64_t> total_try_times;
extern std::vector<uint64_t> total_commit_times;

extern std::atomic<bool> to_crash[MAX_TNUM_PER_CN];
extern std::atomic<bool> report_crash[MAX_TNUM_PER_CN];
extern uint64_t try_times[MAX_TNUM_PER_CN];
extern std::atomic<int> probe_times;
extern std::atomic<bool> probe[MAX_TNUM_PER_CN];

__thread std::vector<TpProbe>* tp_probe_list;
/////////////////////////////////////////////////////////

__thread size_t ATTEMPTED_NUM;
__thread uint64_t seed;                           // Thread-global random seed
__thread FastRandom* random_generator = nullptr;  // Per coroutine random generator
__thread t_id_t thread_gid;
__thread t_id_t thread_local_id;

__thread TATP* tatp_client = nullptr;
__thread SmallBank* smallbank_client = nullptr;
__thread TPCC* tpcc_client = nullptr;

__thread MetaManager* meta_man;
__thread QPManager* qp_man;

__thread LocalBufferAllocator* rdma_buffer_allocator;
__thread RemoteDeltaOffsetAllocator* delta_offset_allocator;
__thread LockedKeyTable* locked_key_table;
__thread AddrCache* addr_cache;

__thread TATPTxType* tatp_workgen_arr;
__thread SmallBankTxType* smallbank_workgen_arr;
__thread TPCCTxType* tpcc_workgen_arr;

__thread coro_id_t coro_num;
__thread CoroutineScheduler* coro_sched;  // Each transaction thread has a coroutine scheduler

// Performance measurement (thread granularity)
__thread struct timespec msr_start, msr_end, last_end;
__thread double* timer;
__thread uint64_t stat_attempted_tx_total = 0;  // Issued transaction number
__thread uint64_t stat_committed_tx_total = 0;  // Committed transaction number
__thread uint64_t last_stat_attempted_tx_total = 0;
__thread uint64_t last_stat_committed_tx_total = 0;
const coro_id_t POLL_ROUTINE_ID = 0;  // The poll coroutine ID

// For MICRO benchmark
__thread ZipfGen* zipf_gen = nullptr;
__thread bool is_skewed;
__thread uint64_t data_set_size;
__thread uint64_t num_keys_global;
__thread uint64_t write_ratio;

// Stat the commit rate
__thread uint64_t* thread_local_try_times;
__thread uint64_t* thread_local_commit_times;
/////////////////////////////////////////////////////////

// Coroutine 0 in each thread does polling
void Poll(coro_yield_t& yield) {
  while (true) {
    coro_sched->PollCompletion(thread_gid);
    Coroutine* next = coro_sched->coro_head->next_coro;
    if (next->coro_id != POLL_ROUTINE_ID) {
      // RDMA_LOG(DBG) << "Coro 0 yields to coro " << next->coro_id;
      coro_sched->RunCoroutine(yield, next);
    }
  }
}

void RecordTpLat(double msr_sec) {
  double attemp_tput = (double)stat_attempted_tx_total / msr_sec;
  double tx_tput = (double)stat_committed_tx_total / msr_sec;

  std::sort(timer, timer + stat_committed_tx_total);
  double percentile_50 = timer[stat_committed_tx_total / 2];
  double percentile_99 = timer[stat_committed_tx_total * 99 / 100];

  mux.lock();

  tid_vec.push_back(thread_gid);
  attemp_tp_vec.push_back(attemp_tput);
  tp_vec.push_back(tx_tput);
  medianlat_vec.push_back(percentile_50);
  taillat_vec.push_back(percentile_99);

  for (size_t i = 0; i < total_try_times.size(); i++) {
    // Records the total number of tried and committed txn in all threads
    // across all txn types (i.e., i) in the current workload
    total_try_times[i] += thread_local_try_times[i];
    total_commit_times[i] += thread_local_commit_times[i];
  }

  mux.unlock();
}

// Run actual transactions
void RunTATP(coro_yield_t& yield, coro_id_t coro_id) {
  // Each coroutine has a txn: Each coroutine is a coordinator
  TXN* txn = new TXN(meta_man,
                     qp_man,
                     thread_gid,
                     coro_id,
                     coro_sched,
                     rdma_buffer_allocator,
                     delta_offset_allocator,
                     locked_key_table,
                     addr_cache);
  struct timespec tx_start_time, tx_end_time;
  bool tx_committed = false;

  // Running transactions
  clock_gettime(CLOCK_REALTIME, &msr_start);
  while (true) {
    // Guarantee that each coroutine has a different seed
    TATPTxType tx_type = tatp_workgen_arr[FastRand(&seed) % 100];
    uint64_t iter = ++tx_id_generator;  // Global atomic transaction id
    stat_attempted_tx_total++;
    clock_gettime(CLOCK_REALTIME, &tx_start_time);
    switch (tx_type) {
      case TATPTxType::kGetSubsciberData: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = TxGetSubsciberData(tatp_client, &seed, yield, iter, txn);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case TATPTxType::kGetNewDestination: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = TxGetNewDestination(tatp_client, &seed, yield, iter, txn);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case TATPTxType::kGetAccessData: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = TxGetAccessData(tatp_client, &seed, yield, iter, txn);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case TATPTxType::kUpdateSubscriberData: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = TxUpdateSubscriberData(tatp_client, &seed, yield, iter, txn);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case TATPTxType::kUpdateLocation: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = TxUpdateLocation(tatp_client, &seed, yield, iter, txn);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case TATPTxType::kInsertCallForwarding: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = TxInsertCallForwarding(tatp_client, &seed, yield, iter, txn);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case TATPTxType::kDeleteCallForwarding: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = TxDeleteCallForwarding(tatp_client, &seed, yield, iter, txn);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      default:
        printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
        abort();
    }

    /********************************** Stat begin *****************************************/
    // Stat after one transaction finishes
    if (tx_committed) {
      clock_gettime(CLOCK_REALTIME, &tx_end_time);
      double tx_usec = (tx_end_time.tv_sec - tx_start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - tx_start_time.tv_nsec) / 1000;
      timer[stat_committed_tx_total++] = tx_usec;
    }
    if (stat_attempted_tx_total >= ATTEMPTED_NUM) {
      // A coroutine calculate the total execution time and exits
      clock_gettime(CLOCK_REALTIME, &msr_end);
      // double msr_usec = (msr_end.tv_sec - msr_start.tv_sec) * 1000000 + (double) (msr_end.tv_nsec - msr_start.tv_nsec) / 1000;
      double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;
      RecordTpLat(msr_sec);
      break;
    }
    /********************************** Stat end *****************************************/
  }

  delete txn;
}

void RunSmallBank(coro_yield_t& yield, coro_id_t coro_id) {
  // Each coroutine has a txn: Each coroutine is a coordinator
  TXN* txn = new TXN(meta_man,
                     qp_man,
                     thread_gid,
                     coro_id,
                     coro_sched,
                     rdma_buffer_allocator,
                     delta_offset_allocator,
                     locked_key_table,
                     addr_cache);
  struct timespec tx_start_time, tx_end_time;
  bool tx_committed = false;

  // Running transactions
  clock_gettime(CLOCK_REALTIME, &msr_start);
  while (true) {
    SmallBankTxType tx_type = smallbank_workgen_arr[FastRand(&seed) % 100];
    uint64_t iter = ++tx_id_generator;  // Global atomic transaction id
    stat_attempted_tx_total++;
    clock_gettime(CLOCK_REALTIME, &tx_start_time);
    switch (tx_type) {
      case SmallBankTxType::kAmalgamate: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = TxAmalgamate(smallbank_client, &seed, yield, iter, txn);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kBalance: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = TxBalance(smallbank_client, &seed, yield, iter, txn);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kDepositChecking: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = TxDepositChecking(smallbank_client, &seed, yield, iter, txn);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kSendPayment: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = TxSendPayment(smallbank_client, &seed, yield, iter, txn);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kTransactSaving: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = TxTransactSaving(smallbank_client, &seed, yield, iter, txn);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kWriteCheck: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = TxWriteCheck(smallbank_client, &seed, yield, iter, txn);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      default:
        printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
        abort();
    }

    /********************************** Stat begin *****************************************/
    // Stat after one transaction finishes
    if (tx_committed) {
      clock_gettime(CLOCK_REALTIME, &tx_end_time);
      double tx_usec = (tx_end_time.tv_sec - tx_start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - tx_start_time.tv_nsec) / 1000;
      timer[stat_committed_tx_total++] = tx_usec;
    }
    if (stat_attempted_tx_total >= ATTEMPTED_NUM) {
      // A coroutine calculate the total execution time and exits
      clock_gettime(CLOCK_REALTIME, &msr_end);
      // double msr_usec = (msr_end.tv_sec - msr_start.tv_sec) * 1000000 + (double) (msr_end.tv_nsec - msr_start.tv_nsec) / 1000;
      double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;
      RecordTpLat(msr_sec);
      break;
    }
    /********************************** Stat end *****************************************/
  }

  delete txn;
}

void RunTPCC(coro_yield_t& yield, coro_id_t coro_id, int finished_num) {
  // Each coroutine has a txn: Each coroutine is a coordinator
  TXN* txn = new TXN(meta_man,
                     qp_man,
                     thread_gid,
                     coro_id,
                     coro_sched,
                     rdma_buffer_allocator,
                     delta_offset_allocator,
                     locked_key_table,
                     addr_cache);
  struct timespec tx_start_time, tx_end_time;
  bool tx_committed = false;

  // Running transactions
  clock_gettime(CLOCK_REALTIME, &msr_start);
  last_end = msr_start;
  while (true) {
    // Guarantee that each coroutine has a different seed
    TPCCTxType tx_type = tpcc_workgen_arr[FastRand(&seed) % 100];
    uint64_t iter = ++tx_id_generator;  // Global atomic transaction id
    stat_attempted_tx_total++;
    // TLOG(INFO, thread_gid) << "Thread " << thread_gid << " attemps txn " << stat_attempted_tx_total << " txn id: " << iter;

    clock_gettime(CLOCK_REALTIME, &tx_start_time);
    switch (tx_type) {
      case TPCCTxType::kDelivery: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = TxDelivery(tpcc_client, random_generator, yield, iter, txn);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
      } break;
      case TPCCTxType::kNewOrder: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = TxNewOrder(tpcc_client, random_generator, yield, iter, txn);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
      } break;
      case TPCCTxType::kOrderStatus: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = TxOrderStatus(tpcc_client, random_generator, yield, iter, txn);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
      } break;
      case TPCCTxType::kPayment: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = TxPayment(tpcc_client, random_generator, yield, iter, txn);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
      } break;
      case TPCCTxType::kStockLevel: {
        do {
          thread_local_try_times[uint64_t(tx_type)]++;
          clock_gettime(CLOCK_REALTIME, &tx_start_time);

          tx_committed = TxStockLevel(tpcc_client, random_generator, yield, iter, txn);
          if (!tx_committed) {
            iter = ++tx_id_generator;
          }
        } while (tx_committed != true);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
      } break;
      default:
        printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
        abort();
    }

    /********************************** Stat begin *****************************************/
    // Stat after one transaction finishes
    if (tx_committed) {
      clock_gettime(CLOCK_REALTIME, &tx_end_time);
      double tx_usec = (tx_end_time.tv_sec - tx_start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - tx_start_time.tv_nsec) / 1000;
      timer[stat_committed_tx_total++] = tx_usec;
    }

    if (stat_attempted_tx_total >= (ATTEMPTED_NUM - finished_num)) {
      // A coroutine calculate the total execution time and exits
      clock_gettime(CLOCK_REALTIME, &msr_end);
      // double msr_usec = (msr_end.tv_sec - msr_start.tv_sec) * 1000000 + (double) (msr_end.tv_nsec - msr_start.tv_nsec) / 1000;
      double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;
      RecordTpLat(msr_sec);

      break;
    }

    try_times[thread_local_id] = stat_attempted_tx_total;

    if (to_crash[thread_local_id]) {
      clock_gettime(CLOCK_REALTIME, &msr_end);
      // std::cerr << "Thread " << thread_gid << " crash" << std::endl;
      double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;
      RecordTpLat(msr_sec);
      // for (int i = 0; i < coro_num; i++) {
      //   if (locked_key_table[i].num_entry) {
      //     RDMA_LOG(INFO) << "cid: " << i << ", txid: " << locked_key_table[i].tx_id << ", num_entry: " << locked_key_table[i].num_entry;
      //   }
      // }
      report_crash[thread_local_id] = true;

      break;
    }

#if PROBE_TP

    if (probe[thread_local_id]) {
      // Probe tp
      double msr_sec, attemp_tput, tx_tput;
      clock_gettime(CLOCK_REALTIME, &msr_end);

      msr_sec = (msr_end.tv_sec - last_end.tv_sec) + (double)(msr_end.tv_nsec - last_end.tv_nsec) / 1000000000;
      last_end = msr_end;

      attemp_tput = (double)(stat_attempted_tx_total - last_stat_attempted_tx_total) / msr_sec;
      last_stat_attempted_tx_total = stat_attempted_tx_total;

      tx_tput = (double)(stat_committed_tx_total - last_stat_committed_tx_total) / msr_sec;
      last_stat_committed_tx_total = stat_committed_tx_total;

      tp_probe_list->emplace_back(TpProbe{.ctr = probe_times, .tp = tx_tput, .attemp_tp = attemp_tput});

      probe[thread_local_id] = false;
    }
#endif
    /********************************** Stat end *****************************************/
  }

  delete txn;
}

void RunMICRO(coro_yield_t& yield, coro_id_t coro_id) {
  double total_msr_us = 0;
  // Each coroutine has a txn: Each coroutine is a coordinator
  TXN* txn = new TXN(meta_man,
                     qp_man,
                     thread_gid,
                     coro_id,
                     coro_sched,
                     rdma_buffer_allocator,
                     delta_offset_allocator,
                     locked_key_table,
                     addr_cache);
  struct timespec tx_start_time, tx_end_time;
  bool tx_committed = false;

  // Running transactions
  clock_gettime(CLOCK_REALTIME, &msr_start);
  while (true) {
    uint64_t iter = ++tx_id_generator;  // Global atomic transaction id
    itemkey_t key;

    if (is_skewed) {
      // Skewed distribution
      key = (itemkey_t)(zipf_gen->next());
    } else {
      // Uniformed distribution
      key = (itemkey_t)FastRand(&seed) & (num_keys_global - 1);
    }

    assert(key >= 0 && key < num_keys_global);

    if (FastRand(&seed) % 100 < write_ratio) {
      // rw
      thread_local_try_times[uint64_t(MicroTxType::kUpdateOne)]++;
      stat_attempted_tx_total++;

      clock_gettime(CLOCK_REALTIME, &tx_start_time);

      tx_committed = TxUpdateOne(yield, iter, txn, key);

      if (tx_committed) thread_local_commit_times[uint64_t(MicroTxType::kUpdateOne)]++;
    } else {
      // ro
      thread_local_try_times[uint64_t(MicroTxType::kReadOne)]++;
      stat_attempted_tx_total++;

      clock_gettime(CLOCK_REALTIME, &tx_start_time);

      tx_committed = TxReadOne(yield, iter, txn, key);

      if (tx_committed) thread_local_commit_times[uint64_t(MicroTxType::kReadOne)]++;
    }

    /********************************** Stat begin *****************************************/
    // Stat after one transaction finishes
    if (tx_committed) {
      clock_gettime(CLOCK_REALTIME, &tx_end_time);
      double tx_usec = (tx_end_time.tv_sec - tx_start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - tx_start_time.tv_nsec) / 1000;
      timer[stat_committed_tx_total++] = tx_usec;
    }

    if (stat_committed_tx_total >= ATTEMPTED_NUM) {
      // A coroutine calculate the total execution time and exits
      clock_gettime(CLOCK_REALTIME, &msr_end);
      // double msr_usec = (msr_end.tv_sec - msr_start.tv_sec) * 1000000 + (double) (msr_end.tv_nsec - msr_start.tv_nsec) / 1000;
      double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;
      RecordTpLat(msr_sec);
      break;
    }
  }

  /********************************** Stat end *****************************************/

  delete txn;
}

void run_thread(thread_params* params,
                TATP* tatp_cli,
                SmallBank* smallbank_cli,
                TPCC* tpcc_cli,
                std::vector<TpProbe>* thread_tp_probe) {
  auto bench_name = params->bench_name;
  std::string config_filepath = "../../../config/" + bench_name + "_config.json";

  auto json_config = JsonConfig::load_file(config_filepath);
  auto conf = json_config.get(bench_name);
  ATTEMPTED_NUM = conf.get("attempted_num").get_uint64();

  if (bench_name == "tatp") {
    tatp_client = tatp_cli;
    tatp_workgen_arr = tatp_client->CreateWorkgenArray();
    thread_local_try_times = new uint64_t[TATP_TX_TYPES]();
    thread_local_commit_times = new uint64_t[TATP_TX_TYPES]();
  } else if (bench_name == "smallbank") {
    smallbank_client = smallbank_cli;
    smallbank_workgen_arr = smallbank_client->CreateWorkgenArray();
    thread_local_try_times = new uint64_t[SmallBank_TX_TYPES]();
    thread_local_commit_times = new uint64_t[SmallBank_TX_TYPES]();
  } else if (bench_name == "tpcc") {
    tpcc_client = tpcc_cli;
    tpcc_workgen_arr = tpcc_client->CreateWorkgenArray();
    thread_local_try_times = new uint64_t[TPCC_TX_TYPES]();
    thread_local_commit_times = new uint64_t[TPCC_TX_TYPES]();
  } else if (bench_name == "micro") {
    thread_local_try_times = new uint64_t[MICRO_TX_TYPES]();
    thread_local_commit_times = new uint64_t[MICRO_TX_TYPES]();
  }

  thread_gid = params->thread_global_id;
  thread_local_id = params->thread_local_id;
  meta_man = params->global_meta_man;
  coro_num = (coro_id_t)params->coro_num;
  coro_sched = new CoroutineScheduler(thread_gid, coro_num);

  addr_cache = params->addr_cache;

  auto alloc_rdma_region_range = params->global_rdma_region->GetThreadLocalRegion(thread_local_id);
  rdma_buffer_allocator = new LocalBufferAllocator(alloc_rdma_region_range.first, alloc_rdma_region_range.second);

  std::unordered_map<node_id_t, DeltaRange> thread_delta_region;
  params->global_delta_region->GetThreadDeltaRegion(thread_gid, thread_delta_region);

  delta_offset_allocator = new RemoteDeltaOffsetAllocator(thread_delta_region);

  char* p = (char*)(params->global_locked_key_table);
  p += sizeof(LockedKeyTable) * thread_local_id * coro_num;
  locked_key_table = (LockedKeyTable*)p;
  tp_probe_list = thread_tp_probe;

  timer = new double[ATTEMPTED_NUM]();

  // Initialize Zipf generator for MICRO benchmark
  if (bench_name == "micro") {
    uint64_t zipf_seed = 2 * thread_gid * GetCPUCycle();
    uint64_t zipf_seed_mask = (uint64_t(1) << 48) - 1;
    std::string micro_config_filepath = "../../../config/micro_config.json";
    auto json_config = JsonConfig::load_file(micro_config_filepath);
    auto micro_conf = json_config.get("micro");
    num_keys_global = micro_conf.get("num_keys").get_int64();
    auto zipf_theta = micro_conf.get("zipf_theta").get_double();
    is_skewed = micro_conf.get("is_skewed").get_bool();
    write_ratio = micro_conf.get("write_ratio").get_uint64();
    data_set_size = micro_conf.get("data_set_size").get_uint64();
    zipf_gen = new ZipfGen(num_keys_global, zipf_theta, zipf_seed & zipf_seed_mask);
  }

  // Init coroutine random gens specialized for TPCC benchmark
  random_generator = new FastRandom[coro_num];

  // Guarantee that each thread has a global different initial seed
  seed = 0xdeadbeef + thread_gid;

  // Init coroutines
  for (coro_id_t coro_i = 0; coro_i < coro_num; coro_i++) {
    uint64_t coro_seed = static_cast<uint64_t>((static_cast<uint64_t>(thread_gid) << 32) | static_cast<uint64_t>(coro_i));
    random_generator[coro_i].SetSeed(coro_seed);
    coro_sched->coro_array[coro_i].coro_id = coro_i;
    // Bind workload to coroutine
    if (coro_i == POLL_ROUTINE_ID) {
      coro_sched->coro_array[coro_i].func = coro_call_t(bind(Poll, _1));
    } else {
      if (bench_name == "tatp") {
        coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunTATP, _1, coro_i));
      } else if (bench_name == "smallbank") {
        coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunSmallBank, _1, coro_i));
      } else if (bench_name == "tpcc") {
        coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunTPCC, _1, coro_i, 0));
      } else if (bench_name == "micro") {
        coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunMICRO, _1, coro_i));
      }
    }
  }

  // Link all coroutines via pointers in a loop manner
  coro_sched->LoopLinkCoroutine(coro_num);

  // Build qp connection in thread granularity
  qp_man = new QPManager(thread_gid);
  qp_man->BuildQPConnection(meta_man);

  // Sync qp connections in one compute node before running transactions
  connected_t_num += 1;
  while (connected_t_num != params->running_tnum) {
    usleep(100);  // wait for all threads connections
  }

  // Start the first coroutine
  coro_sched->coro_array[0].func();

  mux.lock();

  delta_usage.push_back(delta_offset_allocator->GetDeltaUsage());

  mux.unlock();

  // Clean
  delete[] timer;
  if (tatp_workgen_arr) delete[] tatp_workgen_arr;
  if (smallbank_workgen_arr) delete[] smallbank_workgen_arr;
  if (tpcc_workgen_arr) delete[] tpcc_workgen_arr;
  if (random_generator) delete[] random_generator;
  if (zipf_gen) delete zipf_gen;
  delete coro_sched;
  delete thread_local_try_times;
  delete thread_local_commit_times;

  // RDMA_LOG(INFO) << "Thread " << thread_gid << " finishes";
}

void recovery(thread_params* params,
              TATP* tatp_cli,
              SmallBank* smallbank_cli,
              TPCC* tpcc_cli,
              int finished_num,
              std::vector<TpProbe>* thread_tp_probe,
              t_id_t crasher) {
  auto bench_name = params->bench_name;
  std::string config_filepath = "../../../config/" + bench_name + "_config.json";

  auto json_config = JsonConfig::load_file(config_filepath);
  auto conf = json_config.get(bench_name);
  ATTEMPTED_NUM = conf.get("attempted_num").get_uint64();

  if (bench_name == "tatp") {
    tatp_client = tatp_cli;
    tatp_workgen_arr = tatp_client->CreateWorkgenArray();
    thread_local_try_times = new uint64_t[TATP_TX_TYPES]();
    thread_local_commit_times = new uint64_t[TATP_TX_TYPES]();
  } else if (bench_name == "smallbank") {
    smallbank_client = smallbank_cli;
    smallbank_workgen_arr = smallbank_client->CreateWorkgenArray();
    thread_local_try_times = new uint64_t[SmallBank_TX_TYPES]();
    thread_local_commit_times = new uint64_t[SmallBank_TX_TYPES]();
  } else if (bench_name == "tpcc") {
    tpcc_client = tpcc_cli;
    tpcc_workgen_arr = tpcc_client->CreateWorkgenArray();
    thread_local_try_times = new uint64_t[TPCC_TX_TYPES]();
    thread_local_commit_times = new uint64_t[TPCC_TX_TYPES]();
  } else if (bench_name == "micro") {
    thread_local_try_times = new uint64_t[MICRO_TX_TYPES]();
    thread_local_commit_times = new uint64_t[MICRO_TX_TYPES]();
  }

  thread_gid = params->thread_global_id;
  thread_local_id = params->thread_local_id;
  meta_man = params->global_meta_man;
  coro_num = (coro_id_t)params->coro_num;
  coro_sched = new CoroutineScheduler(thread_gid, coro_num);

  addr_cache = params->addr_cache;

  auto alloc_rdma_region_range = params->global_rdma_region->GetThreadLocalRegion(thread_local_id);
  rdma_buffer_allocator = new LocalBufferAllocator(alloc_rdma_region_range.first, alloc_rdma_region_range.second);

  std::unordered_map<node_id_t, DeltaRange> thread_delta_region;
  params->global_delta_region->GetThreadDeltaRegion(thread_gid, thread_delta_region);

  delta_offset_allocator = new RemoteDeltaOffsetAllocator(thread_delta_region);

  char* p = (char*)(params->global_locked_key_table);
  p += sizeof(LockedKeyTable) * thread_local_id * coro_num;
  locked_key_table = (LockedKeyTable*)p;
  tp_probe_list = thread_tp_probe;

  timer = new double[ATTEMPTED_NUM]();

  // Initialize Zipf generator for MICRO benchmark
  if (bench_name == "micro") {
    uint64_t zipf_seed = 2 * thread_gid * GetCPUCycle();
    uint64_t zipf_seed_mask = (uint64_t(1) << 48) - 1;
    std::string micro_config_filepath = "../../../config/micro_config.json";
    auto json_config = JsonConfig::load_file(micro_config_filepath);
    auto micro_conf = json_config.get("micro");
    num_keys_global = micro_conf.get("num_keys").get_int64();
    auto zipf_theta = micro_conf.get("zipf_theta").get_double();
    is_skewed = micro_conf.get("is_skewed").get_bool();
    write_ratio = micro_conf.get("write_ratio").get_uint64();
    data_set_size = micro_conf.get("data_set_size").get_uint64();
    zipf_gen = new ZipfGen(num_keys_global, zipf_theta, zipf_seed & zipf_seed_mask);
  }

  // Init coroutine random gens specialized for TPCC benchmark
  random_generator = new FastRandom[coro_num];

  // Guarantee that each thread has a global different initial seed
  seed = 0xdeadbeef + thread_gid;

  // Init coroutines
  for (coro_id_t coro_i = 0; coro_i < coro_num; coro_i++) {
    uint64_t coro_seed = static_cast<uint64_t>((static_cast<uint64_t>(thread_gid) << 32) | static_cast<uint64_t>(coro_i));
    random_generator[coro_i].SetSeed(coro_seed);
    coro_sched->coro_array[coro_i].coro_id = coro_i;
    // Bind workload to coroutine
    if (coro_i == POLL_ROUTINE_ID) {
      coro_sched->coro_array[coro_i].func = coro_call_t(bind(Poll, _1));
    } else {
      if (bench_name == "tatp") {
        coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunTATP, _1, coro_i));
      } else if (bench_name == "smallbank") {
        coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunSmallBank, _1, coro_i));
      } else if (bench_name == "tpcc") {
        coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunTPCC, _1, coro_i, 0));
      } else if (bench_name == "micro") {
        coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunMICRO, _1, coro_i));
      }
    }
  }

  // Link all coroutines via pointers in a loop manner
  coro_sched->LoopLinkCoroutine(coro_num);

  // Build qp connection in thread granularity
  qp_man = new QPManager(thread_gid);

  // RDMA_LOG(INFO) << "Thread " << params->thread_global_id << " starts recover. Builinding connection...";

  qp_man->BuildQPConnection(meta_man);

  // Sync qp connections in one compute node before running transactions
  connected_recovery_t_num += 1;
  while (connected_recovery_t_num != params->running_tnum) {
    usleep(100);  // wait for all threads connections
  }

#if HAVE_COORD_CRASH
  if (thread_local_id == 30) {
    // Print time
    time_t tt;
    struct timeval tv_;
    struct tm* timeinfo;
    long tv_ms = 0, tv_us = 0;
    char output[20];
    time(&tt);
    timeinfo = localtime(&tt);
    gettimeofday(&tv_, NULL);
    strftime(output, 20, "%Y-%m-%d %H:%M:%S", timeinfo);
    tv_ms = tv_.tv_usec / 1000;
    tv_us = tv_.tv_usec % 1000;
    printf("all connected at :%s %ld:%ld\r\n", output, tv_ms, tv_us);
  }
#endif

  char* t = (char*)(params->global_locked_key_table);
  t += sizeof(LockedKeyTable) * crasher * coro_num;
  LockedKeyTable* target = (LockedKeyTable*)t;

  // Release locks
  for (int i = 0; i < coro_num; i++) {
    int num_entry = target[i].num_entry;
    if (num_entry) {
      // TLOG(INFO, thread_gid) << "coro: " << i << " op_log_size (B): " << sizeof(tx_id_t) + sizeof(num_entry) + num_entry * sizeof(LockedKeyEntry);
      for (int j = 0; j < num_entry; j++) {
        char* cas_buf = rdma_buffer_allocator->Alloc(sizeof(lock_t));
        *(lock_t*)cas_buf = 0;

        auto* qp = qp_man->GetRemoteDataQPWithNodeID(target[i].entries[j].remote_node);
        qp->post_cas(cas_buf, target[i].entries[j].remote_off, target[i].tx_id, 0, 0);
      }
    }
  }

#if HAVE_COORD_CRASH
  if (thread_local_id == 30) {
    // Print time
    time_t tt;
    struct timeval tv_;
    struct tm* timeinfo;
    long tv_ms = 0, tv_us = 0;
    char output[20];
    time(&tt);
    timeinfo = localtime(&tt);
    gettimeofday(&tv_, NULL);
    strftime(output, 20, "%Y-%m-%d %H:%M:%S", timeinfo);
    tv_ms = tv_.tv_usec / 1000;
    tv_us = tv_.tv_usec % 1000;
    printf("release lock at :%s %ld:%ld\r\n", output, tv_ms, tv_us);
  }
#endif

  coro_sched->coro_array[0].func();

  mux.lock();

  delta_usage.push_back(delta_offset_allocator->GetDeltaUsage());

  mux.unlock();

  // Clean
  delete[] timer;
  if (tatp_workgen_arr) delete[] tatp_workgen_arr;
  if (smallbank_workgen_arr) delete[] smallbank_workgen_arr;
  if (tpcc_workgen_arr) delete[] tpcc_workgen_arr;
  if (random_generator) delete[] random_generator;
  if (zipf_gen) delete zipf_gen;
  delete coro_sched;
  delete thread_local_try_times;
  delete thread_local_commit_times;
}

#if 0
// MicroTxType tmp_micro_txn_type[] = {
//     MicroTxType::kTxTest1,
//     MicroTxType::kTxTest2,
//     MicroTxType::kTxTest3,
//     MicroTxType::kTxTest4,
//     MicroTxType::kTxTest5,
//     MicroTxType::kTxTest6,
//     MicroTxType::kTxTest7,
//     MicroTxType::kTxTest8,
//     MicroTxType::kTxTest9,
//     MicroTxType::kTxTest10,
//     MicroTxType::kTxTest9,
//     MicroTxType::kTxTest10,
//     MicroTxType::kTxTest9,
//     MicroTxType::kTxTest10,
//     MicroTxType::kTxTest11,
//     MicroTxType::kTxTest11,
//     MicroTxType::kTxTest11,
//     MicroTxType::kTxTest11,
//     MicroTxType::kTxTest11,
//     MicroTxType::kTxTest11,
//     MicroTxType::kTxTest11,
//     MicroTxType::kTxTest11,
//     MicroTxType::kTxTest11,
//     MicroTxType::kTxTest11,
//     MicroTxType::kTxTest11,
//     MicroTxType::kTxTest11,
//     MicroTxType::kTxTest11,
//     MicroTxType::kTxTest11,
//     MicroTxType::kTxTest12};

// MicroTxType tmp_micro_txn_type[] = {
//   MicroTxType::kTxTest101,
//   MicroTxType::kTxTest100,
//   MicroTxType::kTxTest101,
//   MicroTxType::kTxTest100,
//   MicroTxType::kTxTest101,
//   MicroTxType::kTxTest100,
//   MicroTxType::kTxTest101,
//   MicroTxType::kTxTest100,
//   MicroTxType::kTxTest101,
//   MicroTxType::kTxTest100,
//   MicroTxType::kTxTest101,
//   MicroTxType::kTxTest100,
//   MicroTxType::kTxTest101,
//   MicroTxType::kTxTest100,
//   MicroTxType::kTxTest101,
//   MicroTxType::kTxTest100,
//   MicroTxType::kTxTest101,
//   MicroTxType::kTxTest100,
//   MicroTxType::kTxTest101,
//   MicroTxType::kTxTest100,
//   MicroTxType::kTxTest101,
//   MicroTxType::kTxTest101,
//   MicroTxType::kTxTest101,
//   MicroTxType::kTxTest101,
//   MicroTxType::kTxTest101,
//   MicroTxType::kTxTest101
//   };

// void RunMICRO(coro_yield_t& yield, coro_id_t coro_id) {
//   double total_msr_us = 0;
//   // Each coroutine has a txn: Each coroutine is a coordinator
//   TXN* txn = new TXN(meta_man,
//                      qp_man,
//                      thread_gid,
//                      coro_id,
//                      coro_sched,
//                      rdma_buffer_allocator,
//                      delta_offset_allocator,
//                      locked_key_table,
//                      addr_cache);
//   struct timespec tx_start_time, tx_end_time;
//   bool tx_committed = false;

//   // Running transactions
//   while (true) {
//     uint64_t iter = ++tx_id_generator;  // Global atomic transaction id
//     MicroTxType tx_type = tmp_micro_txn_type[stat_attempted_tx_total];

//     stat_attempted_tx_total++;

//     switch (tx_type) {
//       case MicroTxType::kTxTest100: {
//         tx_committed = TxTest100(zipf_gen, &seed, yield, iter, txn, is_skewed, data_set_size, num_keys_global, write_ratio);
//         if (tx_committed) {
//           // RDMA_LOG(DBG) << "Tx " << iter << " commits :)";
//         } else
//           RDMA_LOG(DBG) << "Tx " << iter << " aborts :)";

//         break;
//       }
//       case MicroTxType::kTxTest101: {
//         tx_committed = TxTest101(zipf_gen, &seed, yield, iter, txn, is_skewed, data_set_size, num_keys_global, write_ratio);
//         if (tx_committed) {
//           // RDMA_LOG(DBG) << "Tx " << iter << " commits :)";
//         } else
//           RDMA_LOG(DBG) << "Tx " << iter << " aborts :)";

//         break;
//       }
//       case MicroTxType::kTxTest1: {
//         tx_committed = TxTest1(zipf_gen, &seed, yield, iter, txn, is_skewed, data_set_size, num_keys_global, write_ratio);
//         if (tx_committed) {
//           RDMA_LOG(DBG) << "TxTest1 commits :)";
//         } else
//           RDMA_LOG(EMPH) << "TxTest1 aborts :(";

//         break;
//       }
//       case MicroTxType::kTxTest2: {
//         tx_committed = TxTest2(zipf_gen, &seed, yield, iter, txn, is_skewed, data_set_size, num_keys_global, write_ratio);
//         if (tx_committed) {
//           RDMA_LOG(DBG) << "TxTest2 commits :)";
//         } else
//           RDMA_LOG(EMPH) << "TxTest2 aborts :(";

//         break;
//       }
//       case MicroTxType::kTxTest3: {
//         tx_committed = TxTest3(zipf_gen, &seed, yield, iter, txn, is_skewed, data_set_size, num_keys_global, write_ratio);
//         if (tx_committed) {
//           RDMA_LOG(DBG) << "TxTest3 commits :)";
//         } else
//           RDMA_LOG(EMPH) << "TxTest3 aborts :(";

//         break;
//       }
//       case MicroTxType::kTxTest4: {
//         tx_committed = TxTest4(zipf_gen, &seed, yield, iter, txn, is_skewed, data_set_size, num_keys_global, write_ratio);
//         if (tx_committed) {
//           RDMA_LOG(DBG) << "TxTest4 commits :)";
//         } else
//           RDMA_LOG(EMPH) << "TxTest4 aborts :(";

//         break;
//       }
//       case MicroTxType::kTxTest5: {
//         tx_committed = TxTest5(zipf_gen, &seed, yield, iter, txn, is_skewed, data_set_size, num_keys_global, write_ratio);
//         if (tx_committed) {
//           RDMA_LOG(DBG) << "TxTest5 commits :)";
//         } else
//           RDMA_LOG(EMPH) << "TxTest5 aborts :(";

//         break;
//       }
//       case MicroTxType::kTxTest6: {
//         tx_committed = TxTest6(zipf_gen, &seed, yield, iter, txn, is_skewed, data_set_size, num_keys_global, write_ratio);
//         if (tx_committed) {
//           RDMA_LOG(DBG) << "TxTest6 commits :)";
//         } else
//           RDMA_LOG(EMPH) << "TxTest6 aborts :(";

//         break;
//       }
//       case MicroTxType::kTxTest7: {
//         tx_committed = TxTest7(zipf_gen, &seed, yield, iter, txn, is_skewed, data_set_size, num_keys_global, write_ratio);
//         if (tx_committed) {
//           RDMA_LOG(DBG) << "TxTest7 commits :)";
//         } else
//           RDMA_LOG(EMPH) << "TxTest7 aborts :(";

//         break;
//       }
//       case MicroTxType::kTxTest8: {
//         tx_committed = TxTest8(zipf_gen, &seed, yield, iter, txn, is_skewed, data_set_size, num_keys_global, write_ratio);
//         if (tx_committed) {
//           RDMA_LOG(DBG) << "TxTest8 commits :)";
//         } else
//           RDMA_LOG(EMPH) << "TxTest8 aborts :(";

//         break;
//       }
//       case MicroTxType::kTxTest9: {
//         tx_committed = TxTest9(zipf_gen, &seed, yield, iter, txn, is_skewed, data_set_size, num_keys_global, write_ratio);
//         if (tx_committed) {
//           RDMA_LOG(DBG) << "TxTest9 commits :)";
//         } else
//           RDMA_LOG(EMPH) << "TxTest9 aborts :(";

//         break;
//       }
//       case MicroTxType::kTxTest10: {
//         tx_committed = TxTest10(zipf_gen, &seed, yield, iter, txn, is_skewed, data_set_size, num_keys_global, write_ratio);
//         if (tx_committed) {
//           RDMA_LOG(DBG) << "TxTest10 commits :)";
//         } else
//           RDMA_LOG(EMPH) << "TxTest10 aborts :(";

//         break;
//       }
//       case MicroTxType::kTxTest11: {
//         tx_committed = TxTest11(zipf_gen, &seed, yield, iter, txn, is_skewed, data_set_size, num_keys_global, write_ratio);
//         if (tx_committed) {
//           RDMA_LOG(DBG) << "TxTest11 commits :)";
//         } else
//           RDMA_LOG(EMPH) << "TxTest11 aborts :(";

//         break;
//       }
//       case MicroTxType::kTxTest12: {
//         tx_committed = TxTest12(zipf_gen, &seed, yield, iter, txn, is_skewed, data_set_size, num_keys_global, write_ratio);
//         if (tx_committed) {
//           RDMA_LOG(DBG) << "TxTest12 commits :)";
//         } else
//           RDMA_LOG(EMPH) << "TxTest12 aborts :(";

//         break;
//       }

//       default:
//         RDMA_LOG(DBG) << " ==================== All finishes ===================";
//         break;
//     }
//     /********************************** Stat begin *****************************************/
//     if (stat_attempted_tx_total >= ATTEMPTED_NUM) {
//       break;
//     }
//   }

//   /********************************** Stat end *****************************************/

//   delete txn;
// }

// void RunMICRO(coro_yield_t& yield, coro_id_t coro_id) {
//   double total_msr_us = 0;
//   // Each coroutine has a txn: Each coroutine is a coordinator
//   TXN* txn = new TXN(meta_man,
//                      qp_man,
//                      thread_gid,
//                      coro_id,
//                      coro_sched,
//                      rdma_buffer_allocator,
//                      delta_offset_allocator,
//                      locked_key_table,
//                      addr_cache);
//   struct timespec tx_start_time, tx_end_time;
//   bool tx_committed = false;

//   // Running transactions
//   clock_gettime(CLOCK_REALTIME, &msr_start);
//   while (true) {
//     uint64_t iter = ++tx_id_generator;  // Global atomic transaction id
//     stat_attempted_tx_total++;

//     thread_local_try_times[uint64_t(MicroTxType::kRWOne)]++;
//     clock_gettime(CLOCK_REALTIME, &tx_start_time);

//     tx_committed = TxRWOne(zipf_gen, &seed, yield, iter, txn, is_skewed, data_set_size, num_keys_global, write_ratio);

//     /********************************** Stat begin *****************************************/
//     // Stat after one transaction finishes
//     if (tx_committed) {
//       clock_gettime(CLOCK_REALTIME, &tx_end_time);
//       thread_local_commit_times[uint64_t(MicroTxType::kRWOne)]++;
//       double tx_usec = (tx_end_time.tv_sec - tx_start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - tx_start_time.tv_nsec) / 1000;
//       timer[stat_committed_tx_total++] = tx_usec;
//     }

//     if (stat_attempted_tx_total >= ATTEMPTED_NUM) {
//       // A coroutine calculate the total execution time and exits
//       clock_gettime(CLOCK_REALTIME, &msr_end);
//       // double msr_usec = (msr_end.tv_sec - msr_start.tv_sec) * 1000000 + (double) (msr_end.tv_nsec - msr_start.tv_nsec) / 1000;
//       double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;
//       RecordTpLat(msr_sec);
//       break;
//     }
//   }

//   /********************************** Stat end *****************************************/

//   delete txn;
// }
#endif

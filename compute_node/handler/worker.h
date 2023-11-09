// Author: Ming Zhang
// Copyright (c) 2023

#pragma once

#include "allocator/region_allocator.h"
#include "base/common.h"
#include "cache/addr_cache.h"
#include "connection/meta_manager.h"
#include "micro/micro_db.h"
#include "smallbank/smallbank_db.h"
#include "tatp/tatp_db.h"
#include "tpcc/tpcc_db.h"
#include "process/oplog.h"

struct thread_params {
  t_id_t thread_local_id;
  t_id_t thread_global_id;
  t_id_t running_tnum;
  MetaManager* global_meta_man;
  AddrCache* addr_cache;
  LocalRegionAllocator* global_rdma_region;
  RemoteDeltaRegionAllocator* global_delta_region;
  LockedKeyTable* global_locked_key_table;
  int coro_num;
  std::string bench_name;
};

struct TpProbe {
  int ctr;
  double tp;
  double attemp_tp;
};

void run_thread(thread_params* params,
                TATP* tatp_client,
                SmallBank* smallbank_client,
                TPCC* tpcc_client,
                std::vector<TpProbe>* thread_tp_probe);

void recovery(thread_params* params,
              TATP* tatp_client,
              SmallBank* smallbank_client,
              TPCC* tpcc_client,
              int finished_num,
              std::vector<TpProbe>* thread_tp_probe,
              t_id_t crasher);

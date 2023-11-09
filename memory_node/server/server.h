// Author: Ming Zhang
// Copyright (c) 2023

#pragma once

#include <sys/mman.h>

#include <cstdio>
#include <cstring>
#include <string>

#include "memstore/cvt.h"
#include "memstore/hash_store.h"
#include "rlib/rdma_ctrl.hpp"

// Load DB
#include "micro/micro_db.h"
#include "smallbank/smallbank_db.h"
#include "tatp/tatp_db.h"
#include "tpcc/tpcc_db.h"

using namespace rdmaio;

class Server {
 public:
  Server(int nid,
         int local_port,
         int local_meta_port,
         size_t data_size,
         size_t delta_size,
         int use_pm,
         std::string& pm_file)
      : server_node_id(nid),
        local_port(local_port),
        local_meta_port(local_meta_port),
        data_size(data_size),
        delta_size(delta_size),
        use_pm(use_pm),
        pm_file(pm_file),
        pm_file_fd(0),
        mem_region(nullptr),
        hash_buffer(nullptr) {}

  ~Server() {
    RDMA_LOG(INFO) << "Do server cleaning...";
    if (tatp_server) {
      delete tatp_server;
      RDMA_LOG(INFO) << "delete tatp tables";
    }

    if (smallbank_server) {
      delete smallbank_server;
      RDMA_LOG(INFO) << "delete smallbank tables";
    }

    if (tpcc_server) {
      delete tpcc_server;
      RDMA_LOG(INFO) << "delete tpcc tables";
    }

    if (micro_server) {
      delete micro_server;
      RDMA_LOG(INFO) << "delete micro tables";
    }

    if (use_pm) {
      munmap(mem_region, data_size + delta_size);
      close(pm_file_fd);
      RDMA_LOG(INFO) << "munmap mr";
    } else {
      if (mem_region) {
        free(mem_region);
        RDMA_LOG(INFO) << "Free mr";
      }
    }
  }

  void AllocMem();

  void InitMem();

  void InitRDMA();

  void ConnectMN();

  void LoadData(node_id_t machine_id, node_id_t machine_num, std::string& workload);

  void SendMeta(node_id_t machine_id,
                std::string& workload,
                size_t compute_node_num,
                offset_t delta_start_off,
                size_t per_thread_delta_size);

  void PrepareHashMeta(node_id_t machine_id,
                       std::string& workload,
                       char** hash_meta_buffer,
                       size_t& total_meta_size,
                       offset_t delta_start_off,
                       size_t per_thread_delta_size);

  void SendHashMeta(char* hash_meta_buffer, size_t& total_meta_size);

  void AcceptReq();

  void CleanTable();

  void CleanQP();

  bool Run(std::string& workload);

  void OutputMemoryFootprint(std::string& workload);

 private:
  const int server_node_id;

  const int local_port;

  const int local_meta_port;

  const size_t data_size;

  const size_t delta_size;

  const int use_pm;

  const std::string pm_file;

  int pm_file_fd;

  // The start address of the whole MR
  char* mem_region;

  // The start address of the whole hash store space
  char* hash_buffer;

  // char* b+tree_buffer;

  // For server-side workload
  TATP* tatp_server = nullptr;

  SmallBank* smallbank_server = nullptr;

  TPCC* tpcc_server = nullptr;

  MICRO* micro_server = nullptr;

  RdmaCtrlPtr rdma_ctrl;

  std::unordered_map<node_id_t, MemoryAttr> other_mn_mrs;

  RCQP* other_mn_qps[MAX_REMOTE_NODE_NUM]{nullptr};
};

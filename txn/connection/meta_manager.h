// Author: Ming Zhang
// Copyright (c) 2023

#pragma once

#include <atomic>
#include <unordered_map>

#include "base/common.h"
#include "memstore/hash_store.h"
#include "rlib/rdma_ctrl.hpp"

extern std::atomic<bool> primary_fail;
extern std::atomic<bool> cannot_lock_new_primary;

extern std::atomic<bool> one_backup_fail;
extern std::atomic<bool> during_backup_recovery;

using namespace rdmaio;

struct RemoteNode {
  node_id_t node_id;
  std::string ip;
  int port;
  int meta_port;
};

enum PrimaryCrashTime : int {
  kBeforeCommit = 0,
  kAtAbort,
  kDuringCommit
};

class MetaManager {
 public:
  MetaManager();

  node_id_t GetMemStoreMeta(std::string& remote_ip, int remote_port);

  void GetMRMeta(const RemoteNode& node) {
    // Get remote node's memory region information via TCP
    MemoryAttr remote_hash_mr{};

    while (QP::get_remote_mr(node.ip, node.port, SERVER_HASH_BUFF_ID, &remote_hash_mr) != SUCC) {
      usleep(2000);
    }

    remote_hash_mrs[node.node_id] = remote_hash_mr;
  }

  /*** Memory Store Metadata ***/
  const HashMeta& GetPrimaryHashMetaWithTableID(const table_id_t table_id) const {
    auto search = primary_hash_metas.find(table_id);
    assert(search != primary_hash_metas.end());
    return search->second;
  }

  const std::vector<HashMeta>* GetBackupHashMetasWithTableID(const table_id_t table_id) const {
    // if (backup_hash_metas.empty()) {
    //   return nullptr;
    // }
    // auto search = backup_hash_metas.find(table_id);
    // assert(search != backup_hash_metas.end());
    // return &(search->second);
    return &(backup_hash_metas[table_id]);
  }

  /*** Node ID Metadata ***/
  node_id_t GetPrimaryNodeIDWithCrash(const table_id_t table_id, PrimaryCrashTime p_crash_t = PrimaryCrashTime::kBeforeCommit) {
#if HAVE_PRIMARY_CRASH
    if (table_id == CRASH_TABLE_ID) {
      if (p_crash_t == PrimaryCrashTime::kBeforeCommit) {
        while (cannot_lock_new_primary) {
          // Wait until the in-flight txn commits before I can see the primary (even though the primary has been already reocvered)
          ;
        }
      }
      if (primary_fail) {
        return PRIMARY_CRASH;
      }
    }
    // Note that, a backup failure does not block accesses to primary, since coordinators
    // do not read data from backup in default
#endif

#if HAVE_BACKUP_CRASH
    if (table_id == CRASH_TABLE_ID) {
      if (during_backup_recovery) {
        return BACKUP_CRASH;
      }
    }
#endif

    auto search = primary_table_nodes.find(table_id);
    assert(search != primary_table_nodes.end());
    return search->second;
  }

  node_id_t GetPrimaryNodeID(const table_id_t table_id) {
    auto search = primary_table_nodes.find(table_id);
    assert(search != primary_table_nodes.end());
    return search->second;
  }

  const std::vector<node_id_t>* GetBackupNodeIDWithCrash(const table_id_t table_id, bool& need_recovery) {
#if HAVE_BACKUP_CRASH

    if (table_id == CRASH_TABLE_ID) {
      if (one_backup_fail) {
        need_recovery = true;
      }
    }

#endif
    return &(backup_table_nodes[table_id]);
  }

  /*** RDMA Memory Region Metadata ***/
  const MemoryAttr& GetRemoteHashMR(const node_id_t node_id) const {
    auto mrsearch = remote_hash_mrs.find(node_id);
    assert(mrsearch != remote_hash_mrs.end());
    return mrsearch->second;
  }

  const size_t GetPerThreadDeltaSize() const {
    return per_thread_delta_size;
  }

  const offset_t GetDeltaStartOffset() const {
    return delta_start_off;
  }

  void GetRemoteIP(node_id_t nid, std::string& r_ip, int& r_metaport) {
    for (int i = 0; i < remote_nodes.size(); i++) {
      if (remote_nodes[i].node_id == nid) {
        r_ip = remote_nodes[i].ip;
        r_metaport = remote_nodes[i].meta_port;
        return;
      }
    }
    RDMA_LOG(FATAL) << "GetRemoteIP fails. wanted remote node id: " << nid;
  }

  void ChangePrimary(const table_id_t table_id) {
    auto p_node_search = primary_table_nodes.find(table_id);
    auto old_p_id = p_node_search->second;
    primary_table_nodes.erase(p_node_search);

    auto p_hash_search = primary_hash_metas.find(table_id);
    auto old_p_hash_meta = p_hash_search->second;
    primary_hash_metas.erase(p_hash_search);

    std::vector<node_id_t>& backup_nodes = backup_table_nodes[table_id];
    std::vector<HashMeta>& backup_hashs = backup_hash_metas[table_id];

    auto vp = backup_nodes.front();
    auto vp_hash_meta = backup_hashs.front();

    backup_nodes.erase(backup_nodes.begin());
    backup_hashs.erase(backup_hashs.begin());

    primary_table_nodes[table_id] = vp;
    primary_hash_metas[table_id] = vp_hash_meta;

    backup_nodes.push_back(old_p_id);
    backup_hashs.push_back(old_p_hash_meta);
  }

 private:
  std::unordered_map<table_id_t, node_id_t> primary_table_nodes;
  std::unordered_map<table_id_t, HashMeta> primary_hash_metas;

  std::vector<node_id_t> backup_table_nodes[MAX_DB_TABLE_NUM];
  std::vector<HashMeta> backup_hash_metas[MAX_DB_TABLE_NUM];

  std::unordered_map<node_id_t, MemoryAttr> remote_hash_mrs;

  node_id_t local_machine_id;


 public:
  offset_t delta_start_off;

  size_t per_thread_delta_size;
  // Used by QP manager and RDMA Region
  RdmaCtrlPtr global_rdma_ctrl;

  std::vector<RemoteNode> remote_nodes;

  RNicHandler* opened_rnic;

  int64_t iso_level;  // Guarantee which isolation level
};

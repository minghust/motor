// Author: Ming Zhang
// Copyright (c) 2023

#pragma once

#include "connection/meta_manager.h"

// This QPManager builds qp connections (compute node <-> memory node) for each txn thread in each compute node
class QPManager {
 public:
  QPManager(t_id_t global_tid) : global_tid(global_tid) {}
  
  ~QPManager() {
    for (int i = 0; i < MAX_REMOTE_NODE_NUM; i++) {
      if (data_qps[i]) {
        delete data_qps[i];
      }
    }
  }

  void BuildQPConnection(MetaManager* meta_man) {
    for (const auto& remote_node : meta_man->remote_nodes) {
      // Note that each remote machine has one MemStore mr and one Log mr
      MemoryAttr remote_hash_mr = meta_man->GetRemoteHashMR(remote_node.node_id);

      // Build QPs with one remote machine (this machine can be a primary or a backup)
      // Create the thread local queue pair
      MemoryAttr local_mr = meta_man->global_rdma_ctrl->get_local_mr(CLIENT_MR_ID);
      RCQP* data_qp = meta_man->global_rdma_ctrl->create_rc_qp(create_rc_idx(remote_node.node_id, (int)global_tid),
                                                               meta_man->opened_rnic,
                                                               &local_mr);

      // Queue pair connection, exchange queue pair info via TCP
      ConnStatus rc;
      do {
        rc = data_qp->connect(remote_node.ip, remote_node.port);
        if (rc == SUCC) {
          // Bind the hash mr as the default remote mr for convenient parameter passing
          data_qp->bind_remote_mr(remote_hash_mr);

          data_qps[remote_node.node_id] = data_qp;
          // RDMA_LOG(INFO) << "Thread " << global_tid << ": Data QP connected! with remote node: " << remote_node.node_id << " ip: " << remote_node.ip;
        }
        usleep(2000);
      } while (rc != SUCC);
    }
  }

  ALWAYS_INLINE
  RCQP* GetRemoteDataQPWithNodeID(const node_id_t node_id) const {
    return data_qps[node_id];
  }

  ALWAYS_INLINE
  void GetRemoteDataQPsWithNodeIDs(const std::vector<node_id_t>* node_ids, std::vector<RCQP*>& qps) {
    for (node_id_t node_id : *node_ids) {
      RCQP* qp = data_qps[node_id];
      if (qp) {
        qps.push_back(qp);
      }
    }
  }

 private:
  RCQP* data_qps[MAX_REMOTE_NODE_NUM]{nullptr};

  t_id_t global_tid;
};

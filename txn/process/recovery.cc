// Author: Ming Zhang
// Copyright (c) 2023

#include "process/txn.h"

std::mutex recover_primary_mux;
std::mutex recover_backup_mux;

void TXN::RecoverPrimary(table_id_t table_id, PrimaryCrashTime p_crash_time) {
  if (t_id == 0 || p_crash_time == PrimaryCrashTime::kDuringCommit) {
    recover_primary_mux.lock();

    if (!primary_fail) {
      // only need one coordinator to do recovery
      recover_primary_mux.unlock();
      return;
    }

    Timer timer;
    timer.Start();

    node_id_t orig_p_id = global_meta_man->GetPrimaryNodeID(table_id);

    // Only one can change primary view
    global_meta_man->ChangePrimary(table_id);

    node_id_t new_p_id = global_meta_man->GetPrimaryNodeID(table_id);

    SendMsgToReplica(new_p_id, orig_p_id, table_id, 1);

    primary_fail = false;

    recover_primary_mux.unlock();

    timer.Stop();

    RDMA_LOG(INFO) << "Thread: " << t_id
                   << " recovers primary of table: " << table_id
                   << ", old primary MN: " << orig_p_id << ", new primary MN: " << new_p_id
                   << ". Before commit? " << (p_crash_time == PrimaryCrashTime::kBeforeCommit ? "Yes" : "No")
                   << ". Total time consumption (us): " << timer.Duration_us();
  }
}

void TXN::RecoverBackup(table_id_t table_id, node_id_t to_recover_backup_node_id) {
  recover_backup_mux.lock();

  if (!one_backup_fail) {
    // only need one coordinator to do recovery
    recover_backup_mux.unlock();
    return;
  }

  during_backup_recovery = true;

  node_id_t p_id = global_meta_man->GetPrimaryNodeID(table_id);

  RDMA_LOG(INFO) << "Thread: " << t_id
                 << " recovers backup of table: " << table_id
                 << ", primary MN: " << p_id << ", new backup MN: " << to_recover_backup_node_id;

  Timer timer;
  timer.Start();

  SendMsgToReplica(p_id, to_recover_backup_node_id, table_id, 0);

  one_backup_fail = false;

  during_backup_recovery = false;

  recover_backup_mux.unlock();

  timer.Stop();

  RDMA_LOG(INFO) << "Total time consumption (us): " << timer.Duration_us();
}

void TXN::SendMsgToReplica(node_id_t copy_from, node_id_t copy_to, table_id_t table_id, int is_primary_fail) {
  // Informing a replica to migrate data
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  std::string remote_ip;
  int remote_metaport;
  global_meta_man->GetRemoteIP(copy_from, remote_ip, remote_metaport);

  if (inet_pton(AF_INET, remote_ip.c_str(), &server_addr.sin_addr) <= 0) {
    RDMA_LOG(FATAL) << "[SendMsgToReplica] inet_pton error: " << strerror(errno);
  }

  server_addr.sin_port = htons(remote_metaport);
  int client_socket = socket(AF_INET, SOCK_STREAM, 0);

  // The port can be used immediately after restart
  int on = 1;
  setsockopt(client_socket, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

  if (client_socket < 0) {
    RDMA_LOG(ERROR) << "[SendMsgToReplica] creates socket error: " << strerror(errno);
    close(client_socket);
    abort();
  }

  if (connect(client_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
    RDMA_LOG(ERROR) << "[SendMsgToReplica] connect error: " << strerror(errno);
    close(client_socket);
    abort();
  }

  size_t msg_len = sizeof(table_id) + sizeof(copy_to) + sizeof(is_primary_fail);
  char* msg_buf = (char*)malloc(msg_len);
  char* p = msg_buf;

  *((table_id_t*)p) = table_id;
  p += sizeof(table_id);

  *((node_id_t*)p) = copy_to;
  p += sizeof(copy_to);

  *((int*)p) = is_primary_fail;

  send(client_socket, msg_buf, msg_len, 0);

  size_t recv_size = 100;
  char* recv_buf = (char*)malloc(recv_size);

  recv(client_socket, recv_buf, recv_size, 0);

  if (strcmp(recv_buf, "MIGRATE_OK") != 0) {
    std::string ack(recv_buf);
    RDMA_LOG(FATAL) << "Client receives error ack: " << ack;
  }

  free(msg_buf);
  free(recv_buf);

  close(client_socket);
}

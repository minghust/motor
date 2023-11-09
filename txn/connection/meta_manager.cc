// Author: Ming Zhang
// Copyright (c) 2023

#include "connection/meta_manager.h"

#include "util/json_config.h"

MetaManager::MetaManager() {
  // Read config json file
  std::string config_filepath = "../../../config/cn_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);
  auto local_node = json_config.get("local_compute_node");
  local_machine_id = (node_id_t)local_node.get("machine_id").get_int64();
  iso_level = local_node.get("iso_level").get_int64();

  auto mem_nodes = json_config.get("remote_mem_nodes");
  auto remote_ips = mem_nodes.get("remote_ips");                // Array
  auto remote_ports = mem_nodes.get("remote_ports");            // Array Used for RDMA exchanges
  auto remote_meta_ports = mem_nodes.get("remote_meta_ports");  // Array Used for transferring datastore metas

  // Get remote machine's memory store meta via TCP
  for (size_t index = 0; index < remote_ips.size(); index++) {
    std::string remote_ip = remote_ips.get(index).get_str();
    int remote_meta_port = (int)remote_meta_ports.get(index).get_int64();
    // RDMA_LOG(INFO) << "get hash meta from " << remote_ip;
    node_id_t remote_machine_id = GetMemStoreMeta(remote_ip, remote_meta_port);
    if (remote_machine_id == -1) {
      RDMA_LOG(FATAL) << "Thread " << std::this_thread::get_id() << " GetMemStoreMeta() failed!, remote_machine_id = -1";
    }
    int remote_port = (int)remote_ports.get(index).get_int64();
    remote_nodes.push_back(RemoteNode{.node_id = remote_machine_id, .ip = remote_ip, .port = remote_port, .meta_port = remote_meta_port});
  }

  std::cout << "--------------\n";
  std::cout << "VNum: " << MAX_VCELL_NUM << std::endl;
  std::cout << "--------------\n";

  RDMA_LOG(INFO) << "All hash table meta received";

#if PRINT_HASH_META
  // Check all the meta received
  std::cerr << "-------------------------------------- Primary Info ---------------------------------------\n";
  std::cerr << "==> Check primary TableID-NodeID\n";
  for (auto p_t_n : primary_table_nodes) {
    std::cerr << "Primary TableID: " << p_t_n.first << " NodeID: " << p_t_n.second << std::endl;
  }

  std::cerr << "==> Check primary Hash Meta\n";
  for (auto p_meta : primary_hash_metas) {
    auto meta = p_meta.second;
    std::cerr << "Primary hash meta for TableID: " << p_meta.first << " HashMeta: "
              << "<<<table_id: " << meta.table_id << ", table_ptr: 0x" << std::hex << meta.table_ptr << ", base_off: 0x" << meta.base_off << ", bucket_num: " << std::dec << meta.bucket_num << ", bucket_size: " << meta.bucket_size << ", hash_core: " << (int)meta.hash_core << ">>>" << std::endl;
  }
  std::cerr << "-------------------------------------- Backup Info ---------------------------------------\n";

  std::cerr << "==> Check backup TableID-NodeIDs\n";

  for (size_t i = 0; i < primary_table_nodes.size(); i++) {
    std::cerr << "Backup nodes for TableID " << i << ": ";
    for (auto node : backup_table_nodes[i]) {
      std::cerr << node << ", ";
    }
    std::cerr << std::endl;
  }

  std::cerr << "==> Check backup Hash Meta\n";
  for (size_t i = 0; i < primary_table_nodes.size(); i++) {
    std::cerr << "Backup hash meta for TableID " << i << ":\n";
    for (auto meta : backup_hash_metas[i]) {
      std::cerr << "  HashMeta: <<<table_id: " << meta.table_id << ", table_ptr: 0x" << std::hex << meta.table_ptr << ", base_off: 0x" << meta.base_off << ", bucket_num: " << std::dec << meta.bucket_num << ", bucket_size: " << meta.bucket_size << ", hash_core: " << (int)meta.hash_core << " >>>" << std::endl;
    }
  }
  std::cerr << "------------------------------------------------------------------------------------------\n";
#endif

  // RDMA setup
  int local_port = (int)local_node.get("local_port").get_int64();
  global_rdma_ctrl = std::make_shared<RdmaCtrl>(local_machine_id, local_port);

  // Using the first RNIC's first port
  RdmaCtrl::DevIdx idx;
  idx.dev_id = 0;
  idx.port_id = 1;

  // Open device
  opened_rnic = global_rdma_ctrl->open_device(idx);

  for (auto& remote_node : remote_nodes) {
    GetMRMeta(remote_node);
  }
  RDMA_LOG(INFO) << "All remote mr meta received!";
}

node_id_t MetaManager::GetMemStoreMeta(std::string& remote_ip, int remote_port) {
  // Get remote memory store metadata for remote accesses, via TCP
  /* ---------------Initialize socket---------------- */
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  if (inet_pton(AF_INET, remote_ip.c_str(), &server_addr.sin_addr) <= 0) {
    RDMA_LOG(ERROR) << "MetaManager inet_pton error: " << strerror(errno);
    abort();
  }
  server_addr.sin_port = htons(remote_port);
  int client_socket = socket(AF_INET, SOCK_STREAM, 0);

  // The port can be used immediately after restart
  int on = 1;
  setsockopt(client_socket, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

  if (client_socket < 0) {
    RDMA_LOG(ERROR) << "MetaManager creates socket error: " << strerror(errno);
    close(client_socket);
    abort();
  }

  if (connect(client_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
    RDMA_LOG(ERROR) << "MetaManager connect error: " << strerror(errno);
    RDMA_LOG(ERROR) << "Memory node may have NOT finished loading data!";
    close(client_socket);
    abort();
  }

  /* --------------- Receiving hash metadata ----------------- */

  size_t hash_meta_size = (size_t)1024 * 1024 * 10;
  char* recv_buf = (char*)malloc(hash_meta_size);

  auto retlen = recv(client_socket, recv_buf, hash_meta_size, 0);
  if (retlen < 0) {
    RDMA_LOG(ERROR) << "MetaManager receives hash meta error: " << strerror(errno);
    free(recv_buf);
    close(client_socket);
    abort();
  }

  char ack[] = "[ACK]hash_meta_received_from_client";
  send(client_socket, ack, strlen(ack) + 1, 0);

  close(client_socket);

  // Parse meta
  char* snooper = recv_buf;
  // Get number of meta
  size_t primary_meta_num = *((size_t*)snooper);
  snooper += sizeof(primary_meta_num);

  size_t backup_meta_num = *((size_t*)snooper);
  snooper += sizeof(backup_meta_num);

  node_id_t remote_machine_id = *((node_id_t*)snooper);
  snooper += sizeof(remote_machine_id);

  if (remote_machine_id >= MAX_REMOTE_NODE_NUM) {
    RDMA_LOG(ERROR) << "remote machine id " << remote_machine_id << " exceeds the max machine number";
    free(recv_buf);
    abort();
  }

  delta_start_off = *((offset_t*)snooper);
  snooper += sizeof(delta_start_off);

  per_thread_delta_size = *((size_t*)snooper);
  snooper += sizeof(per_thread_delta_size);

  RDMA_LOG(DBG) << "META MAN: delta_start_off (DataRegion size, MB): " << (double)delta_start_off / 1024 / 1024 << ", per_thread_delta_size (MB): " << (double)per_thread_delta_size / 1024 / 1024;

  // Get the `end of file' indicator: finish transmitting
  char* eof = snooper + sizeof(HashMeta) * (primary_meta_num + backup_meta_num);

  // Check meta
  // std::cerr << "--------------- Check meta of remote_ip: " << remote_ip << " remote_machine_id: " << remote_machine_id << std::endl;
  if ((*((uint64_t*)eof)) == MEM_STORE_META_END) {
    // std::cerr << "==> Check primary info" << std::endl;
    for (size_t i = 0; i < primary_meta_num; i++) {
      HashMeta meta;
      memcpy(&meta, (HashMeta*)(snooper + i * sizeof(HashMeta)), sizeof(HashMeta));
      primary_hash_metas[meta.table_id] = meta;
      primary_table_nodes[meta.table_id] = remote_machine_id;
      // std::cerr << "tableID: " << meta.table_id << ", primary-nodeID: " << remote_machine_id << std::endl;
      // std::cerr << "PrimaryHashMeta: table_id: " << meta.table_id << ", table_ptr: 0x" << std::hex << meta.table_ptr << ", base_off: 0x" << meta.base_off << ", bucket_num: " << std::dec << meta.bucket_num << ", bucket_size: " << meta.bucket_size << ", hash_core: " << (int)meta.hash_core << " >>>" << std::endl;
    }

    snooper += sizeof(HashMeta) * primary_meta_num;
    // std::cerr << "==> Check backup info" << std::endl;
    for (size_t i = 0; i < backup_meta_num; i++) {
      HashMeta meta;
      memcpy(&meta, (HashMeta*)(snooper + i * sizeof(HashMeta)), sizeof(HashMeta));
      backup_hash_metas[meta.table_id].push_back(meta);
      backup_table_nodes[meta.table_id].push_back(remote_machine_id);
      // std::cerr << "tableID: " << meta.table_id << ", backup-nodeID: " << remote_machine_id << std::endl;
      // std::cerr << "BackupHashMeta: table_id: " << meta.table_id << ", table_ptr: 0x" << std::hex << meta.table_ptr << ", base_off: 0x" << meta.base_off << ", bucket_num: " << std::dec << meta.bucket_num << ", bucket_size: " << meta.bucket_size << ", hash_core: " << (int)meta.hash_core << " >>>" << std::endl;
    }

  } else {
    RDMA_LOG(ERROR) << "EOF mismatches. Received: " << std::hex << "0x" << *((uint64_t*)eof) << ", desired: " << std::hex << "0x" << MEM_STORE_META_END;
    free(recv_buf);
    abort();
  }

  free(recv_buf);
  return remote_machine_id;
}

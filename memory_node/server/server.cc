// Author: Ming Zhang
// Copyright (c) 2023

#include "server.h"

#include <stdlib.h>
#include <unistd.h>

#include <thread>

#include "util/json_config.h"

void Server::AllocMem() {
  RDMA_LOG(INFO) << "Start allocating memory...";

  if (use_pm) {
    pm_file_fd = open(pm_file.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666);
    if (pm_file_fd < 0) {
      printf("open file failed, %s\n", strerror(errno));
    }
    // 0x80003 = MAP_SHARED_VALIDATE | MAP_SYNC. Some old kernel does not have sys/mman.h: MAP_SHARED_VALIDATE, MAP_SYNC
    mem_region = (char*)mmap(0, data_size + delta_size, PROT_READ | PROT_WRITE, 0x80003, pm_file_fd, 0);

    assert(mem_region);

    RDMA_LOG(INFO) << "Alloc PM data region success!";

  } else {
    mem_region = (char*)malloc(data_size + delta_size);

    assert(mem_region);

    RDMA_LOG(INFO) << "Alloc DRAM data region success!";
  }

  hash_buffer = mem_region;  // Different indexes will occupy the memory region
}

void Server::InitMem() {
  RDMA_LOG(INFO) << "Start initializing memory...";

  memset(mem_region, 0, data_size + delta_size);

  RDMA_LOG(INFO) << "Initialize memory success!";
}

void Server::InitRDMA() {
  /************************************* RDMA Initialization ***************************************/
  RDMA_LOG(INFO) << "Start initializing RDMA...";

  rdma_ctrl = std::make_shared<RdmaCtrl>(server_node_id, local_port);
  RdmaCtrl::DevIdx idx{.dev_id = 0, .port_id = 1};  // using the first RNIC's first port
  rdma_ctrl->open_thread_local_device(idx);
  RDMA_ASSERT(rdma_ctrl->register_memory(SERVER_HASH_BUFF_ID, mem_region, data_size + delta_size, rdma_ctrl->get_device()) == true);

  RDMA_LOG(INFO) << "Register memory success!";
}

void Server::ConnectMN() {
  RDMA_LOG(INFO) << "Start connecting MNs...";

  std::string config_filepath = "../../../config/mn_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);

  auto local_node = json_config.get("local_memory_node");
  node_id_t my_mn_id = (node_id_t)local_node.get("machine_id").get_int64();

  auto other_mns = json_config.get("other_memory_nodes");
  auto other_mn_ips = other_mns.get("memory_node_ips");
  auto other_mn_ids = other_mns.get("memory_node_ids");
  auto other_mn_ports = other_mns.get("memory_node_ports");

  for (size_t index = 0; index < other_mn_ips.size(); index++) {
    std::string remote_ip = other_mn_ips.get(index).get_str();
    int remote_node_id = (int)other_mn_ids.get(index).get_int64();
    int remote_port = (int)other_mn_ports.get(index).get_int64();

    // 1. Get remote mr
    MemoryAttr remote_mr{};

    while (QP::get_remote_mr(remote_ip, remote_port, SERVER_HASH_BUFF_ID, &remote_mr) != SUCC) {
      usleep(2000);
    }

    RDMA_LOG(INFO) << "Get MR of MN ID: " << remote_node_id << " IP: " << remote_ip << " PORT: " << remote_port << " Success!";

    other_mn_mrs[remote_node_id] = remote_mr;

    // 2. Build QP connection
    MemoryAttr local_mr = rdma_ctrl->get_local_mr(SERVER_HASH_BUFF_ID);

    // 1000 is used to distinguish qps between MNs with qps between MNs and CNs.
    // This number should be larger than the total amount of CN threads in the cluster
    RCQP* data_qp = rdma_ctrl->create_rc_qp(create_rc_idx(remote_node_id, 1000 + my_mn_id),
                                            rdma_ctrl->get_device(),
                                            &local_mr);

    ConnStatus rc;
    do {
      rc = data_qp->connect(remote_ip, remote_port);
      if (rc == SUCC) {
        // Bind the hash mr as the default remote mr for convenient parameter passing
        data_qp->bind_remote_mr(remote_mr);

        other_mn_qps[remote_node_id] = data_qp;

        RDMA_LOG(INFO) << "Connect QP with MN ID: " << remote_node_id << " IP: " << remote_ip << " PORT: " << remote_port << " Success!";
      }
      usleep(2000);
    } while (rc != SUCC);
  }
}

// All servers need to load data
void Server::LoadData(node_id_t machine_id,
                      node_id_t machine_num,  // number of memory nodes
                      std::string& workload) {
  /************************************* Load Data ***************************************/
  RDMA_LOG(INFO) << "Start loading database data...";
  // Init tables
  MemStoreAllocParam mem_store_alloc_param(mem_region, hash_buffer, 0, mem_region + data_size);

  /******** Memory footprint statistics ********/
  size_t total_size = 0;

  size_t ht_loadfv_size = 0;

  size_t ht_size = 0;

  size_t initfv_size = 0;

  size_t real_cvt_size = 0;
  /*********************************************/

  if (workload == "TATP") {
    tatp_server = new TATP();
    tatp_server->LoadTable(machine_id,
                           machine_num,
                           &mem_store_alloc_param,
                           total_size,
                           ht_loadfv_size,
                           ht_size,
                           initfv_size,
                           real_cvt_size);
  } else if (workload == "SmallBank") {
    smallbank_server = new SmallBank();
    smallbank_server->LoadTable(machine_id,
                                machine_num,
                                &mem_store_alloc_param,
                                total_size,
                                ht_loadfv_size,
                                ht_size,
                                initfv_size,
                                real_cvt_size);
  } else if (workload == "TPCC") {
    tpcc_server = new TPCC();
    tpcc_server->LoadTable(machine_id,
                           machine_num,
                           &mem_store_alloc_param,
                           total_size,
                           ht_loadfv_size,
                           ht_size,
                           initfv_size,
                           real_cvt_size);
  } else if (workload == "MICRO") {
    micro_server = new MICRO();
    micro_server->LoadTable(machine_id,
                            machine_num,
                            &mem_store_alloc_param,
                            total_size,
                            ht_loadfv_size,
                            ht_size,
                            initfv_size,
                            real_cvt_size);
  }

  std::cerr << "----------------------------------------------------------" << std::endl;
  std::cerr << "VNum: " << MAX_VCELL_NUM << std::endl;
  std::cerr << "----------------------------------------------------------" << std::endl;
  // std::cerr << "During Load:" << std::endl;
  // std::cerr << "Total HT LoadCVT LoadFV (MB)" << std::endl;
  // std::cerr << (double)total_size / 1024.0 / 1024.0 << " " << (double)ht_size / 1024.0 / 1024.0 << " " << (double)real_cvt_size / 1024.0 / 1024.0 << " " << (double)initfv_size / 1024.0 / 1024.0 << std::endl;
  // std::cerr << "----------------------------------------------------------" << std::endl;

  std::cerr << "Data area: " << (double)data_size / 1024.0 / 1024.0 << " MB" << std::endl;
  std::cerr << "Delta area: " << (double)delta_size / 1024.0 / 1024.0 << " MB" << std::endl;
  std::cerr << "----------------------------------------------------------" << std::endl;

  // std::string results_cmd = "mkdir -p ../../../mem_ft/";
  // system(results_cmd.c_str());
  // std::string mem_ft_res = "../../../mem_ft/" + workload + "_ht_loadedfv_size.txt";

  // std::ofstream of;
  // of.open(mem_ft_res.c_str(), std::ios::app);
  // of << MAX_VCELL_NUM << " " << (double)ht_loadfv_size / 1024.0 / 1024.0 << std::endl;
  // of.close();

  // std::ofstream of_ft;
  // std::string file_path = "../../../../mem_ft.txt";
  // of_ft.open(file_path.c_str(), std::ios::app);
  // of_ft << std::endl;
  // of_ft << "----------------------------------------------------------" << std::endl;
  // of_ft << workload << " VNum: " << MAX_VCELL_NUM << std::endl;
  // of_ft << "----------------------------------------------------------" << std::endl;
  // of_ft << "Total HT LoadCVT LoadFV (MB)" << std::endl;
  // of_ft << (double)total_size / 1024.0 / 1024.0 << " " << (double)ht_size / 1024.0 / 1024.0 << " " << (double)real_cvt_size / 1024.0 / 1024.0 << " " << (double)initfv_size / 1024.0 / 1024.0 << std::endl;
  // of_ft.close();

  RDMA_LOG(INFO) << "Loading table successfully!";
}

void Server::CleanTable() {
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
}

void Server::CleanQP() {
  rdma_ctrl->destroy_rc_qp();
}

void Server::SendMeta(node_id_t machine_id,
                      std::string& workload,
                      size_t compute_node_num,
                      offset_t delta_start_off,
                      size_t per_thread_delta_size) {
  // Prepare hash meta
  char* hash_meta_buffer = nullptr;
  size_t total_meta_size = 0;
  PrepareHashMeta(machine_id, workload, &hash_meta_buffer, total_meta_size, delta_start_off, per_thread_delta_size);
  assert(hash_meta_buffer != nullptr);
  assert(total_meta_size != 0);
  RDMA_LOG(INFO) << "total meta size(B): " << total_meta_size;

  // Send memory store meta to all the compute nodes via TCP
  for (size_t index = 0; index < compute_node_num; index++) {
    SendHashMeta(hash_meta_buffer, total_meta_size);
  }
  free(hash_meta_buffer);
}

void Server::PrepareHashMeta(node_id_t machine_id,
                             std::string& workload,
                             char** hash_meta_buffer,
                             size_t& total_meta_size,
                             offset_t delta_start_off,
                             size_t per_thread_delta_size) {
  // Get all hash meta
  std::vector<HashMeta*> primary_hash_meta_vec;
  std::vector<HashMeta*> backup_hash_meta_vec;
  std::vector<HashStore*> all_priamry_tables;
  std::vector<HashStore*> all_backup_tables;

  if (workload == "TATP") {
    all_priamry_tables = tatp_server->GetPrimaryHashStore();
    all_backup_tables = tatp_server->GetBackupHashStore();
  } else if (workload == "SmallBank") {
    all_priamry_tables = smallbank_server->GetPrimaryHashStore();
    all_backup_tables = smallbank_server->GetBackupHashStore();
  } else if (workload == "TPCC") {
    all_priamry_tables = tpcc_server->GetPrimaryHashStore();
    all_backup_tables = tpcc_server->GetBackupHashStore();
  } else if (workload == "MICRO") {
    all_priamry_tables = micro_server->GetPrimaryHashStore();
    all_backup_tables = micro_server->GetBackupHashStore();
  }

  for (auto& hash_table : all_priamry_tables) {
    auto* hash_meta = new HashMeta(hash_table->GetTableID(),
                                   (uint64_t)hash_table->GetTablePtr(),
                                   hash_table->GetBaseOff(),
                                   hash_table->GetBucketNum(),
                                   hash_table->GetHashBucketSize(),
                                   hash_table->GetHashCore());
    primary_hash_meta_vec.emplace_back(hash_meta);
  }

  for (auto& hash_table : all_backup_tables) {
    auto* hash_meta = new HashMeta(hash_table->GetTableID(),
                                   (uint64_t)hash_table->GetTablePtr(),
                                   hash_table->GetBaseOff(),
                                   hash_table->GetBucketNum(),
                                   hash_table->GetHashBucketSize(),
                                   hash_table->GetHashCore());
    backup_hash_meta_vec.emplace_back(hash_meta);
  }

  int hash_meta_len = sizeof(HashMeta);
  size_t primary_hash_meta_num = primary_hash_meta_vec.size();
  RDMA_LOG(INFO) << "primary hash meta num: " << primary_hash_meta_num;

  size_t backup_hash_meta_num = backup_hash_meta_vec.size();
  RDMA_LOG(INFO) << "backup hash meta num: " << backup_hash_meta_num;

  total_meta_size = sizeof(primary_hash_meta_num) +
                    sizeof(backup_hash_meta_num) +
                    sizeof(machine_id) +
                    sizeof(delta_start_off) +
                    sizeof(per_thread_delta_size) +
                    primary_hash_meta_num * hash_meta_len +
                    backup_hash_meta_num * hash_meta_len +
                    sizeof(MEM_STORE_META_END);

  *hash_meta_buffer = (char*)malloc(total_meta_size);

  char* local_buf = *hash_meta_buffer;

  // Fill primary hash meta
  *((size_t*)local_buf) = primary_hash_meta_num;
  local_buf += sizeof(primary_hash_meta_num);

  *((size_t*)local_buf) = backup_hash_meta_num;
  local_buf += sizeof(backup_hash_meta_num);

  *((node_id_t*)local_buf) = machine_id;
  local_buf += sizeof(machine_id);

  *((offset_t*)local_buf) = delta_start_off;
  local_buf += sizeof(delta_start_off);

  *((size_t*)local_buf) = per_thread_delta_size;
  local_buf += sizeof(per_thread_delta_size);

  for (size_t i = 0; i < primary_hash_meta_num; i++) {
    memcpy(local_buf + i * hash_meta_len, (char*)primary_hash_meta_vec[i], hash_meta_len);
  }

  local_buf += primary_hash_meta_num * hash_meta_len;

  // Fill backup hash meta
  for (size_t i = 0; i < backup_hash_meta_num; i++) {
    memcpy(local_buf + i * hash_meta_len, (char*)backup_hash_meta_vec[i], hash_meta_len);
  }

  local_buf += backup_hash_meta_num * hash_meta_len;

  // EOF
  *((uint64_t*)local_buf) = MEM_STORE_META_END;
}

void Server::SendHashMeta(char* hash_meta_buffer, size_t& total_meta_size) {
  //> Using TCP to send hash meta
  /* --------------- Initialize socket ---------------- */
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(local_meta_port);    // change host little endian to big endian
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);  // change host "0.0.0.0" to big endian
  int listen_socket = socket(AF_INET, SOCK_STREAM, 0);

  // The port can be used immediately after restart
  int on = 1;
  setsockopt(listen_socket, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

  if (listen_socket < 0) {
    RDMA_LOG(ERROR) << "Server creates socket error: " << strerror(errno);
    close(listen_socket);
    return;
  }

  RDMA_LOG(INFO) << "Server creates socket success";
  if (bind(listen_socket, (const struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
    RDMA_LOG(ERROR) << "Server binds socket error: " << strerror(errno);
    close(listen_socket);
    return;
  }

  RDMA_LOG(INFO) << "Server binds socket success";
  int max_listen_num = 10;  // Larger than # of CNs
  if (listen(listen_socket, max_listen_num) < 0) {
    RDMA_LOG(ERROR) << "Server listens error: " << strerror(errno);
    close(listen_socket);
    return;
  }

  RDMA_LOG(INFO) << "Server listening...";
  // --------------------------------------------------------------------

  int from_client_socket = accept(listen_socket, NULL, NULL);
  // int from_client_socket = accept(listen_socket, (struct sockaddr*) &client_addr, &client_socket_length);
  if (from_client_socket < 0) {
    RDMA_LOG(ERROR) << "Server accepts error: " << strerror(errno);
    close(from_client_socket);
    close(listen_socket);
    return;
  }
  RDMA_LOG(INFO) << "Server accepts success";

  /* --------------- Sending hash metadata ----------------- */
  auto retlen = send(from_client_socket, hash_meta_buffer, total_meta_size, 0);
  if (retlen < 0) {
    RDMA_LOG(ERROR) << "Server sends hash meta error: " << strerror(errno);
    close(from_client_socket);
    close(listen_socket);
    return;
  }
  RDMA_LOG(INFO) << "Server sends hash meta success";

  size_t recv_ack_size = 100;
  char* recv_buf = (char*)malloc(recv_ack_size);
  recv(from_client_socket, recv_buf, recv_ack_size, 0);

  if (strcmp(recv_buf, "[ACK]hash_meta_received_from_client") != 0) {
    std::string ack(recv_buf);
    RDMA_LOG(ERROR) << "Client receives hash meta error. Received ack is: " << ack;
  }

  free(recv_buf);
  close(from_client_socket);
  close(listen_socket);
}

void Server::AcceptReq() {
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(local_meta_port);    // change host little endian to big endian
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);  // change host "0.0.0.0" to big endian
  int listen_socket = socket(AF_INET, SOCK_STREAM, 0);

  // The port can be used immediately after restart
  int on = 1;
  setsockopt(listen_socket, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

  if (listen_socket < 0) {
    RDMA_LOG(ERROR) << "Server creates socket error: " << strerror(errno);
    close(listen_socket);
    return;
  }

  RDMA_LOG(INFO) << "[AcceptReq] Server creates socket success";
  if (bind(listen_socket, (const struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
    RDMA_LOG(ERROR) << "Server binds socket error: " << strerror(errno);
    close(listen_socket);
    return;
  }

  RDMA_LOG(INFO) << "[AcceptReq] Server binds socket success";
  int max_listen_num = 10;  // Larger than # of CNs
  if (listen(listen_socket, max_listen_num) < 0) {
    RDMA_LOG(ERROR) << "Server listens error: " << strerror(errno);
    close(listen_socket);
    return;
  }

  RDMA_LOG(INFO) << "[AcceptReq] Server listening...";
  // --------------------------------------------------------------------

  int from_client_socket = accept(listen_socket, NULL, NULL);
  // int from_client_socket = accept(listen_socket, (struct sockaddr*) &client_addr, &client_socket_length);
  if (from_client_socket < 0) {
    RDMA_LOG(ERROR) << "Server accepts error: " << strerror(errno);
    close(from_client_socket);
    close(listen_socket);
    return;
  }

  size_t recv_ack_size = 100;
  char* recv_buf = (char*)malloc(recv_ack_size);
  recv(from_client_socket, recv_buf, recv_ack_size, 0);

  char* p = recv_buf;

  table_id_t table_id = *(table_id_t*)p;
  p += sizeof(table_id_t);

  node_id_t target_mn_id = *(node_id_t*)p;
  p += sizeof(node_id_t);

  int is_primary_fail = *(int*)p;

  RDMA_LOG(INFO) << "[AcceptReq] IsPrimaryFail: " << is_primary_fail << ". I migrate table " << table_id << " from me to MN " << target_mn_id;

  // migrate data

  std::vector<HashStore*> tables;

  if (is_primary_fail) {
    // Use backup to recover primary
    tables = tpcc_server->GetBackupHashStore();
  } else {
    // Use primary to recover backup
    tables = tpcc_server->GetPrimaryHashStore();
  }

  char* start_copy = nullptr;
  size_t migration_size = 0;

  int write_cnt = 0;

  int new_attr_bar_cnt = 0;
  int new_insert_cnt = 0;

  for (int i = 0; i < tables.size(); i++) {
    if (tables[i]->GetTableID() == table_id) {
      start_copy = tables[i]->GetTablePtr();
      migration_size += tables[i]->GetHTInitFVSize();

      // I have the same table offset with remote node
      other_mn_qps[target_mn_id]->post_send(IBV_WR_RDMA_WRITE, start_copy, migration_size, tables[i]->GetBaseOff(), IBV_SEND_SIGNALED);

      ibv_wc wc{};
      other_mn_qps[target_mn_id]->poll_till_completion(wc, no_timeout);

      write_cnt++;

      for (int k = 0; k < tables[i]->GetBucketNum(); k++) {
        // HashBucket* bkt = (HashBucket*)(k * HashBucketSize + start_copy);

        size_t bkt_size = SLOT_NUM[table_id] * CVTSize;
        char* cvt_start = k * bkt_size + start_copy;

        for (int j = 0; j < SLOT_NUM[table_id]; j++) {
          // CVT* cvt = &(bkt->cvts[j]);
          CVT* cvt = (CVT*)(cvt_start + j * CVTSize);

          if (cvt->header.value_size) {
            if (cvt->header.remote_attribute_offset != UN_INIT_POS) {
              migration_size += ATTR_BAR_SIZE[table_id];
              char* local_attr_addr = mem_region + cvt->header.remote_attribute_offset;

              other_mn_qps[target_mn_id]->post_send(IBV_WR_RDMA_WRITE, local_attr_addr, ATTR_BAR_SIZE[table_id], cvt->header.remote_attribute_offset, IBV_SEND_SIGNALED);

              ibv_wc wc{};
              other_mn_qps[target_mn_id]->poll_till_completion(wc, no_timeout);
              write_cnt++;

              new_attr_bar_cnt++;
            }

            if (cvt->header.user_inserted) {
              size_t vpkg_size = TABLE_VALUE_SIZE[table_id] + sizeof(anchor_t) * 2;
              migration_size += vpkg_size;
              char* local_fv_addr = mem_region + cvt->header.remote_full_value_offset;

              other_mn_qps[target_mn_id]->post_send(IBV_WR_RDMA_WRITE, local_fv_addr, vpkg_size, cvt->header.remote_full_value_offset, IBV_SEND_SIGNALED);
              write_cnt++;

              ibv_wc wc{};
              other_mn_qps[target_mn_id]->poll_till_completion(wc, no_timeout);

              new_insert_cnt++;
            }
          }
        }
      }

      break;
    }
  }

  RDMA_LOG(INFO) << "[AcceptReq] Migrate SUCCESS: " << (double)migration_size / 1024.0 << " KB."
                 << " Write cnt: " << write_cnt << ". new_attr_bar_cnt: " << new_attr_bar_cnt << ". new_insert_cnt: " << new_insert_cnt;

  // while (write_cnt--) {
  //   ibv_wc wc{};
  //   other_mn_qps[target_mn_id]->poll_till_completion(wc, no_timeout);
  // }

  char ack[] = "MIGRATE_OK";
  send(from_client_socket, ack, strlen(ack) + 1, 0);

  free(recv_buf);
  close(from_client_socket);
  close(listen_socket);
}

void Server::OutputMemoryFootprint(std::string& workload) {
  std::vector<HashStore*> all_priamry_tables;
  std::vector<HashStore*> all_backup_tables;

  if (workload == "TATP") {
    all_priamry_tables = tatp_server->GetPrimaryHashStore();
    all_backup_tables = tatp_server->GetBackupHashStore();
  } else if (workload == "SmallBank") {
    all_priamry_tables = smallbank_server->GetPrimaryHashStore();
    all_backup_tables = smallbank_server->GetBackupHashStore();
  } else if (workload == "TPCC") {
    all_priamry_tables = tpcc_server->GetPrimaryHashStore();
    all_backup_tables = tpcc_server->GetBackupHashStore();
  } else if (workload == "MICRO") {
    all_priamry_tables = micro_server->GetPrimaryHashStore();
    all_backup_tables = micro_server->GetBackupHashStore();
  }

  size_t total_cvt_size = 0;

  for (auto* table : all_priamry_tables) {
    total_cvt_size += table->GetValidCVTSize();
  }

  for (auto* table : all_backup_tables) {
    total_cvt_size += table->GetValidCVTSize();
  }

  std::cerr << "after exe:" << std::endl;
  std::cerr << "TotalCVT (MB): " << (double)total_cvt_size / 1024.0 / 1024.0 << std::endl;
  std::cerr << "----------------------------------------------------------" << std::endl;

  std::ofstream of_ft;
  std::string file_path = "../../../../mem_ft.txt";
  of_ft.open(file_path.c_str(), std::ios::app);
  of_ft << std::endl;
  of_ft << "after exe:" << std::endl;
  of_ft << "TotalCVT (MB): " << (double)total_cvt_size / 1024.0 / 1024.0 << std::endl;
  of_ft.close();
}

bool Server::Run(std::string& workload) {
  // Now server just waits for user typing quit to finish
  // Server's CPU is not used during one-sided RDMA requests from clients
  std::cerr << "============== Disaggregated Mode ===============" << std::endl;
  // std::cerr << "Type c for another round, type q to exit :)" << std::endl;

#if HAVE_PRIMARY_CRASH || HAVE_BACKUP_CRASH
  AcceptReq();
#endif

  while (true) {
    char ch;
    scanf("%c", &ch);
    if (ch == 'q') {
      return false;
    } else if (ch == 'c') {
      return true;
    } else {
      std::cerr << "Type c for another round, type q to exit :)" << std::endl;
    }
  }
}

int main(int argc, char* argv[]) {
  // Configure of this server
  std::string config_filepath = "../../../config/mn_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);

  auto local_node = json_config.get("local_memory_node");
  node_id_t machine_num = (node_id_t)local_node.get("machine_num").get_int64();
  node_id_t machine_id = (node_id_t)local_node.get("machine_id").get_int64();
  assert(machine_id >= 0 && machine_id < machine_num);
  int local_port = (int)local_node.get("local_port").get_int64();
  int local_meta_port = (int)local_node.get("local_meta_port").get_int64();
  int use_pm = (int)local_node.get("use_pm").get_int64();
  std::string pm_root = local_node.get("pm_root").get_str();
  std::string workload = local_node.get("workload").get_str();
  auto reserve_GB = local_node.get("reserve_GB").get_uint64();
  auto max_client_num_per_mn = local_node.get("max_client_num_per_mn").get_uint64();
  auto per_thread_delta_size_MB = local_node.get("per_thread_delta_size_MB").get_uint64();

  auto compute_nodes = json_config.get("remote_compute_nodes");
  auto compute_node_ips = compute_nodes.get("compute_node_ips");  // Array
  size_t compute_node_num = compute_node_ips.size();

  // std::string pm_file = pm_root + "pm_node" + std::to_string(machine_id); // Use fsdax
  std::string pm_file = pm_root;  // Use devdax
  size_t data_size = (size_t)1024 * 1024 * 1024 * reserve_GB;
  size_t per_thread_delta_size = (size_t)1024 * 1024 * per_thread_delta_size_MB;
  size_t delta_size = per_thread_delta_size * max_client_num_per_mn;

  auto server = std::make_shared<Server>(machine_id,
                                         local_port,
                                         local_meta_port,
                                         data_size,
                                         delta_size,
                                         use_pm,
                                         pm_file);

  server->AllocMem();
  server->InitMem();
  server->InitRDMA();

#if HAVE_PRIMARY_CRASH || HAVE_BACKUP_CRASH
  server->ConnectMN();
#endif

  server->LoadData(machine_id, machine_num, workload);
  server->SendMeta(machine_id, workload, compute_node_num, data_size, per_thread_delta_size);
  bool run_next_round = server->Run(workload);

  // Continue to run the next round. RDMA does not need to be inited twice
  while (run_next_round) {
    server->InitMem();
    server->CleanTable();
    server->CleanQP();

#if HAVE_PRIMARY_CRASH || HAVE_BACKUP_CRASH
    server->ConnectMN();
#endif

    server->LoadData(machine_id, machine_num, workload);
    server->SendMeta(machine_id, workload, compute_node_num, data_size, per_thread_delta_size);
    run_next_round = server->Run(workload);
  }

  // Stat the cpu utilization
  // auto pid = getpid();
  // std::string copy_cmd = "cp /proc/" + std::to_string(pid) + "/stat ./";
  // system(copy_cmd.c_str());

  // copy_cmd = "cp /proc/uptime ./";
  // system(copy_cmd.c_str());
  return 0;
}
// Author: Ming Zhang
// Copyright (c) 2023

#include "tpcc_db.h"

#define NUM_CUSTOMER_LAST_NAME_FROM_CID 100
#define NUM_ORDER_MINUS_NEWORDER 210

void TPCC::LoadTable(node_id_t node_id,
                     node_id_t num_server,
                     MemStoreAllocParam* mem_store_alloc_param,
                     size_t& total_size,
                     size_t& ht_loadfv_size,
                     size_t& ht_size,
                     size_t& initfv_size,
                     size_t& real_cvt_size) {
  // printf(
  //     "sizeof(tpcc_warehouse_val_t) = %lu, sizeof(tpcc_district_val_t) = %lu\n"
  //     "sizeof(tpcc_customer_val_t) = %lu, sizeof(tpcc_customer_index_val_t) = %lu\n"
  //     "sizeof(tpcc_history_val_t) = %lu, sizeof(tpcc_new_order_val_t) = %lu\n"
  //     "sizeof(tpcc_order_val_t) = %lu, sizeof(tpcc_order_index_val_t) = %lu\n"
  //     "sizeof(tpcc_order_line_val_t) = %lu, sizeof(tpcc_item_val_t) = %lu\n"
  //     "sizeof(tpcc_stock_val_t) = %lu, CVTSize = %lu\n",
  //     sizeof(tpcc_warehouse_val_t),
  //     sizeof(tpcc_district_val_t),
  //     sizeof(tpcc_customer_val_t),
  //     sizeof(tpcc_customer_index_val_t),
  //     sizeof(tpcc_history_val_t),
  //     sizeof(tpcc_new_order_val_t),
  //     sizeof(tpcc_order_val_t),
  //     sizeof(tpcc_order_index_val_t),
  //     sizeof(tpcc_order_line_val_t),
  //     sizeof(tpcc_item_val_t),
  //     sizeof(tpcc_stock_val_t),
  //     CVTSize);

  // Initiate + Populate table for primary role
  std::string config_filepath = "../../../config/tpcc_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);
  auto table_config = json_config.get("tpcc");

  {
    RDMA_LOG(DBG) << "Loading Warehouse table";
    warehouse_table = new HashStore((table_id_t)TPCCTableType::kWarehouseTable,
                                    table_config.get("warehouse_bkt_num").get_uint64(),
                                    mem_store_alloc_param);
    Populate_Warehouse_Table(9324);
    total_size += warehouse_table->GetTotalSize();
    ht_loadfv_size += warehouse_table->GetHTInitFVSize();
    ht_size += warehouse_table->GetHTSize();
    initfv_size += warehouse_table->GetInitFVSize();
    real_cvt_size += warehouse_table->GetLoadCVTSize();
    std::cerr << "Warehouse max occupy slot num: " << warehouse_table->GetMaxOccupySlotNum() << std::endl;
  }

  {
    RDMA_LOG(DBG) << "Loading District table";
    district_table = new HashStore((table_id_t)TPCCTableType::kDistrictTable,
                                   table_config.get("warehouse_bkt_num").get_uint64() *
                                       table_config.get("district_bkt_num").get_uint64(),
                                   mem_store_alloc_param);
    Populate_District_Table(129856349);
    total_size += district_table->GetTotalSize();
    ht_loadfv_size += district_table->GetHTInitFVSize();
    ht_size += district_table->GetHTSize();
    initfv_size += district_table->GetInitFVSize();
    real_cvt_size += district_table->GetLoadCVTSize();
    std::cerr << "District max occupy slot num: " << district_table->GetMaxOccupySlotNum() << std::endl;
  }

  {
    RDMA_LOG(DBG) << "Loading Customer+CustomerIndex+History table";
    customer_table = new HashStore((table_id_t)TPCCTableType::kCustomerTable,
                                   table_config.get("warehouse_bkt_num").get_uint64() *
                                       table_config.get("district_bkt_num").get_uint64() *
                                       table_config.get("customer_bkt_num").get_uint64(),
                                   mem_store_alloc_param);

    customer_index_table = new HashStore((table_id_t)TPCCTableType::kCustomerIndexTable,
                                         table_config.get("warehouse_bkt_num").get_uint64() *
                                             table_config.get("district_bkt_num").get_uint64() *
                                             table_config.get("customer_bkt_num").get_uint64(),
                                         mem_store_alloc_param);

    history_table = new HashStore((table_id_t)TPCCTableType::kHistoryTable,
                                  table_config.get("warehouse_bkt_num").get_uint64() *
                                      table_config.get("district_bkt_num").get_uint64() *
                                      table_config.get("customer_bkt_num").get_uint64(),
                                  mem_store_alloc_param);
    Populate_Customer_CustomerIndex_History_Table(923587856425);
    total_size += customer_table->GetTotalSize();
    ht_loadfv_size += customer_table->GetHTInitFVSize();
    ht_size += customer_table->GetHTSize();
    initfv_size += customer_table->GetInitFVSize();
    real_cvt_size += customer_table->GetLoadCVTSize();

    total_size += customer_index_table->GetTotalSize();
    ht_loadfv_size += customer_index_table->GetHTInitFVSize();
    ht_size += customer_index_table->GetHTSize();
    initfv_size += customer_index_table->GetInitFVSize();
    real_cvt_size += customer_index_table->GetLoadCVTSize();

    total_size += history_table->GetTotalSize();
    ht_loadfv_size += history_table->GetHTInitFVSize();
    ht_size += history_table->GetHTSize();
    initfv_size += history_table->GetInitFVSize();
    real_cvt_size += history_table->GetLoadCVTSize();

    std::cerr << "Customer max occupy slot num: " << customer_table->GetMaxOccupySlotNum() << std::endl;
    std::cerr << "CustomerIndex max occupy slot num: " << customer_index_table->GetMaxOccupySlotNum() << std::endl;
    std::cerr << "History max occupy slot num: " << history_table->GetMaxOccupySlotNum() << std::endl;
  }

  {
    RDMA_LOG(DBG) << "Loading Order+OrderIndex+NewOrder+OrderLine table";
    order_table = new HashStore((table_id_t)TPCCTableType::kOrderTable,
                                table_config.get("warehouse_bkt_num").get_uint64() *
                                    table_config.get("district_bkt_num").get_uint64() *
                                    table_config.get("customer_bkt_num").get_uint64(),
                                mem_store_alloc_param,
                                HashCore::kMurmurFunc);

    order_index_table = new HashStore((table_id_t)TPCCTableType::kOrderIndexTable,
                                      table_config.get("warehouse_bkt_num").get_uint64() *
                                          table_config.get("district_bkt_num").get_uint64() *
                                          table_config.get("customer_bkt_num").get_uint64(),
                                      mem_store_alloc_param,
                                      HashCore::kMurmurFunc);

    new_order_table = new HashStore((table_id_t)TPCCTableType::kNewOrderTable,
                                    table_config.get("warehouse_bkt_num").get_uint64() *
                                        table_config.get("district_bkt_num").get_uint64() *
                                        table_config.get("customer_bkt_num").get_uint64() * 0.3,
                                    mem_store_alloc_param,
                                    HashCore::kMurmurFunc);

    order_line_table = new HashStore((table_id_t)TPCCTableType::kOrderLineTable,
                                     table_config.get("warehouse_bkt_num").get_uint64() *
                                         table_config.get("district_bkt_num").get_uint64() *
                                         table_config.get("customer_bkt_num").get_uint64() * 15,
                                     mem_store_alloc_param,
                                     HashCore::kMurmurFunc);
    Populate_Order_OrderIndex_NewOrder_OrderLine_Table(2343352);
    total_size += order_table->GetTotalSize();
    ht_loadfv_size += order_table->GetHTInitFVSize();
    ht_size += order_table->GetHTSize();
    initfv_size += order_table->GetInitFVSize();
    real_cvt_size += order_table->GetLoadCVTSize();

    total_size += order_index_table->GetTotalSize();
    ht_loadfv_size += order_index_table->GetHTInitFVSize();
    ht_size += order_index_table->GetHTSize();
    initfv_size += order_index_table->GetInitFVSize();
    real_cvt_size += order_index_table->GetLoadCVTSize();

    total_size += new_order_table->GetTotalSize();
    ht_loadfv_size += new_order_table->GetHTInitFVSize();
    ht_size += new_order_table->GetHTSize();
    initfv_size += new_order_table->GetInitFVSize();
    real_cvt_size += new_order_table->GetLoadCVTSize();

    total_size += order_line_table->GetTotalSize();
    ht_loadfv_size += order_line_table->GetHTInitFVSize();
    ht_size += order_line_table->GetHTSize();
    initfv_size += order_line_table->GetInitFVSize();
    real_cvt_size += order_line_table->GetLoadCVTSize();

    std::cerr << "Order max occupy slot num: " << order_table->GetMaxOccupySlotNum() << std::endl;
    std::cerr << "OrderIndex max occupy slot num: " << order_index_table->GetMaxOccupySlotNum() << std::endl;
    std::cerr << "NewOrder max occupy slot num: " << new_order_table->GetMaxOccupySlotNum() << std::endl;
    std::cerr << "OrderLine max occupy slot num: " << order_line_table->GetMaxOccupySlotNum() << std::endl;
  }

  {
    RDMA_LOG(DBG) << "Loading Stock table";
    stock_table = new HashStore((table_id_t)TPCCTableType::kStockTable,
                                table_config.get("warehouse_bkt_num").get_uint64() *
                                    table_config.get("stock_bkt_num").get_uint64(),
                                mem_store_alloc_param);
    Populate_Stock_Table(89785943);
    total_size += stock_table->GetTotalSize();
    ht_loadfv_size += stock_table->GetHTInitFVSize();
    ht_size += stock_table->GetHTSize();
    initfv_size += stock_table->GetInitFVSize();
    real_cvt_size += stock_table->GetLoadCVTSize();

    std::cerr << "Stock max occupy slot num: " << stock_table->GetMaxOccupySlotNum() << std::endl;
  }

  {
    RDMA_LOG(DBG) << "Loading Item table";
    item_table = new HashStore((table_id_t)TPCCTableType::kItemTable,
                               table_config.get("item_bkt_num").get_uint64(),
                               mem_store_alloc_param);
    Populate_Item_Table(235443);
    total_size += item_table->GetTotalSize();
    ht_loadfv_size += item_table->GetHTInitFVSize();
    ht_size += item_table->GetHTSize();
    initfv_size += item_table->GetInitFVSize();
    real_cvt_size += item_table->GetLoadCVTSize();

    std::cerr << "Item max occupy slot num: " << item_table->GetMaxOccupySlotNum() << std::endl;
  }

  std::cout << "----------------------------------------------------------" << std::endl;
  // Assign primary
  if ((node_id_t)TPCCTableType::kWarehouseTable % num_server == node_id) {
    RDMA_LOG(EMPH) << "[Primary] Warehouse table ID: " << (node_id_t)TPCCTableType::kWarehouseTable;
    std::cerr << "Number of initial records: " << std::dec << warehouse_table->GetInitInsertNum() << std::endl;
    primary_table_ptrs.push_back(warehouse_table);
  }

  if ((node_id_t)TPCCTableType::kDistrictTable % num_server == node_id) {
    RDMA_LOG(EMPH) << "[Primary] District table ID: " << (node_id_t)TPCCTableType::kDistrictTable;
    std::cerr << "Number of initial records: " << std::dec << district_table->GetInitInsertNum() << std::endl;
    primary_table_ptrs.push_back(district_table);
  }

  if ((node_id_t)TPCCTableType::kCustomerTable % num_server == node_id) {
    RDMA_LOG(EMPH) << "[Primary] Customer+CustomerIndex+History table IDs: " << (node_id_t)TPCCTableType::kCustomerTable
                   << " + " << (node_id_t)TPCCTableType::kCustomerIndexTable
                   << " + " << (node_id_t)TPCCTableType::kHistoryTable;

    std::cerr << "Number of initial records: " << std::dec << customer_table->GetInitInsertNum() << std::endl;
    std::cerr << "Number of initial records: " << std::dec << customer_index_table->GetInitInsertNum() << std::endl;
    std::cerr << "Number of initial records: " << std::dec << history_table->GetInitInsertNum() << std::endl;

    primary_table_ptrs.push_back(customer_table);
    primary_table_ptrs.push_back(customer_index_table);
    primary_table_ptrs.push_back(history_table);
  }

  if ((node_id_t)TPCCTableType::kOrderTable % num_server == node_id) {
    RDMA_LOG(EMPH) << "[Primary] Order+OrderIndex+NewOrder+OrderLine table IDs: " << (node_id_t)TPCCTableType::kOrderTable
                   << " + " << (node_id_t)TPCCTableType::kOrderIndexTable
                   << " + " << (node_id_t)TPCCTableType::kNewOrderTable
                   << " + " << (node_id_t)TPCCTableType::kOrderLineTable;

    std::cerr << "Number of initial records: " << std::dec << order_table->GetInitInsertNum() << std::endl;
    std::cerr << "Number of initial records: " << std::dec << order_index_table->GetInitInsertNum() << std::endl;
    std::cerr << "Number of initial records: " << std::dec << new_order_table->GetInitInsertNum() << std::endl;
    std::cerr << "Number of initial records: " << std::dec << order_line_table->GetInitInsertNum() << std::endl;

    primary_table_ptrs.push_back(order_table);
    primary_table_ptrs.push_back(order_index_table);
    primary_table_ptrs.push_back(new_order_table);
    primary_table_ptrs.push_back(order_line_table);
  }

  if ((node_id_t)TPCCTableType::kStockTable % num_server == node_id) {
    RDMA_LOG(EMPH) << "[Primary] Stock table ID: " << (node_id_t)TPCCTableType::kStockTable;
    std::cerr << "Number of initial records: " << std::dec << stock_table->GetInitInsertNum() << std::endl;
    primary_table_ptrs.push_back(stock_table);
  }

  if ((node_id_t)TPCCTableType::kItemTable % num_server == node_id) {
    RDMA_LOG(EMPH) << "[Primary] Item table ID: " << (node_id_t)TPCCTableType::kItemTable;
    std::cerr << "Number of initial records: " << std::dec << item_table->GetInitInsertNum() << std::endl;
    primary_table_ptrs.push_back(item_table);
  }

  std::cout << "----------------------------------------------------------" << std::endl;
  // Assign backup
  if (BACKUP_NUM < num_server) {
    for (node_id_t i = 1; i <= BACKUP_NUM; i++) {
      if ((node_id_t)TPCCTableType::kWarehouseTable % num_server == (node_id - i + num_server) % num_server) {
        RDMA_LOG(DBG) << "[Backup] Warehouse table ID: " << (node_id_t)TPCCTableType::kWarehouseTable;
        std::cerr << "Number of initial records: " << std::dec << warehouse_table->GetInitInsertNum() << std::endl;
        backup_table_ptrs.push_back(warehouse_table);
      }

      if ((node_id_t)TPCCTableType::kDistrictTable % num_server == (node_id - i + num_server) % num_server) {
        RDMA_LOG(DBG) << "[Backup] District table ID: " << (node_id_t)TPCCTableType::kDistrictTable;
        std::cerr << "Number of initial records: " << std::dec << district_table->GetInitInsertNum() << std::endl;
        backup_table_ptrs.push_back(district_table);
      }

      if ((node_id_t)TPCCTableType::kCustomerTable % num_server == (node_id - i + num_server) % num_server) {
        RDMA_LOG(DBG) << "[Backup] Customer+CustomerIndex+History table IDs: " << (node_id_t)TPCCTableType::kCustomerTable
                      << " + " << (node_id_t)TPCCTableType::kCustomerIndexTable
                      << " + " << (node_id_t)TPCCTableType::kHistoryTable;

        std::cerr << "Number of initial records: " << std::dec << customer_table->GetInitInsertNum() << std::endl;
        std::cerr << "Number of initial records: " << std::dec << customer_index_table->GetInitInsertNum() << std::endl;
        std::cerr << "Number of initial records: " << std::dec << history_table->GetInitInsertNum() << std::endl;

        backup_table_ptrs.push_back(customer_table);
        backup_table_ptrs.push_back(customer_index_table);
        backup_table_ptrs.push_back(history_table);
      }

      if ((node_id_t)TPCCTableType::kOrderTable % num_server == (node_id - i + num_server) % num_server) {
        RDMA_LOG(DBG) << "[Backup] Order+OrderIndex+NewOrder+OrderLine table IDs: " << (node_id_t)TPCCTableType::kOrderTable
                      << " + " << (node_id_t)TPCCTableType::kOrderIndexTable
                      << " + " << (node_id_t)TPCCTableType::kNewOrderTable
                      << " + " << (node_id_t)TPCCTableType::kOrderLineTable;

        std::cerr << "Number of initial records: " << std::dec << order_table->GetInitInsertNum() << std::endl;
        std::cerr << "Number of initial records: " << std::dec << order_index_table->GetInitInsertNum() << std::endl;
        std::cerr << "Number of initial records: " << std::dec << new_order_table->GetInitInsertNum() << std::endl;
        std::cerr << "Number of initial records: " << std::dec << order_line_table->GetInitInsertNum() << std::endl;

        backup_table_ptrs.push_back(order_table);
        backup_table_ptrs.push_back(order_index_table);
        backup_table_ptrs.push_back(new_order_table);
        backup_table_ptrs.push_back(order_line_table);
      }

      if ((node_id_t)TPCCTableType::kStockTable % num_server == (node_id - i + num_server) % num_server) {
        RDMA_LOG(DBG) << "[Backup] Stock table ID: " << (node_id_t)TPCCTableType::kStockTable;
        std::cerr << "Number of initial records: " << std::dec << stock_table->GetInitInsertNum() << std::endl;
        backup_table_ptrs.push_back(stock_table);
      }

      if ((node_id_t)TPCCTableType::kItemTable % num_server == (node_id - i + num_server) % num_server) {
        RDMA_LOG(DBG) << "[Backup] Item table ID: " << (node_id_t)TPCCTableType::kItemTable;
        std::cerr << "Number of initial records: " << std::dec << item_table->GetInitInsertNum() << std::endl;
        backup_table_ptrs.push_back(item_table);
      }
    }
  }
}

void TPCC::Populate_Warehouse_Table(unsigned long seed) {
  FastRandom random_generator(seed);
  // populate warehouse table
  for (uint32_t w_id = 1; w_id <= num_warehouse; w_id++) {
    tpcc_warehouse_key_t warehouse_key;
    warehouse_key.w_id = w_id;

    /* Initialize the warehouse payload */
    tpcc_warehouse_val_t warehouse_val;
    warehouse_val.w_ytd = 300000 * 100;
    //  NOTICE:: scale should check consistency requirements.
    //  W_YTD = sum(D_YTD) where (W_ID = D_W_ID).
    //  W_YTD = sum(H_AMOUNT) where (W_ID = H_W_ID).
    warehouse_val.w_tax = (float)RandomNumber(random_generator, 0, 2000) / 10000.0;
    strcpy(warehouse_val.w_name,
           RandomStr(random_generator, RandomNumber(random_generator, tpcc_warehouse_val_t::MIN_NAME, tpcc_warehouse_val_t::MAX_NAME)).c_str());
    strcpy(warehouse_val.w_street_1,
           RandomStr(random_generator, RandomNumber(random_generator, Address::MIN_STREET, Address::MAX_STREET)).c_str());
    strcpy(warehouse_val.w_street_2,
           RandomStr(random_generator, RandomNumber(random_generator, Address::MIN_STREET, Address::MAX_STREET)).c_str());
    strcpy(warehouse_val.w_city,
           RandomStr(random_generator, RandomNumber(random_generator, Address::MIN_CITY, Address::MAX_CITY)).c_str());
    strcpy(warehouse_val.w_state, RandomStr(random_generator, Address::STATE).c_str());
    strcpy(warehouse_val.w_zip, "123456789");

    assert(warehouse_val.w_state[2] == '\0' && strcmp(warehouse_val.w_zip, "123456789") == 0);
    LoadRecord(warehouse_table,
               warehouse_key.item_key,
               (void*)&warehouse_val,
               tpcc_warehouse_val_t_size,
               (table_id_t)TPCCTableType::kWarehouseTable);
  }
}

void TPCC::Populate_District_Table(unsigned long seed) {
  FastRandom random_generator(seed);
  for (uint32_t w_id = 1; w_id <= num_warehouse; w_id++) {
    for (uint32_t d_id = 1; d_id <= num_district_per_warehouse; d_id++) {
      tpcc_district_key_t district_key;
      district_key.d_id = MakeDistrictKey(w_id, d_id);

      /* Initialize the district payload */
      tpcc_district_val_t district_val;

      district_val.d_ytd = 30000 * 100;  // different from warehouse, notice it did the scale up
      //  NOTICE:: scale should check consistency requirements.
      //  D_YTD = sum(H_AMOUNT) where (D_W_ID, D_ID) = (H_W_ID, H_D_ID).
      district_val.d_tax = (float)RandomNumber(random_generator, 0, 2000) / 10000.0;
      district_val.d_next_o_id = num_customer_per_district + 1;
      //  NOTICE:: scale should check consistency requirements.
      //  D_NEXT_O_ID - 1 = max(O_ID) = max(NO_O_ID)

      strcpy(district_val.d_name,
             RandomStr(random_generator, RandomNumber(random_generator, tpcc_district_val_t::MIN_NAME, tpcc_district_val_t::MAX_NAME)).c_str());
      strcpy(district_val.d_street_1,
             RandomStr(random_generator, RandomNumber(random_generator, Address::MIN_STREET, Address::MAX_STREET)).c_str());
      strcpy(district_val.d_street_2,
             RandomStr(random_generator, RandomNumber(random_generator, Address::MIN_STREET, Address::MAX_STREET)).c_str());
      strcpy(district_val.d_city,
             RandomStr(random_generator, RandomNumber(random_generator, Address::MIN_CITY, Address::MAX_CITY)).c_str());
      strcpy(district_val.d_state, RandomStr(random_generator, Address::STATE).c_str());
      strcpy(district_val.d_zip, "123456789");

      LoadRecord(district_table,
                 district_key.item_key,
                 (void*)&district_val,
                 tpcc_district_val_t_size,
                 (table_id_t)TPCCTableType::kDistrictTable);
    }
  }
}

// no batch in this implementation
void TPCC::Populate_Customer_CustomerIndex_History_Table(unsigned long seed) {
  FastRandom random_generator(seed);
  // printf("num_warehouse = %d, num_district_per_warehouse = %d, num_customer_per_district = %d\n", num_warehouse, num_district_per_warehouse, num_customer_per_district);
  for (uint32_t w_id = 1; w_id <= num_warehouse; w_id++) {
    for (uint32_t d_id = 1; d_id <= num_district_per_warehouse; d_id++) {
      for (uint32_t c_id = 1; c_id <= num_customer_per_district; c_id++) {
        tpcc_customer_key_t customer_key;
        customer_key.c_id = MakeCustomerKey(w_id, d_id, c_id);

        tpcc_customer_val_t customer_val;
        customer_val.c_discount = (float)(RandomNumber(random_generator, 1, 5000) / 10000.0);
        if (RandomNumber(random_generator, 1, 100) <= 10)
          strcpy(customer_val.c_credit, "BC");
        else
          strcpy(customer_val.c_credit, "GC");
        std::string c_last;
        if (c_id <= num_customer_per_district / 3) {
          c_last.assign(GetCustomerLastName(random_generator, c_id - 1));
          strcpy(customer_val.c_last, c_last.c_str());
        } else {
          c_last.assign(GetNonUniformCustomerLastNameLoad(random_generator));
          strcpy(customer_val.c_last, c_last.c_str());
        }

        std::string c_first = RandomStr(random_generator, RandomNumber(random_generator, tpcc_customer_val_t::MIN_FIRST, tpcc_customer_val_t::MAX_FIRST));
        strcpy(customer_val.c_first, c_first.c_str());

        customer_val.c_credit_lim = 50000;

        customer_val.c_balance = -10;
        customer_val.c_ytd_payment = 10;
        customer_val.c_payment_cnt = 1;
        customer_val.c_delivery_cnt = 0;
        strcpy(customer_val.c_street_1,
               RandomStr(random_generator, RandomNumber(random_generator, Address::MIN_STREET, Address::MAX_STREET)).c_str());
        strcpy(customer_val.c_street_2,
               RandomStr(random_generator, RandomNumber(random_generator, Address::MIN_STREET, Address::MAX_STREET)).c_str());
        strcpy(customer_val.c_city,
               RandomStr(random_generator, RandomNumber(random_generator, Address::MIN_CITY, Address::MAX_CITY)).c_str());
        strcpy(customer_val.c_state, RandomStr(random_generator, Address::STATE).c_str());
        strcpy(customer_val.c_zip, (RandomNStr(random_generator, 4) + "11111").c_str());

        strcpy(customer_val.c_phone, RandomNStr(random_generator, tpcc_customer_val_t::PHONE).c_str());
        customer_val.c_since = GetCurrentTimeMillis();
        strcpy(customer_val.c_middle, "OE");
        strcpy(customer_val.c_data, RandomStr(random_generator, RandomNumber(random_generator, tpcc_customer_val_t::MIN_DATA, tpcc_customer_val_t::MAX_DATA)).c_str());

        assert(!strcmp(customer_val.c_credit, "BC") || !strcmp(customer_val.c_credit, "GC"));
        assert(!strcmp(customer_val.c_middle, "OE"));
        // printf("before insert customer record\n");

        LoadRecord(customer_table,
                   customer_key.item_key,
                   (void*)&customer_val,
                   tpcc_customer_val_t_size,
                   (table_id_t)TPCCTableType::kCustomerTable);

        tpcc_history_key_t history_key;
        history_key.h_id = MakeHistoryKey(w_id, d_id, w_id, d_id, c_id);
        tpcc_history_val_t history_val;
        history_val.h_date = GetCurrentTimeMillis();
        history_val.h_amount = 10;
        strcpy(history_val.h_data, RandomStr(random_generator, RandomNumber(random_generator, tpcc_history_val_t::MIN_DATA, tpcc_history_val_t::MAX_DATA)).c_str());

        LoadRecord(history_table,
                   history_key.item_key,
                   (void*)&history_val,
                   tpcc_history_val_t_size,
                   (table_id_t)TPCCTableType::kHistoryTable);
      }
    }
  }
}

void TPCC::Populate_Order_OrderIndex_NewOrder_OrderLine_Table(unsigned long seed) {
  FastRandom random_generator(seed);
  for (uint32_t w_id = 1; w_id <= num_warehouse; w_id++) {
    for (uint32_t d_id = 1; d_id <= num_district_per_warehouse; d_id++) {
      std::set<uint32_t> c_ids_s;
      std::vector<uint32_t> c_ids;
      while (c_ids.size() != num_customer_per_district) {
        const auto x = (random_generator.Next() % num_customer_per_district) + 1;
        if (c_ids_s.count(x))
          continue;
        c_ids_s.insert(x);
        c_ids.emplace_back(x);
      }
      for (uint32_t c = 1; c <= num_customer_per_district; c++) {
        tpcc_order_key_t order_key;
        order_key.o_id = MakeOrderKey(w_id, d_id, c);

        tpcc_order_val_t order_val;
        order_val.o_c_id = c_ids[c - 1];
        if (c <= num_customer_per_district * 0.7)
          order_val.o_carrier_id = RandomNumber(random_generator, tpcc_order_val_t::MIN_CARRIER_ID, tpcc_order_val_t::MAX_CARRIER_ID);
        else
          order_val.o_carrier_id = 0;
        order_val.o_ol_cnt = RandomNumber(random_generator, tpcc_order_line_val_t::MIN_OL_CNT, tpcc_order_line_val_t::MAX_OL_CNT);

        order_val.o_all_local = 1;
        order_val.o_entry_d = GetCurrentTimeMillis();

        LoadRecord(order_table,
                   order_key.item_key,
                   (void*)&order_val,
                   tpcc_order_val_t_size,
                   (table_id_t)TPCCTableType::kOrderTable);

        if (c > num_customer_per_district * tpcc_new_order_val_t::SCALE_CONSTANT_BETWEEN_NEWORDER_ORDER) {
          // Must obey the relationship between the numbers of entries in Order and New-Order specified in tpcc docs
          // The number of entries in New-Order is about 30% of that in Order
          tpcc_new_order_key_t new_order_key;
          new_order_key.no_id = MakeNewOrderKey(w_id, d_id, c);

          tpcc_new_order_val_t new_order_val;
          new_order_val.debug_magic = tpcc_add_magic;
          LoadRecord(new_order_table,
                     new_order_key.item_key,
                     (void*)&new_order_val,
                     tpcc_new_order_val_t_size,
                     (table_id_t)TPCCTableType::kNewOrderTable);
        }
        for (uint32_t l = 1; l <= uint32_t(order_val.o_ol_cnt); l++) {
          tpcc_order_line_key_t order_line_key;
          order_line_key.ol_id = MakeOrderLineKey(w_id, d_id, c, l);

          tpcc_order_line_val_t order_line_val;
          order_line_val.ol_i_id = RandomNumber(random_generator, 1, num_item);
          if (c <= num_customer_per_district * 0.7) {
            order_line_val.ol_delivery_d = order_val.o_entry_d;
            order_line_val.ol_amount = 0;
          } else {
            order_line_val.ol_delivery_d = 0;
            /* random within [0.01 .. 9,999.99] */
            order_line_val.ol_amount = (float)(RandomNumber(random_generator, 1, 999999) / 100.0);
          }

          order_line_val.ol_supply_w_id = w_id;
          order_line_val.ol_quantity = 5;
          // order_line_val.ol_dist_info comes from stock_data(ol_supply_w_id, ol_o_id)

          order_line_val.debug_magic = tpcc_add_magic;
          assert(order_line_val.ol_i_id >= 1 && static_cast<size_t>(order_line_val.ol_i_id) <= num_item);
          LoadRecord(order_line_table,
                     order_line_key.item_key,
                     (void*)&order_line_val,
                     tpcc_order_line_val_t_size,
                     (table_id_t)TPCCTableType::kOrderLineTable);
        }
      }
    }
  }
}

void TPCC::Populate_Item_Table(unsigned long seed) {
  FastRandom random_generator(seed);
  for (int64_t i_id = 1; i_id <= num_item; i_id++) {
    tpcc_item_key_t item_key;
    item_key.i_id = i_id;

    /* Initialize the item payload */
    tpcc_item_val_t item_val;

    strcpy(item_val.i_name,
           RandomStr(random_generator, RandomNumber(random_generator, tpcc_item_val_t::MIN_NAME, tpcc_item_val_t::MAX_NAME)).c_str());
    item_val.i_price = (float)(RandomNumber(random_generator, 100, 10000) / 100.0);
    const int len = RandomNumber(random_generator, tpcc_item_val_t::MIN_DATA, tpcc_item_val_t::MAX_DATA);
    if (RandomNumber(random_generator, 1, 100) > 10) {
      strcpy(item_val.i_data, RandomStr(random_generator, len).c_str());
    } else {
      const int startOriginal = RandomNumber(random_generator, 2, (len - 8));
      const std::string i_data = RandomStr(random_generator, startOriginal) +
                                 "ORIGINAL" + RandomStr(random_generator, len - startOriginal - 8);
      strcpy(item_val.i_data, i_data.c_str());
    }
    item_val.i_im_id = RandomNumber(random_generator, tpcc_item_val_t::MIN_IM, tpcc_item_val_t::MAX_IM);
    item_val.debug_magic = tpcc_add_magic;
    // check item price
    assert(item_val.i_price >= 1.0 && item_val.i_price <= 100.0);

    LoadRecord(item_table,
               item_key.item_key,
               (void*)&item_val,
               tpcc_item_val_t_size,
               (table_id_t)TPCCTableType::kItemTable);
  }
}

void TPCC::Populate_Stock_Table(unsigned long seed) {
  for (uint32_t w_id = 1; w_id <= num_warehouse; w_id++) {
    for (uint32_t i_id = 1; i_id <= num_item; i_id++) {
      tpcc_stock_key_t stock_key;
      stock_key.s_id = MakeStockKey(w_id, i_id);

      /* Initialize the stock payload */
      tpcc_stock_val_t stock_val;
      FastRandom random_generator(seed);
      stock_val.s_quantity = RandomNumber(random_generator, 10, 100);
      stock_val.s_ytd = 0;
      stock_val.s_order_cnt = 0;
      stock_val.s_remote_cnt = 0;

      const int len = RandomNumber(random_generator, tpcc_stock_val_t::MIN_DATA, tpcc_stock_val_t::MAX_DATA);
      if (RandomNumber(random_generator, 1, 100) > 10) {
        const std::string s_data = RandomStr(random_generator, len);
        strcpy(stock_val.s_data, s_data.c_str());
      } else {
        const int startOriginal = RandomNumber(random_generator, 2, (len - 8));
        const std::string s_data = RandomStr(random_generator, startOriginal) + "ORIGINAL" + RandomStr(random_generator, len - startOriginal - 8);
        strcpy(stock_val.s_data, s_data.c_str());
      }

      stock_val.debug_magic = tpcc_add_magic;
      LoadRecord(stock_table,
                 stock_key.item_key,
                 (void*)&stock_val,
                 tpcc_stock_val_t_size,
                 (table_id_t)TPCCTableType::kStockTable);
    }
  }
}

void TPCC::LoadRecord(HashStore* table,
                      itemkey_t item_key,
                      void* val_ptr,
                      size_t val_size,
                      table_id_t table_id) {
  assert(val_size <= MAX_VALUE_SIZE);
  /* Insert into HashStore */
  table->LocalInsertTuple(item_key, (char*)val_ptr, val_size);
}

// Author: Ming Zhang
// Copyright (c) 2023

#pragma once

#include "flags.h"
#include "micro/micro_table.h"
#include "smallbank/smallbank_table.h"
#include "tatp/tatp_table.h"
#include "tpcc/tpcc_table.h"

#if WORKLOAD_TPCC

constexpr size_t TABLE_VALUE_SIZE[TPCC_TOTAL_TABLES] =
    {
        sizeof(tpcc_warehouse_val_t),
        sizeof(tpcc_district_val_t),
        sizeof(tpcc_customer_val_t),
        sizeof(tpcc_history_val_t),
        sizeof(tpcc_new_order_val_t),
        sizeof(tpcc_order_val_t),
        sizeof(tpcc_order_line_val_t),
        sizeof(tpcc_item_val_t),
        sizeof(tpcc_stock_val_t),
        sizeof(tpcc_customer_index_val_t),
        sizeof(tpcc_order_index_val_t)};

// For each table, researve the max-size modified attr
constexpr size_t ATTR_BAR_SIZE[TPCC_TOTAL_TABLES] = {
    // warehouse, fixed
    sizeof(tpcc_warehouse_val_t::w_ytd) * MAX_VCELL_NUM,

    // district, max(d_ytd, d_next_o_id), fixed
    sizeof(tpcc_district_val_t::d_next_o_id) * MAX_VCELL_NUM,

    // customer, max(c_balance+c_ytd_payment+c_payment_cnt, c_balance+c_ytd_payment+c_payment_cnt+c_data, c_balance+c_delivery_cnt)
    // (sizeof(tpcc_customer_val_t::c_balance) + sizeof(tpcc_customer_val_t::c_ytd_payment) + sizeof(tpcc_customer_val_t::c_payment_cnt) + sizeof(tpcc_customer_val_t::c_data)) * MAX_VCELL_NUM,
    12 * MAX_VCELL_NUM + 513 * 1 + 8 * 1,  // according to the frequency

    // history, fixed, insert -> update
    (sizeof(tpcc_history_val_t::h_date) + sizeof(tpcc_history_val_t::h_amount) + sizeof(tpcc_history_val_t::h_data)) * MAX_VCELL_NUM,

    // new order, fixed, insert -> update
    sizeof(tpcc_new_order_val_t::no_dummy) * MAX_VCELL_NUM,

    // order, max(o_c_id+o_carrier_id+o_ol_cnt+o_all_local+o_entry_d <insert -> update>, o_carrier_id)
    // (sizeof(tpcc_order_val_t::o_c_id) + sizeof(tpcc_order_val_t::o_carrier_id) + sizeof(tpcc_order_val_t::o_ol_cnt) + sizeof(tpcc_order_val_t::o_all_local) + sizeof(tpcc_order_val_t::o_entry_d)) * MAX_VCELL_NUM,
    20 * (MAX_VCELL_NUM / 2) + 4 * (MAX_VCELL_NUM / 2),  // according to the frequency

    // order line, max(ol_i_id+ol_delivery_d+ol_amount+ol_supply_w_id+pl_quantity <insert->update>, ol_delivery_d)
    // (sizeof(tpcc_order_line_val_t::ol_i_id) + sizeof(tpcc_order_line_val_t::ol_delivery_d) + sizeof(tpcc_order_line_val_t::ol_amount) + sizeof(tpcc_order_line_val_t::ol_supply_w_id) + sizeof(tpcc_order_line_val_t::ol_quantity)) * MAX_VCELL_NUM,
    20 * (MAX_VCELL_NUM / 2 + 1) + 4 * (MAX_VCELL_NUM / 2),  // according to the frequency

    // item
    0,

    // stock, s_quantity+s_ytd+s_remote_cnt, fixed
    (sizeof(tpcc_stock_val_t::s_quantity) + sizeof(tpcc_stock_val_t::s_ytd) + sizeof(tpcc_stock_val_t::s_remote_cnt)) * MAX_VCELL_NUM,

    // customer index
    0,

    // order index, o_id, fixed
    sizeof(tpcc_order_index_val_t::o_id) * MAX_VCELL_NUM,
};

constexpr size_t SLOT_NUM[TPCC_TOTAL_TABLES] = {
    1, 1, 3, 15, 15, 15, 15, 1, 4, 1, 15};

constexpr int ATTRIBUTE_NUM[TPCC_TOTAL_TABLES] = {8, 9, 18, 3, 1, 5, 6, 4, 6, 1, 1};

constexpr int ATTR_SIZE[TPCC_TOTAL_TABLES][MAX_ATTRIBUTE_NUM_PER_TABLE] = {
    {                                      // warehouse
     0,                                    // used for easy calculating attribute index
     sizeof(tpcc_warehouse_val_t::w_tax),  // 1-st attribute
     sizeof(tpcc_warehouse_val_t::w_ytd),
     sizeof(tpcc_warehouse_val_t::w_name),
     sizeof(tpcc_warehouse_val_t::w_street_1),
     sizeof(tpcc_warehouse_val_t::w_street_2),
     sizeof(tpcc_warehouse_val_t::w_city),
     sizeof(tpcc_warehouse_val_t::w_state),
     sizeof(tpcc_warehouse_val_t::w_zip)},
    {    // district
     0,  // used for easy calculating attribute index
     sizeof(tpcc_district_val_t::d_tax),
     sizeof(tpcc_district_val_t::d_ytd),
     sizeof(tpcc_district_val_t::d_next_o_id),
     sizeof(tpcc_district_val_t::d_name),
     sizeof(tpcc_district_val_t::d_street_1),
     sizeof(tpcc_district_val_t::d_street_2),
     sizeof(tpcc_district_val_t::d_city),
     sizeof(tpcc_district_val_t::d_state),
     sizeof(tpcc_district_val_t::d_zip)},
    {    // customer
     0,  // used for easy calculating attribute index
     sizeof(tpcc_customer_val_t::c_credit_lim),
     sizeof(tpcc_customer_val_t::c_data),
     sizeof(tpcc_customer_val_t::c_discount),
     sizeof(tpcc_customer_val_t::c_balance),
     sizeof(tpcc_customer_val_t::c_ytd_payment),
     sizeof(tpcc_customer_val_t::c_payment_cnt),
     sizeof(tpcc_customer_val_t::c_delivery_cnt),
     sizeof(tpcc_customer_val_t::c_first),
     sizeof(tpcc_customer_val_t::c_middle),
     sizeof(tpcc_customer_val_t::c_last),
     sizeof(tpcc_customer_val_t::c_street_1),
     sizeof(tpcc_customer_val_t::c_street_2),
     sizeof(tpcc_customer_val_t::c_city),
     sizeof(tpcc_customer_val_t::c_state),
     sizeof(tpcc_customer_val_t::c_zip),
     sizeof(tpcc_customer_val_t::c_phone),
     sizeof(tpcc_customer_val_t::c_since),
     sizeof(tpcc_customer_val_t::c_credit)},
    {    // history
     0,  // used for easy calculating attribute index
     sizeof(tpcc_history_val_t::h_amount),
     sizeof(tpcc_history_val_t::h_date),
     sizeof(tpcc_history_val_t::h_data)},
    {    // new order
     0,  // used for easy calculating attribute index
     sizeof(tpcc_new_order_val_t::no_dummy)},
    {    // order
     0,  // used for easy calculating attribute index
     sizeof(tpcc_order_val_t::o_c_id),
     sizeof(tpcc_order_val_t::o_carrier_id),
     sizeof(tpcc_order_val_t::o_ol_cnt),
     sizeof(tpcc_order_val_t::o_all_local),
     sizeof(tpcc_order_val_t::o_entry_d)},
    {    // order line
     0,  // used for easy calculating attribute index
     sizeof(tpcc_order_line_val_t::ol_i_id),
     sizeof(tpcc_order_line_val_t::ol_supply_w_id),
     sizeof(tpcc_order_line_val_t::ol_quantity),
     sizeof(tpcc_order_line_val_t::ol_amount),
     sizeof(tpcc_order_line_val_t::ol_delivery_d),
     sizeof(tpcc_order_line_val_t::ol_dist_info)},
    {    // item
     0,  // used for easy calculating attribute index
     sizeof(tpcc_item_val_t::i_im_id),
     sizeof(tpcc_item_val_t::i_price),
     sizeof(tpcc_item_val_t::i_name),
     sizeof(tpcc_item_val_t::i_data)},
    {    // stock
     0,  // used for easy calculating attribute index
     sizeof(tpcc_stock_val_t::s_quantity),
     sizeof(tpcc_stock_val_t::s_ytd),
     sizeof(tpcc_stock_val_t::s_order_cnt),
     sizeof(tpcc_stock_val_t::s_remote_cnt),
     sizeof(tpcc_stock_val_t::s_dist),
     sizeof(tpcc_stock_val_t::s_data)},
    {    // customer index
     0,  // used for easy calculating attribute index
     sizeof(tpcc_customer_index_val_t::c_id)},
    {    // order index
     0,  // used for easy calculating attribute index
     sizeof(tpcc_order_index_val_t::o_id)}

};

#elif WORKLOAD_TATP

constexpr size_t TABLE_VALUE_SIZE[TATP_TOTAL_TABLES] =
    {
        sizeof(tatp_sub_val_t),
        sizeof(tatp_sec_sub_val_t),
        sizeof(tatp_specfac_val_t),
        sizeof(tatp_accinf_val_t),
        sizeof(tatp_callfwd_val_t)};

constexpr size_t SLOT_NUM[TATP_TOTAL_TABLES] = {
    1, 5, 5, 5, 5};

constexpr size_t ATTR_BAR_SIZE[TATP_TOTAL_TABLES] = {
    4 * MAX_VCELL_NUM + 2 * 1,  // according to the frequency
    0,
    // fixed
    sizeof(tatp_specfac_val_t::data_a) * MAX_VCELL_NUM,
    0,
    // fixed
    (sizeof(tatp_callfwd_val_t::end_time) + sizeof(tatp_callfwd_val_t::numberx)) * MAX_VCELL_NUM,
};

constexpr int ATTRIBUTE_NUM[TATP_TOTAL_TABLES] = {7, 1, 4, 4, 2};

constexpr int ATTR_SIZE[TATP_TOTAL_TABLES][MAX_ATTRIBUTE_NUM_PER_TABLE] = {
    {0,
     sizeof(tatp_sub_val_t::sub_number),
     sizeof(tatp_sub_val_t::sub_number_unused),
     sizeof(tatp_sub_val_t::hex),
     sizeof(tatp_sub_val_t::bytes),
     sizeof(tatp_sub_val_t::bits),
     sizeof(tatp_sub_val_t::msc_location),
     sizeof(tatp_sub_val_t::vlr_location)},
    {0,
     sizeof(tatp_sec_sub_val_t::s_id)},
    {0,
     sizeof(tatp_specfac_val_t::is_active),
     sizeof(tatp_specfac_val_t::error_cntl),
     sizeof(tatp_specfac_val_t::data_a),
     sizeof(tatp_specfac_val_t::data_b)},
    {0,
     sizeof(tatp_accinf_val_t::data1),
     sizeof(tatp_accinf_val_t::data2),
     sizeof(tatp_accinf_val_t::data3),
     sizeof(tatp_accinf_val_t::data4)},
    {0,
     sizeof(tatp_callfwd_val_t::end_time),
     sizeof(tatp_callfwd_val_t::numberx)}};

#elif WORKLOAD_SmallBank

constexpr size_t TABLE_VALUE_SIZE[SmallBank_TOTAL_TABLES] = {
    sizeof(smallbank_savings_val_t),
    sizeof(smallbank_checking_val_t)};

constexpr size_t SLOT_NUM[SmallBank_TOTAL_TABLES] = {
    1, 1};

constexpr size_t ATTR_BAR_SIZE[SmallBank_TOTAL_TABLES] = {
    // fixed
    sizeof(smallbank_savings_val_t::bal) * MAX_VCELL_NUM,
    // fixed
    sizeof(smallbank_checking_val_t::bal) * MAX_VCELL_NUM,
};

constexpr int ATTRIBUTE_NUM[SmallBank_TOTAL_TABLES] = {1, 1};

constexpr int ATTR_SIZE[SmallBank_TOTAL_TABLES][MAX_ATTRIBUTE_NUM_PER_TABLE] = {
    {0, sizeof(smallbank_savings_val_t::bal)},
    {0, sizeof(smallbank_checking_val_t::bal)}};

#elif WORKLOAD_MICRO

constexpr size_t TABLE_VALUE_SIZE[MICRO_TOTAL_TABLES] = {sizeof(micro_val_t)};
constexpr size_t SLOT_NUM[MICRO_TOTAL_TABLES] = {1};

constexpr size_t ATTR_BAR_SIZE[MICRO_TOTAL_TABLES] = {
    // fixed
    sizeof(micro_val_t::d2) * MAX_VCELL_NUM,
};

constexpr int ATTRIBUTE_NUM[MICRO_TOTAL_TABLES] = {5};
constexpr int ATTR_SIZE[MICRO_TOTAL_TABLES][MAX_ATTRIBUTE_NUM_PER_TABLE] = {
    {0,
     sizeof(micro_val_t::d1),
     sizeof(micro_val_t::d2),
     sizeof(micro_val_t::d3),
     sizeof(micro_val_t::d4),
     sizeof(micro_val_t::d5)}};

#endif
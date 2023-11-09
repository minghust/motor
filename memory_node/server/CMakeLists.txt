# Author: Ming Zhang
# Copyright (c) 2023

set(SERVER_SOURCE server.cc)
add_executable(motor_mempool ${SERVER_SOURCE})
target_link_libraries(motor_mempool tatp_db smallbank_db tpcc_db micro_db rlib)

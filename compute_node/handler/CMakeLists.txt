# Author: Ming Zhang
# Copyright (c) 2023

set(HANDLER_SRC
        config_cn.cc
        gen_output.cc
        worker.cc)

add_library(handler STATIC
        ${HANDLER_SRC}
        )

set_target_properties(handler PROPERTIES LINKER_LANGUAGE CXX)

target_link_libraries(handler motor tatp_db tatp_txn smallbank_db smallbank_txn tpcc_db tpcc_txn micro_db micro_txn)

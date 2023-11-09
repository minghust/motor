# Author: Ming Zhang
# Copyright (c) 2023

set(CONNECTION_SRC
        connection/meta_manager.cc
        )

set(TXN_SRC
        process/framework.cc
        process/read_cvt.cc
        process/check_cvt.cc
        process/read_value.cc
        process/check_value.cc
        process/collect_attr.cc
        process/validate.cc
        process/commit.cc
        process/recovery.cc
        )

add_library(motor STATIC
        ${CONNECTION_SRC}
        ${TXN_SRC}
        )

set_target_properties(motor PROPERTIES LINKER_LANGUAGE CXX)

target_link_libraries(motor rlib pthread boost_coroutine boost_context boost_system)
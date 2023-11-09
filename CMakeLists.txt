# Author: Ming Zhang
# Copyright (c) 2023

cmake_minimum_required(VERSION 3.3)

project(Motor)

set(CMAKE_CXX_STANDARD 11)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-result -fPIC")

if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -DNDEBUG")
else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -DDEBUG -g")
endif()

include_directories(
    txn
    workload
    thirdparty
    compute_node
)

add_subdirectory(thirdparty/rlib)

add_subdirectory(txn)

add_subdirectory(workload)

add_subdirectory(compute_node) # Dep list: rlib->motor->workload_db+_txn->handler

add_subdirectory(memory_node) # Dep list: rlib->workload_db->server
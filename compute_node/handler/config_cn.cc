// Author: Ming Zhang
// Copyright (c) 2023

#include "handler/handler.h"
#include "rlib/logging.hpp"
#include "util/json_config.h"

using namespace rdmaio;

void Handler::ConfigureComputeNode(int argc, char* argv[]) {
  // ./run <benchmark_name> <thread_num> <coroutine_num> <isolation_level>

  std::string config_file = "../../../config/cn_config.json";
  std::string iso_level = std::string(argv[4]);
  std::string s1 = "sed -i '5c \"thread_num_per_machine\": " + std::string(argv[2]) + ",' " + config_file;
  std::string s2 = "sed -i '6c \"coroutine_num\": " + std::string(argv[3]) + ",' " + config_file;
  system(s1.c_str());
  system(s2.c_str());

  int iso_level_value = 0;
  if (iso_level == "SI") {
    iso_level_value = 1;
  } else if (iso_level == "SR") {
    iso_level_value = 2;
  }

  std::string s = "sed -i '9c \"iso_level\": " + std::to_string(iso_level_value) + ",' " + config_file;
  system(s.c_str());
}

void Handler::ConfigureComputeNodeForMICRO(int argc, char* argv[]) {
  // ./run_micro <thread_num> <coroutine_num> <access_pattern> <skewness> <write_ratio> <isolation_level>

  std::string workload_filepath = "../../../config/micro_config.json";
  std::string config_file = "../../../config/cn_config.json";

  std::string thread_num = std::string(argv[1]);
  std::string coroutine_num = std::string(argv[2]);
  std::string access_pattern = argv[3];
  std::string skewness = argv[4];
  std::string write_ratio = argv[5];
  std::string iso_level = std::string(argv[6]);

  std::string s = "sed -i '5c \"thread_num_per_machine\": " + thread_num + ",' " + config_file;
  system(s.c_str());

  s = "sed -i '6c \"coroutine_num\": " + coroutine_num + ",' " + config_file;
  system(s.c_str());

  if (access_pattern == "skewed") {
    // skewed
    s = "sed -i '4c \"is_skewed\": true,' " + workload_filepath;
    s = "sed -i '5c \"zipf_theta\": " + skewness + ",' " + workload_filepath;
  } else if (access_pattern == "uniform") {
    // uniform
    s = "sed -i '4c \"is_skewed\": false,' " + workload_filepath;
  }
  system(s.c_str());

  // write ratio
  s = "sed -i '7c \"write_ratio\": " + write_ratio + ",' " + workload_filepath;
  system(s.c_str());

  int iso_level_value = 0;
  if (iso_level == "SI") {
    iso_level_value = 1;
  } else if (iso_level == "SR") {
    iso_level_value = 2;
  }
  s = "sed -i '9c \"iso_level\": " + std::to_string(iso_level_value) + ",' " + config_file;
  system(s.c_str());
}

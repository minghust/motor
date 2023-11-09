// Author: Ming Zhang
// Copyright (c) 2023

#pragma once

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <fstream>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>

class Handler {
 public:
  Handler() {}

  void ConfigureComputeNode(int argc, char* argv[]);

  void ConfigureComputeNodeForMICRO(int argc, char* argv[]);

  void GenThreads(std::string bench_name);

  void OutputResult(std::string bench_name, std::string system_name);
};

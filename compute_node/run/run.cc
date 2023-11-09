// Author: Ming Zhang
// Copyright (c) 2023

#include "handler/handler.h"

// Entrance to run threads that spawn coroutines as coordinators to run distributed transactions
int main(int argc, char* argv[]) {
  if (argc != 5) {
    std::cerr << "./run <benchmark_name> <thread_num> <coroutine_num> <isolation_level>" << std::endl;
    return 0;
  }

  Handler* handler = new Handler();

  handler->ConfigureComputeNode(argc, argv);

  handler->GenThreads(std::string(argv[1]));

  handler->OutputResult(std::string(argv[1]), "Motor");
}

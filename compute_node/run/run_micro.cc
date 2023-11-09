// Author: Ming Zhang
// Copyright (c) 2023

#include "handler/handler.h"

// Entrance to run threads that spawn coroutines as coordinators to run distributed transactions
int main(int argc, char* argv[]) {
  if (argc != 7) {
    std::cerr << "./run_micro <thread_num> <coroutine_num> <access_pattern> <skewness> <write_ratio> <isolation_level>" << std::endl;
    return 0;
  }

  Handler* handler = new Handler();

  handler->ConfigureComputeNodeForMICRO(argc, argv);

  handler->GenThreads("micro");

  handler->OutputResult("micro", "Motor");
}

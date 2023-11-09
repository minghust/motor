// Author: Ming Zhang
// Copyright (c) 2023

#pragma once

/*********************** Configure workload **********************/
#define WORKLOAD_TPCC 1
#define WORKLOAD_TATP 0
#define WORKLOAD_SmallBank 0
#define WORKLOAD_MICRO 0

#if WORKLOAD_TPCC
#define MAX_VALUE_SIZE 664
#define MAX_VCELL_NUM 4

#elif WORKLOAD_TATP
#define MAX_VALUE_SIZE 40
#define MAX_VCELL_NUM 2

#elif WORKLOAD_SmallBank
#define MAX_VALUE_SIZE 8
#define MAX_VCELL_NUM 3

#elif WORKLOAD_MICRO
#define MAX_VALUE_SIZE 40
#define MAX_VCELL_NUM 4
#endif

#define bitmap_t uint8_t

#define LargeAttrBar 0

/*********************** Some limits, change them as needed **********************/
#define MAX_REMOTE_NODE_NUM 100  
#define MAX_TNUM_PER_CN 100
#define MAX_CLIENT_NUM_PER_MN 50  
#define BACKUP_NUM 2  // Backup memory node number. **NOT** 0

#define MAX_DB_TABLE_NUM 15 
#define MAX_ATTRIBUTE_NUM_PER_TABLE 20

/*********************** Options **********************/
#define EARLY_ABORT 1
#define PRINT_HASH_META 0
#define OUTPUT_EVENT_STAT 0
#define OUTPUT_KEY_STAT 0

/*********************** Crash test only **********************/
#define PROBE_TP 0  // Probing throughput during execution
#define HAVE_COORD_CRASH 0
#define HAVE_PRIMARY_CRASH 0
#define HAVE_BACKUP_CRASH 0
#define CRASH_TABLE_ID 2
#define PRIMARY_CRASH -33
#define BACKUP_CRASH -13


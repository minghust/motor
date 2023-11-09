// Author: Ming Zhang
// Copyright (c) 2023

#pragma once

#include "base/common.h"
#include "process/structs.h"
#include "rlib/rdma_ctrl.hpp"
#include "scheduler/corotine_scheduler.h"

using namespace rdmaio;

static const int MAX_DOORBELL_LEN = 124;

// Several RDMA requests are sent to the QP in a doorbelled (or batched) way.
// These requests are executed within one round trip
// Target: improve performance

class LockReadBatch {
 public:
  LockReadBatch() {
    // The key of doorbell: set the pointer to link requests

    // lock cvt
    sr[0].num_sge = 1;
    sr[0].sg_list = &sge[0];
    sr[0].send_flags = 0;
    sr[0].next = &sr[1];

    // read cvt
    sr[1].num_sge = 1;
    sr[1].sg_list = &sge[1];
    sr[1].send_flags = IBV_SEND_SIGNALED;
    sr[1].next = NULL;
  }

  // SetLockReq and SetReadReq are a doorbelled group
  // First lock, then read
  void SetLockReq(char* local_addr, uint64_t remote_off, uint64_t compare, uint64_t swap) {
    sr[0].opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    sr[0].wr.atomic.remote_addr = remote_off;
    sr[0].wr.atomic.compare_add = compare;
    sr[0].wr.atomic.swap = swap;
    sge[0].length = sizeof(uint64_t);
    sge[0].addr = (uint64_t)local_addr;
  }

  void SetReadReq(char* local_addr, uint64_t remote_off, size_t size) {
    // Read cannot set send_flags IBV_SEND_INLINE
    sr[1].opcode = IBV_WR_RDMA_READ;
    sr[1].wr.rdma.remote_addr = remote_off;
    sge[1].addr = (uint64_t)local_addr;
    sge[1].length = size;
  }

  // Send doorbelled requests to the queue pair
  void SendReqs(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id) {
    // sr[0] must be an atomic operation
    sr[0].wr.atomic.remote_addr += qp->remote_mr_.buf;
    sr[0].wr.atomic.rkey = qp->remote_mr_.key;
    sge[0].lkey = qp->local_mr_.key;

    sr[1].wr.rdma.remote_addr += qp->remote_mr_.buf;
    sr[1].wr.rdma.rkey = qp->remote_mr_.key;
    sge[1].lkey = qp->local_mr_.key;

    coro_sched->RDMABatch(coro_id, qp, &(sr[0]), &bad_sr, 1);
  }

 private:
  struct ibv_send_wr sr[2];

  struct ibv_sge sge[2];

  struct ibv_send_wr* bad_sr;
};

class LockReadTwoBatch {
 public:
  LockReadTwoBatch() {
    // lock cvt
    sr[0].num_sge = 1;
    sr[0].sg_list = &sge[0];
    sr[0].send_flags = 0;
    sr[0].next = &sr[1];

    // read cvt
    sr[1].num_sge = 1;
    sr[1].sg_list = &sge[1];
    sr[1].send_flags = 0;
    sr[1].next = &sr[2];

    // read value
    sr[2].num_sge = 1;
    sr[2].sg_list = &sge[2];
    sr[2].send_flags = IBV_SEND_SIGNALED;
    sr[2].next = NULL;
  }

  // SetLockReq and SetReadReq are a doorbelled group
  // First lock, then read
  void SetLockReq(char* local_addr, uint64_t remote_off, uint64_t compare, uint64_t swap) {
    sr[0].opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    sr[0].wr.atomic.remote_addr = remote_off;
    sr[0].wr.atomic.compare_add = compare;
    sr[0].wr.atomic.swap = swap;
    sge[0].length = sizeof(uint64_t);
    sge[0].addr = (uint64_t)local_addr;
  }

  void SetReadCVTReq(char* local_addr, uint64_t remote_off, size_t size) {
    // Read cannot set send_flags IBV_SEND_INLINE
    sr[1].opcode = IBV_WR_RDMA_READ;
    sr[1].wr.rdma.remote_addr = remote_off;
    sge[1].addr = (uint64_t)local_addr;
    sge[1].length = size;
  }

  void SetReadValueReq(char* local_addr, uint64_t remote_off, size_t size) {
    // Read cannot set send_flags IBV_SEND_INLINE
    sr[2].opcode = IBV_WR_RDMA_READ;
    sr[2].wr.rdma.remote_addr = remote_off;
    sge[2].addr = (uint64_t)local_addr;
    sge[2].length = size;
  }

  // Send doorbelled requests to the queue pair
  void SendReqs(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id) {
    // sr[0] must be an atomic operation
    sr[0].wr.atomic.remote_addr += qp->remote_mr_.buf;
    sr[0].wr.atomic.rkey = qp->remote_mr_.key;
    sge[0].lkey = qp->local_mr_.key;

    sr[1].wr.rdma.remote_addr += qp->remote_mr_.buf;
    sr[1].wr.rdma.rkey = qp->remote_mr_.key;
    sge[1].lkey = qp->local_mr_.key;

    sr[2].wr.rdma.remote_addr += qp->remote_mr_.buf;
    sr[2].wr.rdma.rkey = qp->remote_mr_.key;
    sge[2].lkey = qp->local_mr_.key;

    coro_sched->RDMABatch(coro_id, qp, &(sr[0]), &bad_sr, 2);
  }

 private:
  struct ibv_send_wr sr[3];

  struct ibv_sge sge[3];

  struct ibv_send_wr* bad_sr;
};

class LockReadThreeBatch {
 public:
  LockReadThreeBatch(size_t num_attr) : num_attr_read(num_attr) {
    // Lock
    sr[0].num_sge = 1;
    sr[0].sg_list = &sge[0];
    sr[0].send_flags = 0;
    sr[0].next = &sr[1];

    // Read cvt
    sr[1].num_sge = 1;
    sr[1].sg_list = &sge[1];
    sr[1].send_flags = 0;
    sr[1].next = &sr[2];

    // read value
    sr[2].num_sge = 1;
    sr[2].sg_list = &sge[2];
    sr[2].send_flags = 0;
    sr[2].next = &sr[3];

    // Read attributes
    for (size_t i = prev_cnt; i < (num_attr_read + prev_cnt); i++) {
      sr[i].num_sge = 1;
      sr[i].sg_list = &sge[i];

      if (i == num_attr_read + prev_cnt - 1) {
        sr[i].send_flags = IBV_SEND_SIGNALED;
        sr[i].next = NULL;
      } else {
        sr[i].send_flags = 0;
        sr[i].next = &sr[i + 1];
      }
    }
  }

  void SetLockReq(char* local_addr, uint64_t remote_off, uint64_t compare, uint64_t swap) {
    sr[0].opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    sr[0].wr.atomic.remote_addr = remote_off;
    sr[0].wr.atomic.compare_add = compare;
    sr[0].wr.atomic.swap = swap;
    sge[0].length = sizeof(uint64_t);
    sge[0].addr = (uint64_t)local_addr;
  }

  void SetReadCVTReq(char* local_addr, uint64_t remote_off, size_t size) {
    // Read cannot set send_flags IBV_SEND_INLINE
    sr[1].opcode = IBV_WR_RDMA_READ;
    sr[1].wr.rdma.remote_addr = remote_off;
    sge[1].addr = (uint64_t)local_addr;
    sge[1].length = size;
  }

  void SetReadValueReq(char* local_addr, uint64_t remote_off, size_t size) {
    // Read cannot set send_flags IBV_SEND_INLINE
    sr[2].opcode = IBV_WR_RDMA_READ;
    sr[2].wr.rdma.remote_addr = remote_off;
    sge[2].addr = (uint64_t)local_addr;
    sge[2].length = size;
  }

  void SetReadAttrReq(std::vector<AttrRead>& attr_read_list) {
    assert(num_attr_read == attr_read_list.size());
    for (size_t i = prev_cnt; i < (num_attr_read + prev_cnt); i++) {
      sr[i].opcode = IBV_WR_RDMA_READ;
      sr[i].wr.rdma.remote_addr = attr_read_list[i - prev_cnt].remote_attr_off;
      sge[i].addr = (uint64_t)attr_read_list[i - prev_cnt].local_attr_buf;
      sge[i].length = attr_read_list[i - prev_cnt].attr_size;
    }
  }

  // Send doorbelled requests to the queue pair
  void SendReqs(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id) {
    // sr[0] must be an atomic operation
    sr[0].wr.atomic.remote_addr += qp->remote_mr_.buf;
    sr[0].wr.atomic.rkey = qp->remote_mr_.key;
    sge[0].lkey = qp->local_mr_.key;

    for (size_t i = 1; i < (num_attr_read + prev_cnt); i++) {
      sr[i].wr.rdma.remote_addr += qp->remote_mr_.buf;
      sr[i].wr.rdma.rkey = qp->remote_mr_.key;
      sge[i].lkey = qp->local_mr_.key;
    }

    coro_sched->RDMABatch(coro_id, qp, &(sr[0]), &bad_sr, 2 + num_attr_read);
  }

 private:
  const static int prev_cnt = 3;

  struct ibv_send_wr sr[MAX_ATTRIBUTE_NUM_PER_TABLE + prev_cnt];

  struct ibv_sge sge[MAX_ATTRIBUTE_NUM_PER_TABLE + prev_cnt];

  struct ibv_send_wr* bad_sr;

  size_t num_attr_read;
};

class DeleteRead {
 public:
  DeleteRead() {
    sr[0].num_sge = 1;
    sr[0].sg_list = &sge[0];
    sr[0].send_flags = 0;
    sr[0].next = &sr[1];

    sr[1].num_sge = 1;
    sr[1].sg_list = &sge[1];
    sr[1].send_flags = IBV_SEND_SIGNALED;
    sr[1].next = NULL;
  }

  void SetReadValueReq(char* local_addr, uint64_t remote_off, size_t size) {
    sr[0].opcode = IBV_WR_RDMA_READ;
    sr[0].wr.rdma.remote_addr = remote_off;
    sge[0].addr = (uint64_t)local_addr;
    sge[0].length = size;
  }

  void SetReadAttrReq(char* local_addr, uint64_t remote_off, size_t size) {
    sr[1].opcode = IBV_WR_RDMA_READ;
    sr[1].wr.rdma.remote_addr = remote_off;
    sge[1].addr = (uint64_t)local_addr;
    sge[1].length = size;
  }

  void SendReqs(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id) {
    sr[0].wr.rdma.remote_addr += qp->remote_mr_.buf;
    sr[0].wr.rdma.rkey = qp->remote_mr_.key;
    sge[0].lkey = qp->local_mr_.key;

    sr[1].wr.rdma.remote_addr += qp->remote_mr_.buf;
    sr[1].wr.rdma.rkey = qp->remote_mr_.key;
    sge[1].lkey = qp->local_mr_.key;

    coro_sched->RDMABatch(coro_id, qp, &(sr[0]), &bad_sr, 1);
  }

 private:
  struct ibv_send_wr sr[2];

  struct ibv_sge sge[2];

  struct ibv_send_wr* bad_sr;
};

class DeleteLock {
 public:
  DeleteLock() {
    // Lock
    sr[0].num_sge = 1;
    sr[0].sg_list = &sge[0];
    sr[0].send_flags = 0;
    sr[0].next = &sr[1];

    // Read cvt
    sr[1].num_sge = 1;
    sr[1].sg_list = &sge[1];
    sr[1].send_flags = IBV_SEND_SIGNALED;
    sr[1].next = NULL;
  }

  void SetLockReq(char* local_addr, uint64_t remote_off, uint64_t compare, uint64_t swap) {
    sr[0].opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    sr[0].wr.atomic.remote_addr = remote_off;
    sr[0].wr.atomic.compare_add = compare;
    sr[0].wr.atomic.swap = swap;
    sge[0].length = sizeof(uint64_t);
    sge[0].addr = (uint64_t)local_addr;
  }

  void SetReadCVTReq(char* local_addr, uint64_t remote_off, size_t size) {
    // Read cannot set send_flags IBV_SEND_INLINE
    sr[1].opcode = IBV_WR_RDMA_READ;
    sr[1].wr.rdma.remote_addr = remote_off;
    sge[1].addr = (uint64_t)local_addr;
    sge[1].length = size;
  }

  // Send doorbelled requests to the queue pair
  void SendReqs(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id) {
    // sr[0] must be an atomic operation
    sr[0].wr.atomic.remote_addr += qp->remote_mr_.buf;
    sr[0].wr.atomic.rkey = qp->remote_mr_.key;
    sge[0].lkey = qp->local_mr_.key;

    sr[1].wr.rdma.remote_addr += qp->remote_mr_.buf;
    sr[1].wr.rdma.rkey = qp->remote_mr_.key;
    sge[1].lkey = qp->local_mr_.key;

    coro_sched->RDMABatch(coro_id, qp, &(sr[0]), &bad_sr, 1);
  }

 private:
  struct ibv_send_wr sr[2];

  struct ibv_sge sge[2];

  struct ibv_send_wr* bad_sr;
};

class DeleteLockRead {
 public:
  DeleteLockRead() {
    // Lock
    sr[0].num_sge = 1;
    sr[0].sg_list = &sge[0];
    sr[0].send_flags = 0;
    sr[0].next = &sr[1];

    // Read cvt
    sr[1].num_sge = 1;
    sr[1].sg_list = &sge[1];
    sr[1].send_flags = 0;
    sr[1].next = &sr[2];

    // read value
    sr[2].num_sge = 1;
    sr[2].sg_list = &sge[2];
    sr[2].send_flags = 0;
    sr[2].next = &sr[3];

    // Read attribute
    sr[3].num_sge = 1;
    sr[3].sg_list = &sge[3];
    sr[3].send_flags = IBV_SEND_SIGNALED;
    sr[3].next = NULL;
  }

  void SetLockReq(char* local_addr, uint64_t remote_off, uint64_t compare, uint64_t swap) {
    sr[0].opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    sr[0].wr.atomic.remote_addr = remote_off;
    sr[0].wr.atomic.compare_add = compare;
    sr[0].wr.atomic.swap = swap;
    sge[0].length = sizeof(uint64_t);
    sge[0].addr = (uint64_t)local_addr;
  }

  void SetReadCVTReq(char* local_addr, uint64_t remote_off, size_t size) {
    // Read cannot set send_flags IBV_SEND_INLINE
    sr[1].opcode = IBV_WR_RDMA_READ;
    sr[1].wr.rdma.remote_addr = remote_off;
    sge[1].addr = (uint64_t)local_addr;
    sge[1].length = size;
  }

  void SetReadValueReq(char* local_addr, uint64_t remote_off, size_t size) {
    // Read cannot set send_flags IBV_SEND_INLINE
    sr[2].opcode = IBV_WR_RDMA_READ;
    sr[2].wr.rdma.remote_addr = remote_off;
    sge[2].addr = (uint64_t)local_addr;
    sge[2].length = size;
  }

  void SetReadAttrReq(char* local_addr, uint64_t remote_off, size_t size) {
    // Read cannot set send_flags IBV_SEND_INLINE
    sr[3].opcode = IBV_WR_RDMA_READ;
    sr[3].wr.rdma.remote_addr = remote_off;
    sge[3].addr = (uint64_t)local_addr;
    sge[3].length = size;
  }

  // Send doorbelled requests to the queue pair
  void SendReqs(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id) {
    // sr[0] must be an atomic operation
    sr[0].wr.atomic.remote_addr += qp->remote_mr_.buf;
    sr[0].wr.atomic.rkey = qp->remote_mr_.key;
    sge[0].lkey = qp->local_mr_.key;

    for (size_t i = 1; i < 4; i++) {
      sr[i].wr.rdma.remote_addr += qp->remote_mr_.buf;
      sr[i].wr.rdma.rkey = qp->remote_mr_.key;
      sge[i].lkey = qp->local_mr_.key;
    }

    coro_sched->RDMABatch(coro_id, qp, &(sr[0]), &bad_sr, 3);
  }

 private:
  struct ibv_send_wr sr[4];

  struct ibv_sge sge[4];

  struct ibv_send_wr* bad_sr;
};

class ReadValueAttrBatch {
 public:
  ReadValueAttrBatch(size_t num_attr) : num_attr_read(num_attr) {
    assert(num_attr >= 1);

    // Read value
    sr[0].num_sge = 1;
    sr[0].sg_list = &sge[0];
    sr[0].send_flags = 0;
    sr[0].next = &sr[1];

    // Read attributes
    for (size_t i = 1; i < (num_attr_read + 1); i++) {
      sr[i].num_sge = 1;
      sr[i].sg_list = &sge[i];

      if (i == num_attr_read) {
        sr[i].send_flags = IBV_SEND_SIGNALED;
        sr[i].next = NULL;
      } else {
        sr[i].send_flags = 0;
        sr[i].next = &sr[i + 1];
      }
    }
  }

  void SetReadValueReq(char* local_addr, uint64_t remote_off, size_t size) {
    sr[0].opcode = IBV_WR_RDMA_READ;
    sr[0].wr.rdma.remote_addr = remote_off;
    sge[0].addr = (uint64_t)local_addr;
    sge[0].length = size;
  }

  void SetReadAttrReq(std::vector<AttrRead>& attr_read_list) {
    assert(num_attr_read == attr_read_list.size());
    for (size_t i = 1; i < (num_attr_read + 1); i++) {
      sr[i].opcode = IBV_WR_RDMA_READ;
      sr[i].wr.rdma.remote_addr = attr_read_list[i - 1].remote_attr_off;
      sge[i].addr = (uint64_t)attr_read_list[i - 1].local_attr_buf;
      sge[i].length = attr_read_list[i - 1].attr_size;
    }
  }

  void SendReqs(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id) {
    // 1+num_attr_read reads
    for (size_t i = 0; i < (num_attr_read + 1); i++) {
      sr[i].wr.rdma.remote_addr += qp->remote_mr_.buf;
      sr[i].wr.rdma.rkey = qp->remote_mr_.key;
      sge[i].lkey = qp->local_mr_.key;
    }
    coro_sched->RDMABatch(coro_id, qp, &(sr[0]), &bad_sr, num_attr_read);
  }

 private:
  struct ibv_send_wr sr[MAX_ATTRIBUTE_NUM_PER_TABLE + 1];

  struct ibv_sge sge[MAX_ATTRIBUTE_NUM_PER_TABLE + 1];

  struct ibv_send_wr* bad_sr;

  size_t num_attr_read;
};

class DeleteNoFVBatch {
 public:
  DeleteNoFVBatch() {
    sr[0].num_sge = 1;
    sr[0].sg_list = &sge[0];
    sr[0].send_flags = 0;
    sr[0].next = &sr[1];

    sr[1].num_sge = 1;
    sr[1].sg_list = &sge[1];
    sr[1].send_flags = IBV_SEND_SIGNALED;
    sr[1].next = NULL;
  }

  void SetInvalidReq(char* local_addr, uint64_t remote_off, size_t size) {
    sr[0].opcode = IBV_WR_RDMA_WRITE;
    sr[0].wr.rdma.remote_addr = remote_off;
    sge[0].addr = (uint64_t)local_addr;
    sge[0].length = size;
    sr[0].send_flags |= IBV_SEND_INLINE;
  }

  void UnlockReq(char* local_addr, uint64_t remote_off, size_t size) {
    sr[1].opcode = IBV_WR_RDMA_WRITE;
    sr[1].wr.rdma.remote_addr = remote_off;
    sge[1].addr = (uint64_t)local_addr;
    sge[1].length = size;
    sr[1].send_flags |= IBV_SEND_INLINE;
  }

  // Send doorbelled requests to the queue pair
  void SendReqs(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id) {
    for (int i = 0; i < 2; i++) {
      sr[i].wr.rdma.remote_addr += qp->remote_mr_.buf;
      sr[i].wr.rdma.rkey = qp->remote_mr_.key;
      sge[i].lkey = qp->local_mr_.key;
    }

    coro_sched->RDMABatch(coro_id, qp, &(sr[0]), &bad_sr, 1);
  }

 private:
  struct ibv_send_wr sr[2];

  struct ibv_sge sge[2];

  struct ibv_send_wr* bad_sr;
};

class DeleteBatch {
 public:
  DeleteBatch() {
    sr[0].num_sge = 1;
    sr[0].sg_list = &sge[0];
    sr[0].send_flags = 0;
    sr[0].next = &sr[1];

    sr[1].num_sge = 1;
    sr[1].sg_list = &sge[1];
    sr[1].send_flags = 0;
    sr[1].next = &sr[2];

    sr[2].num_sge = 1;
    sr[2].sg_list = &sge[2];
    sr[2].send_flags = IBV_SEND_SIGNALED;
    sr[2].next = NULL;
  }

  void SetInvalidReq(char* local_addr, uint64_t remote_off, size_t size) {
    sr[0].opcode = IBV_WR_RDMA_WRITE;
    sr[0].wr.rdma.remote_addr = remote_off;
    sge[0].addr = (uint64_t)local_addr;
    sge[0].length = size;
    sr[0].send_flags |= IBV_SEND_INLINE;
  }

  void SetValueReq(char* local_addr, uint64_t remote_off, size_t size) {
    sr[1].opcode = IBV_WR_RDMA_WRITE;
    sr[1].wr.rdma.remote_addr = remote_off;
    sge[1].addr = (uint64_t)local_addr;
    sge[1].length = size;
    if (size <= MAX_DOORBELL_LEN) {
      sr[1].send_flags |= IBV_SEND_INLINE;
    }
  }

  void UnlockReq(char* local_addr, uint64_t remote_off, size_t size) {
    sr[2].opcode = IBV_WR_RDMA_WRITE;
    sr[2].wr.rdma.remote_addr = remote_off;
    sge[2].addr = (uint64_t)local_addr;
    sge[2].length = size;
    sr[2].send_flags |= IBV_SEND_INLINE;
  }

  // Send doorbelled requests to the queue pair
  void SendReqs(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id) {
    for (int i = 0; i < 3; i++) {
      sr[i].wr.rdma.remote_addr += qp->remote_mr_.buf;
      sr[i].wr.rdma.rkey = qp->remote_mr_.key;
      sge[i].lkey = qp->local_mr_.key;
    }

    coro_sched->RDMABatch(coro_id, qp, &(sr[0]), &bad_sr, 2);
  }

 private:
  struct ibv_send_wr sr[3];

  struct ibv_sge sge[3];

  struct ibv_send_wr* bad_sr;
};

class UpdateBatch {
 public:
  UpdateBatch() {
    // value, delta, vcell, unlock
    sr[0].num_sge = 1;
    sr[0].sg_list = &sge[0];
    sr[0].send_flags = 0;
    sr[0].next = &sr[1];

    sr[1].num_sge = 1;
    sr[1].sg_list = &sge[1];
    sr[1].send_flags = 0;
    sr[1].next = &sr[2];

    sr[2].num_sge = 1;
    sr[2].sg_list = &sge[2];
    sr[2].send_flags = 0;
    sr[2].next = &sr[3];

    sr[3].num_sge = 1;
    sr[3].sg_list = &sge[3];
    sr[3].send_flags = IBV_SEND_SIGNALED;
    sr[3].next = NULL;
  }

  void SetValueReq(char* local_addr, uint64_t remote_off, size_t size) {
    sr[0].opcode = IBV_WR_RDMA_WRITE;
    sr[0].wr.rdma.remote_addr = remote_off;
    sge[0].addr = (uint64_t)local_addr;
    sge[0].length = size;
    if (size <= MAX_DOORBELL_LEN) {
      sr[0].send_flags |= IBV_SEND_INLINE;
    }
  }

  void SetDeltaReq(char* local_addr, uint64_t remote_off, size_t size) {
    sr[1].opcode = IBV_WR_RDMA_WRITE;
    sr[1].wr.rdma.remote_addr = remote_off;
    sge[1].addr = (uint64_t)local_addr;
    sge[1].length = size;
    if (size <= MAX_DOORBELL_LEN) {
      sr[1].send_flags |= IBV_SEND_INLINE;
    }
  }

  void SetVCellOrCVTReq(char* local_addr, uint64_t remote_off, size_t size) {
    sr[2].opcode = IBV_WR_RDMA_WRITE;
    sr[2].wr.rdma.remote_addr = remote_off;
    sge[2].addr = (uint64_t)local_addr;
    sge[2].length = size;
    if (size <= MAX_DOORBELL_LEN) {
      sr[2].send_flags |= IBV_SEND_INLINE;
    }
  }

  void UnlockReq(char* local_addr, uint64_t remote_off, size_t size) {
    sr[3].opcode = IBV_WR_RDMA_WRITE;
    sr[3].wr.rdma.remote_addr = remote_off;
    sge[3].addr = (uint64_t)local_addr;
    sge[3].length = size;
    sr[3].send_flags |= IBV_SEND_INLINE;
  }

  // Send doorbelled requests to the queue pair
  void SendReqs(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id) {
    for (int i = 0; i < 4; i++) {
      sr[i].wr.rdma.remote_addr += qp->remote_mr_.buf;
      sr[i].wr.rdma.rkey = qp->remote_mr_.key;
      sge[i].lkey = qp->local_mr_.key;
    }

    coro_sched->RDMABatch(coro_id, qp, &(sr[0]), &bad_sr, 3);
  }

 private:
  struct ibv_send_wr sr[4];

  struct ibv_sge sge[4];

  struct ibv_send_wr* bad_sr;
};

class UpdateBatchAttrAddr {
 public:
  UpdateBatchAttrAddr() {
    // value, delta, vcell, attr_addr, unlock
    sr[0].num_sge = 1;
    sr[0].sg_list = &sge[0];
    sr[0].send_flags = 0;
    sr[0].next = &sr[1];

    sr[1].num_sge = 1;
    sr[1].sg_list = &sge[1];
    sr[1].send_flags = 0;
    sr[1].next = &sr[2];

    sr[2].num_sge = 1;
    sr[2].sg_list = &sge[2];
    sr[2].send_flags = 0;
    sr[2].next = &sr[3];

    sr[3].num_sge = 1;
    sr[3].sg_list = &sge[3];
    sr[3].send_flags = 0;
    sr[3].next = &sr[4];

    sr[4].num_sge = 1;
    sr[4].sg_list = &sge[4];
    sr[4].send_flags = IBV_SEND_SIGNALED;
    sr[4].next = NULL;
  }

  void SetValueReq(char* local_addr, uint64_t remote_off, size_t size) {
    sr[0].opcode = IBV_WR_RDMA_WRITE;
    sr[0].wr.rdma.remote_addr = remote_off;
    sge[0].addr = (uint64_t)local_addr;
    sge[0].length = size;
    if (size <= MAX_DOORBELL_LEN) {
      sr[0].send_flags |= IBV_SEND_INLINE;
    }
  }

  void SetDeltaReq(char* local_addr, uint64_t remote_off, size_t size) {
    sr[1].opcode = IBV_WR_RDMA_WRITE;
    sr[1].wr.rdma.remote_addr = remote_off;
    sge[1].addr = (uint64_t)local_addr;
    sge[1].length = size;
    if (size <= MAX_DOORBELL_LEN) {
      sr[1].send_flags |= IBV_SEND_INLINE;
    }
  }

  void SetAttrAddrReq(char* local_addr, uint64_t remote_off, size_t size) {
    sr[2].opcode = IBV_WR_RDMA_WRITE;
    sr[2].wr.rdma.remote_addr = remote_off;
    sge[2].addr = (uint64_t)local_addr;
    sge[2].length = size;
    sr[2].send_flags |= IBV_SEND_INLINE;
  }

  void SetVCellReq(char* local_addr, uint64_t remote_off, size_t size) {
    sr[3].opcode = IBV_WR_RDMA_WRITE;
    sr[3].wr.rdma.remote_addr = remote_off;
    sge[3].addr = (uint64_t)local_addr;
    sge[3].length = size;
    sr[3].send_flags |= IBV_SEND_INLINE;
  }

  void UnlockReq(char* local_addr, uint64_t remote_off, size_t size) {
    sr[4].opcode = IBV_WR_RDMA_WRITE;
    sr[4].wr.rdma.remote_addr = remote_off;
    sge[4].addr = (uint64_t)local_addr;
    sge[4].length = size;
    sr[4].send_flags |= IBV_SEND_INLINE;
  }

  // Send doorbelled requests to the queue pair
  void SendReqs(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id) {
    for (int i = 0; i < 5; i++) {
      sr[i].wr.rdma.remote_addr += qp->remote_mr_.buf;
      sr[i].wr.rdma.rkey = qp->remote_mr_.key;
      sge[i].lkey = qp->local_mr_.key;
    }

    coro_sched->RDMABatch(coro_id, qp, &(sr[0]), &bad_sr, 4);
  }

 private:
  struct ibv_send_wr sr[5];

  struct ibv_sge sge[5];

  struct ibv_send_wr* bad_sr;
};

class InsertBatch {
 public:
  InsertBatch() {
    // value
    sr[0].num_sge = 1;
    sr[0].sg_list = &sge[0];
    sr[0].send_flags = 0;
    sr[0].next = &sr[1];

    // vcell
    sr[1].num_sge = 1;
    sr[1].sg_list = &sge[1];
    sr[1].send_flags = 0;
    sr[1].next = &sr[2];

    // header
    sr[2].num_sge = 1;
    sr[2].sg_list = &sge[2];
    sr[2].send_flags = IBV_SEND_SIGNALED;
    sr[2].next = NULL;
  }

  void SetValueReq(char* local_addr, uint64_t remote_off, size_t size) {
    sr[0].opcode = IBV_WR_RDMA_WRITE;
    sr[0].wr.rdma.remote_addr = remote_off;
    sge[0].addr = (uint64_t)local_addr;
    sge[0].length = size;
    if (size <= MAX_DOORBELL_LEN) {
      sr[0].send_flags |= IBV_SEND_INLINE;
    }
  }

  void SetVCellReq(char* local_addr, uint64_t remote_off, size_t size) {
    sr[1].opcode = IBV_WR_RDMA_WRITE;
    sr[1].wr.rdma.remote_addr = remote_off;
    sge[1].addr = (uint64_t)local_addr;
    sge[1].length = size;
    sr[1].send_flags |= IBV_SEND_INLINE;
  }

  void SetHeaderReq(char* local_addr, uint64_t remote_off, size_t size) {
    sr[2].opcode = IBV_WR_RDMA_WRITE;
    sr[2].wr.rdma.remote_addr = remote_off;
    sge[2].addr = (uint64_t)local_addr;
    sge[2].length = size;
    sr[2].send_flags |= IBV_SEND_INLINE;
  }

  // Send doorbelled requests to the queue pair
  void SendReqs(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id) {
    for (int i = 0; i < 3; i++) {
      sr[i].wr.rdma.remote_addr += qp->remote_mr_.buf;
      sr[i].wr.rdma.rkey = qp->remote_mr_.key;
      sge[i].lkey = qp->local_mr_.key;
    }

    coro_sched->RDMABatch(coro_id, qp, &(sr[0]), &bad_sr, 2);
  }

 private:
  struct ibv_send_wr sr[3];

  struct ibv_sge sge[3];

  struct ibv_send_wr* bad_sr;
};

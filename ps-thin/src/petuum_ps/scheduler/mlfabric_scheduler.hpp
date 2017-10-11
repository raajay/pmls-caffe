#pragma once

#include <pthread.h>
#include <petuum_ps/util/pthread_barrier.hpp>
#include <petuum_ps/scheduler/scheduler_recv_thread.hpp>

namespace petuum {
  class MLFabricScheduler {
  public:
    static void Init();
    static void ShutDown();
  private:
    static SchedulerRecvThread *scheduler_recv_thread_;
    static pthread_barrier_t init_barrier_;
  };
}

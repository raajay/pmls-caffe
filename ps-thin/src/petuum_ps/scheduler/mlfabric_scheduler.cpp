#include <petuum_ps/scheduler/mlfabric_scheduler.hpp>

namespace petuum {
    // declare the static variables again in this file
    SchedulerRecvThread *MLFabricScheduler::scheduler_recv_thread_;
    SchedulerSendThread *MLFabricScheduler::scheduler_send_thread_;
    pthread_barrier_t MLFabricScheduler::init_barrier_;

    void MLFabricScheduler::Init() {
      // we set up a barrier, that use to synchronize the current thread,
      // the scheduler recv and send thread.
      pthread_barrier_init(&init_barrier_, nullptr, 3);

      int32_t recv_id = GlobalContext::get_scheduler_recv_thread_id();
      scheduler_recv_thread_ = new SchedulerRecvThread(recv_id, &init_barrier_);
      scheduler_recv_thread_->Start();

      int32_t send_id = GlobalContext::get_scheduler_send_thread_id();
      scheduler_send_thread_ = new SchedulerSendThread(send_id, &init_barrier_);
      scheduler_send_thread_->Start();

      // wait until the comm bus has been setup for the spawned thread. Comm
      // bus setup, would have been triggered upon Start.
      pthread_barrier_wait(&init_barrier_);
    }

    void MLFabricScheduler::ShutDown() {
      scheduler_recv_thread_->ShutDown();
      scheduler_send_thread_->ShutDown();
      delete scheduler_recv_thread_;
    }
}

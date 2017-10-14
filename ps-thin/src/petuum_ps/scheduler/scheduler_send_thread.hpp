#pragma once

#include <cstdint>

#include <glog/logging.h>

#include <petuum_ps/util/pthread_barrier.hpp>
#include <petuum_ps/util/non_worker_thread.hpp>
#include <petuum_ps/util/mpmc_queue.hpp>
#include <petuum_ps/scheduler/scheduler.hpp>

namespace petuum {

    class SchedulerSendThread : public NonWorkerThread {
        public:
            SchedulerSendThread(int32_t my_id, pthread_barrier_t *init_barrier, Scheduler *scheduler)
                : NonWorkerThread(my_id,
                        init_barrier,
                        GlobalContext::get_num_total_bg_threads()), scheduler_(scheduler) {}

            ~SchedulerSendThread() = default;

            void ShutDown() {
                Join();
                VLOG(0) << "Scheduler Send Thread terminated";
            }

        protected:

            void InitSchedulerThread();
            virtual void *operator()();

        private:

            Scheduler* scheduler_;
    };

}

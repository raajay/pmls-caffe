#pragma once

#include <cstdint>

#include <glog/logging.h>

#include <petuum_ps/util/pthread_barrier.hpp>
#include <petuum_ps/util/non_worker_thread.hpp>


namespace petuum {

    class SchedulerRecvThread : public NonWorkerThread {
        public:
            SchedulerRecvThread(int32_t my_id, pthread_barrier_t *init_barrier)
                : NonWorkerThread(my_id, init_barrier, GlobalContext::get_num_total_bg_threads()) {}

            ~SchedulerRecvThread() = default;

            void ShutDown() { Join(); }

        protected:

            void InitSchedulerThread();
            virtual void *operator()();
    };

}

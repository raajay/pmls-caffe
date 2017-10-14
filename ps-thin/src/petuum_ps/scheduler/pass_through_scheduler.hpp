#pragma once

#include <chrono>
#include <thread>
#include <petuum_ps/scheduler/scheduler.hpp>
#include <petuum_ps/util/mpmc_queue.hpp>
#include <petuum_ps/util/OneDimCounter.hpp>

namespace petuum {

    class PassThroughScheduler : public Scheduler {
        public:
            PassThroughScheduler();
            ~PassThroughScheduler();
            void AddRequest(MLFabricRequest *request) override;
            MLFabricRequest* TakeRequest() override;

        protected:

            void UpdateOplogCounter(MLFabricRequest *request);

            MPMCQueue<MLFabricRequest*> *internal_queue_;
            // keeps track of number of oplogs applied to each server thread
            OneDimCounter<int32_t, int32_t> oplog_counter_;
    };
}

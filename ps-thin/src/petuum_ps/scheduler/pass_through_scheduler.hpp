#pragma once

#include <chrono>
#include <thread>
#include <petuum_ps/scheduler/scheduler.hpp>
#include <petuum_ps/util/mpmc_queue.hpp>

namespace petuum {

    class PassThroughScheduler : public Scheduler {
        public:
            PassThroughScheduler();
            ~PassThroughScheduler();
            void AddRequest(MLFabricRequest *request) override;
            MLFabricRequest* TakeRequest() override;

        protected:
            MPMCQueue<MLFabricRequest*> *internal_queue_;
    };
}

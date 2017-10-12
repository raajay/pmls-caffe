#pragma once

#include <petuum_ps/scheduler/scheduler.hpp>
#include <petuum_ps/util/mpmc_queue.hpp>

namespace petuum {

    class PassThroughScheduler : public Scheduler {
        public:
            PassThroughScheduler() = default;

            ~PassThroughScheduler() = default;

            void AddRequest() override {

            }

            void TakeRequest() override {

            }

        protected:
            MPMCQueue<int32_t> internal_queue_;
    };
}

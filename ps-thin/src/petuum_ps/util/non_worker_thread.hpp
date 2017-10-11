#pragma once

#include <petuum_ps/util/thread.hpp>

namespace petuum {

    class NonWorkerThread : public Thread {
        public:
            NonWorkerThread(int32_t id, pthread_barrier_t *barrier, int32_t num_bgs)
                : Thread(id, barrier),
                  bg_worker_ids_(num_bgs),
                  num_shutdown_bgs_(0) {}

        protected:

            bool HandleShutDownMsg();

            std::vector<int32_t> bg_worker_ids_;
            int32_t num_shutdown_bgs_;
    };

}

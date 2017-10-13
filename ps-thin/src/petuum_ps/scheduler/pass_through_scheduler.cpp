#include <petuum_ps/scheduler/pass_through_scheduler.hpp>

namespace petuum {

    PassThroughScheduler::PassThroughScheduler() {
        internal_queue_ = new MPMCQueue<MLFabricRequest*>(1000);
    }

    PassThroughScheduler::~PassThroughScheduler() {
        delete internal_queue_;
    }

    void PassThroughScheduler::AddRequest(MLFabricRequest *request) {
        if (request != nullptr) {
          request->OutputDestinationId = request->InputDestinationId;
          VLOG(20) << "Add : " << request->toString();
        }
        internal_queue_->Push(request);
    }

    MLFabricRequest* PassThroughScheduler::TakeRequest() {
        MLFabricRequest* value = nullptr;
        bool success = false;
        while (!success) {
          success = internal_queue_->Pop(&value);
          if (success) break;

          // see: https://stackoverflow.com/questions/4184468/sleep-for-milliseconds
          std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return value;
    }

}

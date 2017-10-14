#include <petuum_ps/scheduler/pass_through_scheduler.hpp>
#include <petuum_ps/thread/context.hpp>

namespace petuum {

    PassThroughScheduler::PassThroughScheduler() {
        internal_queue_ = new MPMCQueue<MLFabricRequest*>(1000);
    }

    PassThroughScheduler::~PassThroughScheduler() {
        delete internal_queue_;
    }

    /**
     */
    void PassThroughScheduler::AddRequest(MLFabricRequest *request) {/*{{{*/
        if (request != nullptr) {
          request->OutputDestinationId = request->InputDestinationId;
          VLOG(20) << "Add : " << request->toString();
        }
        internal_queue_->Push(request);
    }/*}}}*/

    /**
     */
    MLFabricRequest* PassThroughScheduler::TakeRequest() {
        MLFabricRequest* value = nullptr;
        bool success = false;
        while (!success) {
          success = internal_queue_->Pop(&value);
          if (success) break;
          // see: https://stackoverflow.com/questions/4184468/sleep-for-milliseconds
          std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        // XXX(raajay): this is a hack to keep track of the number of oplogs
        // that are forwarded to the server. It may so happen that the
        // scheduler send threads may not forward the requests to the source
        // thread.
        if (nullptr != value) {
            UpdateOplogCounter(value);
        }
        return value;
    }

    /**
     */
    void PassThroughScheduler::UpdateOplogCounter(MLFabricRequest *request) {
        int32_t dest_id = request->OutputDestinationId;
        if (!GlobalContext::is_server_thread(dest_id)) {
            return;
        }
        int32_t delay = oplog_counter_.Get(dest_id) - request->OplogVersion;
        VLOG(0) << "Delay : " << delay;
        oplog_counter_.Increment(dest_id, 1);
    }
}

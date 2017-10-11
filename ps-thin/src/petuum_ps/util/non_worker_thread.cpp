#include <petuum_ps/util/non_worker_thread.hpp>

namespace petuum {

    bool NonWorkerThread::HandleShutDownMsg() {
      ++num_shutdown_bgs_;
      if (num_shutdown_bgs_ == bg_worker_ids_.size()) {
        ServerShutDownAckMsg msg;
        SendToAll(&msg, bg_worker_ids_);
        return true;
      }
      return false;
    }

}

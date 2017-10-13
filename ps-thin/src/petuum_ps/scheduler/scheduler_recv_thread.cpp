#include <glog/logging.h>

#include <petuum_ps/scheduler/scheduler_recv_thread.hpp>

namespace petuum {

    /**
     * Waits until expected number of connections are received from bg workers.
     */
    void SchedulerRecvThread::InitSchedulerThread() {
        int32_t num_expected_conns = GlobalContext::get_num_total_bg_threads();
        int32_t nc;
        for(nc = 0; nc < num_expected_conns; ++nc) {
            int32_t sender_id = WaitForConnect();
            CHECK(GlobalContext::is_worker_thread(sender_id));
            bg_worker_ids_[nc] = sender_id;
        }

        // init some internal objects

        // send a start message to all the clients
        ClientStartMsg client_start_msg;
        SendToAll(&client_start_msg, bg_worker_ids_);
    }



    void *SchedulerRecvThread::operator()() {
        SetupCommBus(my_id_);
        pthread_barrier_wait(init_barrier_);
        InitSchedulerThread();
        VLOG(0) << "MLFabricScheduler accepting transfer requests!";

        zmq::message_t zmq_msg;
        MsgType msg_type;
        while(true) {
            int32_t sender_id = GetNextMessage(zmq_msg);
            msg_type = MsgBase::get_msg_type(zmq_msg.data());

            switch (msg_type) {

                case kClientShutDown:
                    if (HandleShutDownMsg()) {
                        comm_bus_->ThreadDeregister();
                        return nullptr;
                    }
                    break;

                default:
                  LOG(FATAL) << "Unrecognized message type " << msg_type
                             << " sender = " << sender_id;
            }
        }
        return nullptr;
    }

}

#include <petuum_ps/scheduler/scheduler_send_thread.hpp>

namespace petuum {

    /**
     * Waits until expected number of connections are received from bg workers.
     */
    void SchedulerSendThread::InitSchedulerThread() {
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


    void *SchedulerSendThread::operator()() {
        SetupCommBus(my_id_);
        pthread_barrier_wait(init_barrier_);
        InitSchedulerThread();
        VLOG(0) << "MLFabricScheduler sending thread started!";

        while(true) {
            // pull from a queue

            // terminate on getting message that asks to break
            break;
        }

        return nullptr;
    }


}

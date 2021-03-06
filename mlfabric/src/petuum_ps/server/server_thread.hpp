// author: jinliang
#pragma once

#include <vector>
#include <deque>
#include <stdint.h>
#include <pthread.h>
#include <petuum_ps/util/pthread_barrier.hpp>

#include <petuum_ps/server/server.hpp>
#include <petuum_ps/util/thread.hpp>
#include <petuum_ps/thread/context.hpp>
#include <petuum_ps/util/high_resolution_timer.hpp>

namespace petuum {
  class ServerThread : public Thread {
  public:
    ServerThread(int32_t my_id, pthread_barrier_t *init_barrier):
      my_id_(my_id),
      bg_worker_ids_(GlobalContext::get_num_worker_clients()),
      aggregator_ids_(GlobalContext::get_num_aggregator_clients()),
      num_registered_workers_(0),
      num_registered_aggregators_(0),
      num_registered_replicas_(0),
      num_shutdown_bgs_(0),
      comm_bus_(GlobalContext::comm_bus),
      init_barrier_(init_barrier) { }

    virtual ~ServerThread() { }

    void ShutDown() {
      Join();
    }

  protected:
    static bool WaitMsgBusy(int32_t *sender_id, zmq::message_t *zmq_msg,
                            long timeout_milli = -1);
    static bool WaitMsgSleep(int32_t *sender_id, zmq::message_t *zmq_msg,
                             long timeout_milli  = -1);
    static bool WaitMsgTimeOut(int32_t *sender_id, zmq::message_t *zmq_msg,
                               long timeout_milli);
    CommBus::WaitMsgTimeOutFunc WaitMsg_;

    virtual void InitWhenStart();

    virtual void SetWaitMsg();

    virtual void InitServer();
    virtual void ServerPushRow(bool clock_changed) { }
    virtual void RowSubscribe(ServerRow *server_row, int32_t client_id) { }

    void SetUpCommBus();
    void ConnectToEntity(int32_t entity_id);
    int32_t GetConnection();
    void SendToAllBgThreads(MsgBase *msg);
    void SendToAllAggregatorThreads(MsgBase *msg);


    bool HandleShutDownMsg();
    void HandleCreateTable(int32_t sender_id,
                           CreateTableMsg &create_table_msg);
    void HandleRowRequest(int32_t sender_id,
                          RowRequestMsg &row_request_msg);
    void HandleBulkRowRequest(int32_t bg_id,
                              BulkRowRequestMsg &bulk_request_msg);


    void ReplyRowRequest(int32_t bg_id,
                         ServerRow *server_row,
                         int32_t table_id,
                         int32_t row_id,
                         int32_t server_clock,
                         uint32_t version,
                         int32_t global_model_version);

    void HandleOpLogMsg(int32_t sender_id,
                        ClientSendOpLogMsg &client_send_oplog_msg);

    virtual long ServerIdleWork();
    virtual long ResetServerIdleMilli();

    virtual void SendOpLogAckMsg(int32_t bg_id, uint32_t version);

    virtual void *operator() ();

    int32_t my_id_;
    std::vector<int32_t> bg_worker_ids_;
    std::vector<int32_t> aggregator_ids_;
    int32_t num_registered_workers_;
    int32_t num_registered_aggregators_;
    int32_t num_registered_replicas_;
    Server server_obj_;
    int32_t num_shutdown_bgs_;
    CommBus* const comm_bus_;
    pthread_barrier_t *init_barrier_;
    std::deque<HighResolutionTimer*> replica_timers_;
  };

}

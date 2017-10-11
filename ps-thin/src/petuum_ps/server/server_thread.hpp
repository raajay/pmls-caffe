// author: jinliang
#pragma once

#include <vector>
#include <deque>
#include <stdint.h>
#include <pthread.h>
#include <petuum_ps/util/pthread_barrier.hpp>

#include <petuum_ps/server/server.hpp>
#include <petuum_ps/util/thread.hpp>
#include <petuum_ps/util/non_worker_thread.hpp>
#include <petuum_ps/thread/context.hpp>
#include <petuum_ps/util/high_resolution_timer.hpp>

namespace petuum {
class ServerThread : public NonWorkerThread {
public:
  ServerThread(int32_t my_id, pthread_barrier_t *init_barrier)
      : NonWorkerThread(my_id, init_barrier, GlobalContext::get_num_worker_clients()) {}

  virtual ~ServerThread() {}

  void ShutDown() { Join(); }

protected:
  static bool WaitMsgBusy(int32_t *sender_id, zmq::message_t *zmq_msg,
                          long timeout_milli = -1);
  static bool WaitMsgSleep(int32_t *sender_id, zmq::message_t *zmq_msg,
                           long timeout_milli = -1);
  static bool WaitMsgTimeOut(int32_t *sender_id, zmq::message_t *zmq_msg,
                             long timeout_milli);
  CommBus::WaitMsgTimeOutFunc WaitMsg_;

  virtual void InitWhenStart();

  virtual void SetWaitMsg();

  virtual void InitServer();

  void HandleCreateTable(int32_t sender_id, CreateTableMsg &create_table_msg);
  void HandleRowRequest(int32_t sender_id, RowRequestMsg &row_request_msg);

  void ReplyRowRequest(int32_t bg_id, ServerRow *server_row, int32_t table_id,
                       int32_t row_id, int32_t server_clock, uint32_t version,
                       unsigned long global_model_version);

  void HandleOpLogMsg(int32_t sender_id, ClientSendOpLogMsg &client_send_oplog_msg);

  virtual long ServerIdleWork();
  virtual long ResetServerIdleMilli();

  virtual void *operator()();


  Server server_obj_;
  std::deque<HighResolutionTimer *> replica_timers_;
};
}

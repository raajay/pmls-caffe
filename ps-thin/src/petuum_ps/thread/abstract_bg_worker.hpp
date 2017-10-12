// author: jinliang

#pragma once

#include <cstdint>
#include <map>
#include <vector>
#include <condition_variable>
#include <boost/unordered_map.hpp>

#include <petuum_ps/util/thread.hpp>
#include <petuum_ps/thread/row_request_oplog_mgr.hpp>
#include <petuum_ps/thread/bg_oplog.hpp>
#include <petuum_ps/thread/ps_msgs.hpp>
#include <petuum_ps/comm_bus/comm_bus.hpp>
#include <petuum_ps/include/configs.hpp>
#include <petuum_ps/client/client_table.hpp>
#include <petuum_ps/thread/append_only_row_oplog_buffer.hpp>
#include <petuum_ps/thread/row_oplog_serializer.hpp>
#include <petuum_ps/util/OneDimCounter.hpp>
#include <petuum_ps/util/TwoDimCounter.hpp>
#include <petuum_ps/util/OneDimStorage.hpp>
#include <petuum_ps/storage/oplog_storage.hpp>

namespace petuum {
class AbstractBgWorker : public Thread {
public:
  AbstractBgWorker(int32_t id, int32_t comm_channel_idx,
                   std::map<int32_t, ClientTable *> *tables,
                   pthread_barrier_t *init_barrier,
                   pthread_barrier_t *create_table_barrier);

  virtual ~AbstractBgWorker();
  void ShutDown();
  void AppThreadRegister();
  void AppThreadDeregister();
  void SyncThreadRegister();
  void SyncThreadDeregister();
  bool CreateTable(int32_t table_id, const ClientTableConfig &table_config);
  bool RequestRow(int32_t table_id, int32_t row_id, int32_t clock);
  void RequestRowAsync(int32_t table_id, int32_t row_id, int32_t clock, bool forced);
  void GetAsyncRowRequestReply();
  void ClockAllTables();
  void ClockTable(int32_t table_id);
  void SendOpLogsAllTables();
  double GetElapsedTime() { return from_start_timer_.elapsed(); }
  virtual void *operator()();

protected:
  static bool WaitMsgBusy(int32_t *sender_id, zmq::message_t *zmq_msg,
                          long timeout_milli = -1);
  static bool WaitMsgSleep(int32_t *sender_id, zmq::message_t *zmq_msg,
                           long timeout_milli = -1);
  static bool WaitMsgTimeOut(int32_t *sender_id, zmq::message_t *zmq_msg,
                             long timeout_milli);
  CommBus::WaitMsgTimeOutFunc WaitMsg_;
  virtual void SetWaitMsg();
  virtual long ResetBgIdleMilli();

  void BgServerHandshake();
  void RecvAppInitThreadConnection(int32_t *num_connected_app_threads);

  virtual void CreateRowRequestOpLogMgr() = 0;

  virtual void InitWhenStart();
  void PrepareBeforeInfiniteLoop();

  void HandleCreateTables();
  virtual long HandleClockMsg(int32_t table_id, bool clock_advanced);

  virtual long BgIdleWork();

  virtual BgOpLog *PrepareOpLogs(int32_t table_id) = 0;
  void FinalizeTableOplogSize(int32_t table_id);
  void CreateOpLogMsgs(int32_t table_id, const BgOpLog *bg_oplog);
  void SendOpLogTransferRequests();
  size_t SendOpLogMsgs(int32_t table_id, bool clock_advanced);

  void HandleServerRowRequestReply(
      int32_t server_id,
      ServerRowRequestReplyMsg &server_row_request_reply_msg);
  void CheckForwardRowRequestToServer(int32_t app_thread_id,
                                      RowRequestMsg &row_request_msg);


  size_t SendMsg(MsgBase *msg);
  void RecvMsg(zmq::message_t &zmq_msg);

  virtual ClientRow *CreateClientRow(int32_t clock, int32_t global_version,
                                     AbstractRow *row_data) = 0;

  void InsertUpdateRow(const int32_t table_id, const int32_t row_id, const void
          *data, const size_t row_update_size, int32_t new_clock, int32_t
          global_row_version);


  void IncrementUpdateVersion(int32_t table_id);

  uint32_t GetUpdateVersion(int32_t table_id);

  void SendRowRequestToServer(int32_t table_id, int32_t row_id, int32_t clock);

  void SendRowRequestToServer(RowRequestMsg &msg);

  void SendRowRequestReplyToApp(int32_t app_id, int32_t table_id, int32_t row_id, int32_t clock);


  int32_t my_comm_channel_idx_;

  std::map<int32_t, ClientTable *> *tables_;
  std::vector<int32_t> server_ids_;

  OneDimCounter<int32_t, uint32_t> table_update_version_;

  // TODO(raajay) make the clock per table
  int32_t worker_clock_;
  int32_t clock_has_pushed_;

  // stores un-sent row requests
  RowRequestOpLogMgr *row_request_oplog_mgr_;
  OplogStorage *oplog_storage_;

  pthread_barrier_t *create_table_barrier_;

  OneDimCounter<int32_t, size_t> ephemeral_server_byte_counter_;
  TwoDimCounter<int32_t, int32_t, size_t> ephemeral_server_table_size_counter_;

  HighResolutionTimer from_start_timer_;
};
}

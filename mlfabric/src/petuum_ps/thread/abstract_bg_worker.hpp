// author: jinliang

#pragma once

#include <stdint.h>
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

namespace petuum {
  class AbstractBgWorker : public Thread {
  public:
    AbstractBgWorker(int32_t id, int32_t comm_channel_idx,
                     std::map<int32_t, ClientTable* > *tables,
                     pthread_barrier_t *init_barrier,
                     pthread_barrier_t *create_table_barrier);
    virtual ~AbstractBgWorker();

    void ShutDown();

    void AppThreadRegister();
    void AppThreadDeregister();

    bool CreateTable(int32_t table_id,
                     const ClientTableConfig& table_config);

    bool RequestRow(int32_t table_id, int32_t row_id, int32_t clock);
    void RequestRowAsync(int32_t table_id, int32_t row_id, int32_t clock,
                         bool forced);
    void GetAsyncRowRequestReply();

    void ClockAllTables();
    void SendOpLogsAllTables();

    double GetElapsedTime() {
      return from_start_timer_.elapsed();
    }

    virtual void *operator() ();

  protected:
    virtual void InitWhenStart();

    virtual void SetWaitMsg();
    virtual void CreateRowRequestOpLogMgr() = 0;

    static bool WaitMsgBusy(int32_t *sender_id, zmq::message_t *zmq_msg,
                            long timeout_milli = -1);
    static bool WaitMsgSleep(int32_t *sender_id, zmq::message_t *zmq_msg,
                             long timeout_milli  = -1);
    static bool WaitMsgTimeOut(int32_t *sender_id,
                               zmq::message_t *zmq_msg,
                               long timeout_milli);

    CommBus::WaitMsgTimeOutFunc WaitMsg_;

    typedef size_t (*GetSerializedRowOpLogSizeFunc)(AbstractRowOpLog *row_oplog);
    static size_t GetDenseSerializedRowOpLogSize(AbstractRowOpLog *row_oplog);
    static size_t GetSparseSerializedRowOpLogSize(AbstractRowOpLog *row_oplog);

    /* Functions Called From Main Loop -- BEGIN */
    void InitCommBus();
    void BgHandshake();
    void HandleCreateTables();

    // get connection from init thread
    void RecvAppInitThreadConnection(int32_t *num_connected_app_threads);

    virtual void PrepareBeforeInfiniteLoop();
    // invoked after all tables have been created
    virtual void FinalizeTableStats();
    virtual long ResetBgIdleMilli();
    virtual long BgIdleWork();

    /* Functions Called From Main Loop -- END */


    /* Handles Sending OpLogs -- BEGIN */
    virtual long HandleClockMsg(bool clock_advanced);

    virtual BgOpLog *PrepareOpLogsToSend() = 0;
    void CreateOpLogMsgs(const BgOpLog *bg_oplog);
    size_t SendOpLogMsgs(bool clock_advanced) ;

    size_t CountRowOpLogToSend(
                               int32_t row_id, AbstractRowOpLog *row_oplog,
                               std::map<int32_t, size_t> *table_num_bytes_by_server,
                               BgOpLogPartition *bg_table_oplog,
                               GetSerializedRowOpLogSizeFunc GetSerializedRowOpLogSize);

    virtual void TrackBgOpLog(BgOpLog *bg_oplog) = 0;

    void FinalizeOpLogMsgStats(
                               int32_t table_id,
                               std::map<int32_t, size_t> *table_num_bytes_by_server,
                               std::map<int32_t, std::map<int32_t, size_t> >
                               *server_table_oplog_size_map);
    /* Handles Sending OpLogs -- END */

    /* Handles Row Requests -- BEGIN */
    void CheckForwardRowRequestToServer(int32_t app_thread_id,
                                        RowRequestMsg &row_request_msg);
    void HandleServerRowRequestReply(
                                     int32_t server_id,
                                     ServerRowRequestReplyMsg &server_row_request_reply_msg);
    /* Handles Row Requests -- END */


    /* Helper Functions */
    size_t SendMsg(MsgBase *msg);
    void RecvMsg(zmq::message_t &zmq_msg);
    void ConnectToEntity(int32_t entity_id);
    void ReceiveFromEntity(int32_t entity_id);

    virtual ClientRow *CreateClientRow(int32_t clock, int32_t global_version, AbstractRow *row_data) = 0;

    virtual void UpdateExistingRow(int32_t table_id, int32_t row_id,
                                   ClientRow *clien_row, ClientTable *client_table,
                                   const void *data, size_t row_size, uint32_t version);

    virtual void InsertNonexistentRow(int32_t table_id,
                                      int32_t row_id, ClientTable *client_table, const void *data,
                                      size_t row_size, uint32_t version, int32_t clock, int32_t global_model_version);

    int32_t my_id_;
    int32_t my_comm_channel_idx_;
    std::map<int32_t, ClientTable* > *tables_;
    std::vector<int32_t> server_ids_;
    std::vector<int32_t> aggregator_ids_;

    uint32_t version_;
    int32_t client_clock_;
    int32_t clock_has_pushed_;
    RowRequestOpLogMgr *row_request_oplog_mgr_;
    CommBus* const comm_bus_;

    pthread_barrier_t *init_barrier_;
    pthread_barrier_t *create_table_barrier_;

    // initialized at Creation time, used in CreateSendOpLogs()
    // For server x, table y, the size of serialized OpLog is ...
    std::map<int32_t, std::map<int32_t, size_t> > server_table_oplog_size_map_;
    // The OpLog msg to each server
    std::map<int32_t, ClientSendOpLogMsg* > server_oplog_msg_map_;
    // size of oplog per table, reused across multiple tables
    std::map<int32_t, size_t> table_num_bytes_by_server_;

    std::unordered_map<int32_t, AppendOnlyRowOpLogBuffer*> append_only_row_oplog_buffer_map_;
    std::unordered_map<int32_t, int32_t> append_only_buff_proc_count_;

    std::unordered_map<int32_t, RowOpLogSerializer*> row_oplog_serializer_map_;
    HighResolutionTimer  from_start_timer_;

    std::unordered_map<int32_t, ClientSendOpLogMsg*> backlog_msgs_;
    int32_t model_version_prepared_;
    int32_t current_unique_id_;
  };

} // end namespace -- petuum

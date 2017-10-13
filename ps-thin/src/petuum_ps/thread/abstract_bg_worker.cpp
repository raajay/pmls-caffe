#include <petuum_ps/thread/abstract_bg_worker.hpp>

#include <petuum_ps/util/class_register.hpp>
#include <petuum_ps/client/oplog_serializer.hpp>
#include <petuum_ps/client/ssp_client_row.hpp>
#include <petuum_ps/thread/mem_transfer.hpp>
#include <petuum_ps/util/utils.hpp>

#include <climits>
#include <utility>

namespace petuum {

AbstractBgWorker::AbstractBgWorker(int32_t id, int32_t comm_channel_idx,
                                   std::map<int32_t, ClientTable *> *tables,
                                   pthread_barrier_t *init_barrier,
                                   pthread_barrier_t *create_table_barrier)
    : Thread(id, init_barrier),
      my_comm_channel_idx_(comm_channel_idx),
      tables_(tables),
      worker_clock_(0), clock_has_pushed_(-1),
      create_table_barrier_(create_table_barrier) {
          oplog_storage_ = new OplogStorage(id);
  GlobalContext::GetServerThreadIDs(my_comm_channel_idx_, &(server_ids_));
}

/**
 */
AbstractBgWorker::~AbstractBgWorker() {/*{{{*/
    delete oplog_storage_;
}/*}}}*/

void AbstractBgWorker::ShutDown() { Join(); }

/**
 */
void AbstractBgWorker::AppThreadRegister() {/*{{{*/
  AppConnectMsg app_connect_msg;
  void *msg = app_connect_msg.get_mem();
  size_t msg_size = app_connect_msg.get_size();

  comm_bus_->ConnectTo(my_id_, msg, msg_size);
}/*}}}*/

/**
 */
void AbstractBgWorker::AppThreadDeregister() {/*{{{*/
  AppThreadDeregMsg msg;
  size_t sent_size = SendMsg(reinterpret_cast<MsgBase *>(&msg));
  CHECK_EQ(sent_size, msg.get_size());
}/*}}}*/

/**
 */
void AbstractBgWorker::SyncThreadRegister() {/*{{{*/
  SyncThreadConnectMsg connect_msg;
  void *msg = connect_msg.get_mem();
  size_t msg_size = connect_msg.get_size();
  comm_bus_->ConnectTo(my_id_, msg, msg_size);
}/*}}}*/

/**
 */
void AbstractBgWorker::SyncThreadDeregister() {/*{{{*/
  SyncThreadDeregMsg msg;
  size_t sent_size = SendMsg(reinterpret_cast<MsgBase *>(&msg));
  CHECK_EQ(sent_size, msg.get_size());
}/*}}}*/

/**
 */
bool AbstractBgWorker::CreateTable(int32_t table_id,/*{{{*/
                                   const ClientTableConfig &table_config) {
  {
    const TableInfo &table_info = table_config.table_info;
    BgCreateTableMsg bg_create_table_msg;
    bg_create_table_msg.get_table_id() = table_id;
    bg_create_table_msg.get_staleness() = table_info.table_staleness;
    bg_create_table_msg.get_row_type() = table_info.row_type;
    bg_create_table_msg.get_row_capacity() = table_info.row_capacity;
    bg_create_table_msg.get_process_cache_capacity() =
        table_config.process_cache_capacity;
    bg_create_table_msg.get_thread_cache_capacity() =
        table_config.thread_cache_capacity;
    bg_create_table_msg.get_oplog_capacity() = table_config.oplog_capacity;

    bg_create_table_msg.get_oplog_dense_serialized() =
        table_info.oplog_dense_serialized;
    bg_create_table_msg.get_row_oplog_type() = table_info.row_oplog_type;
    bg_create_table_msg.get_dense_row_oplog_capacity() =
        table_info.dense_row_oplog_capacity;

    bg_create_table_msg.get_oplog_type() = table_config.oplog_type;
    bg_create_table_msg.get_append_only_oplog_type() =
        table_config.append_only_oplog_type;
    bg_create_table_msg.get_append_only_buff_capacity() =
        table_config.append_only_buff_capacity;
    bg_create_table_msg.get_per_thread_append_only_buff_pool_size() =
        table_config.per_thread_append_only_buff_pool_size;
    bg_create_table_msg.get_bg_apply_append_oplog_freq() =
        table_config.bg_apply_append_oplog_freq;
    bg_create_table_msg.get_process_storage_type() =
        table_config.process_storage_type;
    bg_create_table_msg.get_no_oplog_replay() = table_config.no_oplog_replay;

    size_t sent_size =
        SendMsg(reinterpret_cast<MsgBase *>(&bg_create_table_msg));
    CHECK_EQ((int32_t)sent_size, bg_create_table_msg.get_size());
    VLOG(5) << "THREAD-" << my_id_
            << ": Send CREATE_TABLE request from bgworker=" << this->my_id_
            << " for table=" << table_id;
  }
  // waiting for response
  {
    zmq::message_t zmq_msg;
    int32_t sender_id;
    comm_bus_->RecvInProc(&sender_id, &zmq_msg);
    MsgType msg_type = MsgBase::get_msg_type(zmq_msg.data());
    CHECK_EQ(msg_type, kCreateTableReply);
    VLOG(5) << "THREAD-" << my_id_
            << ": Received reply for CREATE_TABLE from sender=" << sender_id;
  }
  return true;
}/*}}}*/

/**
 */
bool AbstractBgWorker::RequestRow(int32_t table_id, int32_t row_id,/*{{{*/
                                  int32_t req_clock) {
  petuum::HighResolutionTimer rr_send;
  {
    RowRequestMsg request_row_msg;
    request_row_msg.get_table_id() = table_id;
    request_row_msg.get_row_id() = row_id;
    request_row_msg.get_clock() = req_clock;
    request_row_msg.get_forced_request() = false;
    VLOG(20) << "RR App Thread >>> Bg Thread (" << my_id_ << ") "
             << petuum::GetTableRowStringId(table_id, row_id)
             << " clock=" << req_clock;
    size_t sent_size = SendMsg(reinterpret_cast<MsgBase *>(&request_row_msg));
    CHECK_EQ(sent_size, request_row_msg.get_size());
  }

  {
    zmq::message_t zmq_msg;
    int32_t sender_id;
    comm_bus_->RecvInProc(&sender_id, &zmq_msg);
    VLOG(20) << "RR Latency@App Thread for "
             << petuum::GetTableRowStringId(table_id, row_id)
             << " clock=" << req_clock << " equals " << rr_send.elapsed() << " s";
    MsgType msg_type = MsgBase::get_msg_type(zmq_msg.data());
    CHECK_EQ(msg_type, kRowRequestReply);
  }
  return true;
}/*}}}*/

/**
 */
void AbstractBgWorker::RequestRowAsync(int32_t table_id, int32_t row_id,/*{{{*/
                                       int32_t clock, bool forced) {
  RowRequestMsg request_row_msg;
  request_row_msg.get_table_id() = table_id;
  request_row_msg.get_row_id() = row_id;
  request_row_msg.get_clock() = clock;
  request_row_msg.get_forced_request() = forced;

  VLOG(20) << "RR-Async App Thread >>> Bg Thread "
           << petuum::GetTableRowStringId(table_id, row_id)
           << " clock=" << clock;

  size_t sent_size = SendMsg(reinterpret_cast<MsgBase *>(&request_row_msg));
  CHECK_EQ(sent_size, request_row_msg.get_size());
}/*}}}*/

/**
 */
void AbstractBgWorker::GetAsyncRowRequestReply() {/*{{{*/
  zmq::message_t zmq_msg;
  int32_t sender_id;
  comm_bus_->RecvInProc(&sender_id, &zmq_msg);
  MsgType msg_type = MsgBase::get_msg_type(zmq_msg.data());
  CHECK_EQ(msg_type, kRowRequestReply);
}/*}}}*/

/**
 */
void AbstractBgWorker::ClockAllTables() {/*{{{*/
  BgClockMsg bg_clock_msg;
  size_t sent_size = SendMsg(reinterpret_cast<MsgBase *>(&bg_clock_msg));
  CHECK_EQ(sent_size, bg_clock_msg.get_size());
}/*}}}*/

/**
 */
void AbstractBgWorker::ClockTable(int32_t table_id) {/*{{{*/
  BgClockMsg msg;
  msg.get_table_id() = table_id;
  VLOG(20) << "Sync Thread >>> Bg Worker: Clock table_id=" << table_id;
  size_t sent_size = SendMsg(reinterpret_cast<MsgBase *>(&msg));
  CHECK_EQ(sent_size, msg.get_size());
}/*}}}*/

/**
 */
void AbstractBgWorker::SendOpLogsAllTables() {/*{{{*/
  BgSendOpLogMsg bg_send_oplog_msg;
  size_t sent_size = SendMsg(reinterpret_cast<MsgBase *>(&bg_send_oplog_msg));
  CHECK_EQ(sent_size, bg_send_oplog_msg.get_size());
}/*}}}*/

/**
 */
void AbstractBgWorker::InitWhenStart() {/*{{{*/
  SetWaitMsg();
  CreateRowRequestOpLogMgr();
}/*}}}*/

/**
 */
bool AbstractBgWorker::WaitMsgBusy(int32_t *sender_id, zmq::message_t *zmq_msg,/*{{{*/
                                   long timeout_milli __attribute__((unused))) {
  bool received =
      (GlobalContext::comm_bus->*(GlobalContext::comm_bus->RecvAsyncAny_))(
          sender_id, zmq_msg);
  while (!received)
    received =
        (GlobalContext::comm_bus->*(GlobalContext::comm_bus->RecvAsyncAny_))(
            sender_id, zmq_msg);
  return true;
}/*}}}*/

/**
 */
bool AbstractBgWorker::WaitMsgSleep(int32_t *sender_id, zmq::message_t *zmq_msg,/*{{{*/
                                    long timeout_milli
                                    __attribute__((unused))) {
  (GlobalContext::comm_bus->*(GlobalContext::comm_bus->RecvAny_))(sender_id,
                                                                  zmq_msg);
  return true;
}/*}}}*/

/**
 */
bool AbstractBgWorker::WaitMsgTimeOut(int32_t *sender_id,/*{{{*/
                                      zmq::message_t *zmq_msg,
                                      long timeout_milli) {
  bool received =
      (GlobalContext::comm_bus->*(GlobalContext::comm_bus->RecvTimeOutAny_))(
          sender_id, zmq_msg, timeout_milli);
  return received;
}/*}}}*/

/**
 */
void AbstractBgWorker::SetWaitMsg() {/*{{{*/
  if (GlobalContext::get_aggressive_cpu()) {
    WaitMsg_ = WaitMsgBusy;
  } else {
    WaitMsg_ = WaitMsgSleep;
  }
}/*}}}*/

/**
 */
void AbstractBgWorker::BgServerHandshake() {/*{{{*/
  // connect to name node
  int32_t name_node_id = GlobalContext::get_name_node_id();
  ConnectTo(name_node_id, my_id_);
  WaitForReply(name_node_id, kConnectServer);
  WaitForReply(name_node_id, kClientStart);

  if (GlobalContext::use_mlfabric()) {
    // connect to scheduler (bot recv and transmit threads)
    ConnectTo(GlobalContext::get_scheduler_recv_thread_id(), my_id_);
    WaitForReply(GlobalContext::get_scheduler_recv_thread_id(), kClientStart);

    ConnectTo(GlobalContext::get_scheduler_send_thread_id(), my_id_);
    WaitForReply(GlobalContext::get_scheduler_send_thread_id(), kClientStart);
  }

  // connect to servers
  for (const auto &server_id : server_ids_) {
    ConnectTo(server_id, my_id_);
  }

  // get start messages from servers
  int32_t nss = 0;
  for (nss = 0; nss < server_ids_.size(); ++nss) {
    WaitForReply(GlobalContext::kAnyThreadId, kClientStart);
  }
}/*}}}*/

/**
 */
void AbstractBgWorker::HandleCreateTables() {/*{{{*/
  for (int32_t num_created_tables = 0;
       num_created_tables < GlobalContext::get_num_tables();
       ++num_created_tables) {

    int32_t table_id;
    int32_t sender_id;
    ClientTableConfig client_table_config;

    {
      zmq::message_t zmq_msg;
      comm_bus_->RecvInProc(&sender_id, &zmq_msg);
      MsgType msg_type = MsgBase::get_msg_type(zmq_msg.data());
      CHECK_EQ(msg_type, kBgCreateTable);
      BgCreateTableMsg bg_create_table_msg(zmq_msg.data());
      // set up client table config
      client_table_config.table_info.table_staleness =
          bg_create_table_msg.get_staleness();
      client_table_config.table_info.row_type =
          bg_create_table_msg.get_row_type();
      client_table_config.table_info.row_capacity =
          bg_create_table_msg.get_row_capacity();
      client_table_config.process_cache_capacity =
          bg_create_table_msg.get_process_cache_capacity();
      client_table_config.thread_cache_capacity =
          bg_create_table_msg.get_thread_cache_capacity();
      client_table_config.oplog_capacity =
          bg_create_table_msg.get_oplog_capacity();

      client_table_config.table_info.oplog_dense_serialized =
          bg_create_table_msg.get_oplog_dense_serialized();
      client_table_config.table_info.row_oplog_type =
          bg_create_table_msg.get_row_oplog_type();
      client_table_config.table_info.dense_row_oplog_capacity =
          bg_create_table_msg.get_dense_row_oplog_capacity();

      client_table_config.oplog_type = bg_create_table_msg.get_oplog_type();
      client_table_config.append_only_oplog_type =
          bg_create_table_msg.get_append_only_oplog_type();
      client_table_config.append_only_buff_capacity =
          bg_create_table_msg.get_append_only_buff_capacity();
      client_table_config.per_thread_append_only_buff_pool_size =
          bg_create_table_msg.get_per_thread_append_only_buff_pool_size();
      client_table_config.bg_apply_append_oplog_freq =
          bg_create_table_msg.get_bg_apply_append_oplog_freq();
      client_table_config.process_storage_type =
          bg_create_table_msg.get_process_storage_type();
      client_table_config.no_oplog_replay =
          bg_create_table_msg.get_no_oplog_replay();

      CreateTableMsg create_table_msg;
      create_table_msg.get_table_id() = bg_create_table_msg.get_table_id();
      create_table_msg.get_staleness() = bg_create_table_msg.get_staleness();
      create_table_msg.get_row_type() = bg_create_table_msg.get_row_type();
      create_table_msg.get_row_capacity() =
          bg_create_table_msg.get_row_capacity();
      create_table_msg.get_oplog_dense_serialized() =
          bg_create_table_msg.get_oplog_dense_serialized();
      create_table_msg.get_row_oplog_type() =
          bg_create_table_msg.get_row_oplog_type();
      create_table_msg.get_dense_row_oplog_capacity() =
          bg_create_table_msg.get_dense_row_oplog_capacity();

      table_id = create_table_msg.get_table_id();

      // send msg to name node
      int32_t name_node_id = GlobalContext::get_name_node_id();
      size_t sent_size = (comm_bus_->*(comm_bus_->SendAny_))(
          name_node_id, create_table_msg.get_mem(),
          create_table_msg.get_size());
      CHECK_EQ(sent_size, create_table_msg.get_size());
      VLOG(5) << "THREAD-" << my_id_
              << ": Send CREATE_TABLE request from bgworker=" << my_id_
              << " to namenode=" << name_node_id;
    }

    // wait for response from name node
    {
      zmq::message_t zmq_msg;
      int32_t name_node_id;
      (comm_bus_->*(comm_bus_->RecvAny_))(&name_node_id, &zmq_msg);
      MsgType msg_type = MsgBase::get_msg_type(zmq_msg.data());

      CHECK_EQ(msg_type, kCreateTableReply);
      CreateTableReplyMsg create_table_reply_msg(zmq_msg.data());
      CHECK_EQ(create_table_reply_msg.get_table_id(), table_id);

      ClientTable *client_table;
      try {
        VLOG(5) << "THREAD-" << my_id_
                << ": Creating an instance of a ClientTable(id=" << table_id
                << ", address:" << &client_table << ") in bgworker=" << my_id_;
        client_table = new ClientTable(table_id, client_table_config);
      } catch (std::bad_alloc &e) {
        LOG(FATAL) << "Bad alloc exception";
      }
      // not thread-safe
      (*tables_)[table_id] = client_table;

      size_t sent_size =
          comm_bus_->SendInProc(sender_id, zmq_msg.data(), zmq_msg.size());
      CHECK_EQ(sent_size, zmq_msg.size());
    }
  }

  {
    zmq::message_t zmq_msg;
    int32_t sender_id;
    (comm_bus_->*(comm_bus_->RecvAny_))(&sender_id, &zmq_msg);
    MsgType msg_type = MsgBase::get_msg_type(zmq_msg.data());
    CHECK_EQ(msg_type, kCreatedAllTables);
    VLOG(5) << "THREAD-" << my_id_
            << ": Received a kCreatedAllTables message from sender="
            << sender_id;
  }
}/*}}}*/

/**
 */
long AbstractBgWorker::HandleClockMsg(int32_t table_id, bool clock_advanced) {/*{{{*/
  // preparation, partitions the oplog based on destination. The current
  // bg_thread only deals with row_ids that it is responsible for. After
  // preparation, we will know exactly how many bytes are being sent to each
  // server. We will also know how the data being sent to the server is split
  // across tables.
  BgOpLog *bg_oplog = PrepareOpLogs(table_id);

  clock_has_pushed_ = worker_clock_;

  // here the oplogs is actually created; serialization happens
  STATS_BG_ACCUM_CLOCK_END_OPLOG_SERIALIZE_BEGIN();
  std::vector<int32_t> oplog_ids = CreateAndStoreOpLogMsgs(table_id, clock_advanced, bg_oplog);
  STATS_BG_ACCUM_CLOCK_END_OPLOG_SERIALIZE_END();
  CHECK_EQ(oplog_ids.size(), server_ids_.size());

  if (false && GlobalContext::use_mlfabric()) {
    SendOpLogTransferRequests(oplog_ids);
  } else {
    CHECK_EQ(oplog_storage_->GetNumOplogs(), server_ids_.size());
    SendOpLogMsgs(oplog_ids);
  }
  delete bg_oplog;
  IncrementUpdateVersion(table_id);
  return 0;
  // the clock (worker_clock_) is immediately incremented after this function
  // completes
}/*}}}*/


long AbstractBgWorker::ResetBgIdleMilli() { return 0; }

long AbstractBgWorker::BgIdleWork() { return 0; }

/**
 */
void AbstractBgWorker::FinalizeTableOplogSize(int32_t table_id) {/*{{{*/
  for (auto server_id : ephemeral_server_byte_counter_.GetKeysPosValue()) {
    // add the size used to represent the number of rows in an update to stats
    ephemeral_server_byte_counter_.Increment(server_id, sizeof(int32_t));
    ephemeral_server_table_size_counter_.Increment(server_id, table_id,
            ephemeral_server_byte_counter_.Get(server_id));
  }
}/*}}}*/

/**
 * @brief Create the oplog msgs, write the oplop values to msgs and store them
 */
std::vector<int32_t> AbstractBgWorker::CreateAndStoreOpLogMsgs(int32_t table_id, bool clock_advanced, const BgOpLog *bg_oplog) {/*{{{*/

    std::vector<int32_t> oplog_ids;

  std::map<int32_t, std::map<int32_t, void *>> table_server_mem_map;
  size_t bytes_written = 0;
  size_t bytes_allocated = 0;

  for (auto server_id : server_ids_) {

    ServerOpLogSerializer oplog_serializer;
    size_t msg_size = oplog_serializer.Init(server_id, ephemeral_server_table_size_counter_);

    ClientSendOpLogMsg *msg = new ClientSendOpLogMsg(msg_size);
    int32_t oplog_id = oplog_storage_->Add(server_id, msg);
    oplog_storage_->SetTableId(oplog_id, table_id);
    oplog_storage_->SetClockAdvanced(oplog_id, clock_advanced);
    oplog_storage_->SetBgVersion(oplog_id, GetUpdateVersion(table_id));
    oplog_storage_->SetClock(oplog_id, clock_has_pushed_ + 1);

    // TODO(raajay)  add proper value for model version
    oplog_storage_->SetModelVersion(oplog_id, 0);

    oplog_ids.push_back(oplog_id);

    if (msg_size == 0) { continue; }

    bytes_allocated += msg_size;
    oplog_serializer.AssignMem(msg->get_data(), bytes_written);

    for (const auto &table_pair : (*tables_)) {
      int32_t curr_table_id = table_pair.first;
      uint8_t *table_ptr = reinterpret_cast<uint8_t *>
          (oplog_serializer.GetTablePtr(curr_table_id));

      if (table_ptr == nullptr) {
        table_server_mem_map[curr_table_id].erase(server_id);
        continue;
      }

      // write table id
      *(reinterpret_cast<int32_t *>(table_ptr)) = curr_table_id;
      bytes_written += sizeof(int32_t);

      // write table update size
      *(reinterpret_cast<size_t *>(table_ptr + sizeof(int32_t))) =
          table_pair.second->get_sample_row()->get_update_size();
      bytes_written += sizeof(size_t);

      // offset for table rows -- store the offset for each table and each
      // server. This is the offset into oplog msg's memory.
      table_server_mem_map[curr_table_id][server_id] =
          table_ptr + sizeof(int32_t) + sizeof(size_t);
    }
  }

  // here the re-arranging of the different dimensions happen. We use the
  // bg_oplog data structure that partitions oplog into tables and server to
  // write into locations pointed in the table_server_mem_map.

  for (const auto &table_pair : (*tables_)) {
    int32_t curr_table_id = table_pair.first;
    if (table_id != ALL_TABLES && table_id != curr_table_id) {
        continue;
    }
    BgOpLogPartition *oplog_partition = bg_oplog->Get(curr_table_id);
    // the second argument to function is an indicator to notify is the
    // serialization is dense or sparse
    bytes_written += oplog_partition->SerializeByServer(
        &(table_server_mem_map[curr_table_id]),
        table_pair.second->oplog_dense_serialized());
  }

  VLOG(20) << "Total bytes allocated=" << bytes_allocated;
  VLOG(20) << "Total bytes_written=" << bytes_written;
  CHECK_EQ(bytes_written, bytes_allocated);

  return oplog_ids;
}/*}}}*/

/**
 * @brief Send request msgs to the MLFabric scheduler for getting permissions
 * to transfer the oplogs.
 */
void AbstractBgWorker::SendOpLogTransferRequests(std::vector<int32_t> oplog_ids) {/*{{{*/
    for(auto oplog_id : oplog_ids) {
        if (0 == oplog_storage_->GetSize(oplog_id)) {
          ClientSendOpLogMsg *msg = oplog_storage_->GetOplogMsg(oplog_id);
          MemTransfer::TransferMem(comm_bus_, oplog_storage_->GetServerId(oplog_id), msg);
          oplog_storage_->EraseOplog(oplog_id);
          continue;
        }
        auto pRequestMsg = oplog_storage_->GetOplogRequestMsg(oplog_id);
        Send(pRequestMsg, GlobalContext::get_scheduler_recv_thread_id());
        delete pRequestMsg;
    }
}/*}}}*/

/**
 * @brief Send the oplogs to the destination. The oplogs are accessed from the
 * storage using the oplog_ids.
 */
size_t AbstractBgWorker::SendOpLogMsgs(std::vector<int32_t> oplog_ids) {/*{{{*/
  // TODO(raajay) table_id, and clock_advanced were passed as arguments
  size_t accum_size = 0;
  for (auto oplog_id : oplog_ids) {
      ClientSendOpLogMsg *msg = oplog_storage_->GetOplogMsg(oplog_id);
      accum_size += msg->get_size();
      MemTransfer::TransferMem(comm_bus_, oplog_storage_->GetServerId(oplog_id), msg);
      oplog_storage_->EraseOplog(oplog_id);
  }
  STATS_BG_ADD_PER_CLOCK_OPLOG_SIZE(accum_size);
  return accum_size;
}/*}}}*/

/**
 */
void AbstractBgWorker::RecvAppInitThreadConnection(/*{{{*/
    int32_t *num_connected_app_threads) {
  zmq::message_t zmq_msg;
  int32_t sender_id;
  comm_bus_->RecvInProc(&sender_id, &zmq_msg);
  MsgType msg_type = MsgBase::get_msg_type(zmq_msg.data());
  CHECK_EQ(msg_type, kAppConnect) << "send_id = " << sender_id;
  ++(*num_connected_app_threads);
  CHECK(*num_connected_app_threads <= GlobalContext::get_num_app_threads());
}/*}}}*/

/**
 */
void AbstractBgWorker::CheckForwardRowRequestToServer(/*{{{*/
    int32_t app_thread_id, RowRequestMsg &row_request_msg) {

  int32_t table_id = row_request_msg.get_table_id();
  int32_t row_id = row_request_msg.get_row_id();
  int32_t clock = row_request_msg.get_clock();
  bool forced = row_request_msg.get_forced_request();

  if (!forced) {
    // Check if the row exists in process cache
    RowAccessor row_accessor;
    ClientRow *client_row = tables_->find(table_id)->second->
        get_process_storage().Find(row_id, &row_accessor);

    if (client_row != nullptr &&
            GlobalContext::get_consistency_model() == SSP &&
            client_row->GetClock() >= clock) {
        SendRowRequestReplyToApp(app_thread_id, table_id, row_id, client_row->GetClock());
        return;
    }
  }

  RowRequestInfo request_info;
  request_info.app_thread_id = app_thread_id;
  request_info.clock = row_request_msg.get_clock();
  request_info.version = GetUpdateVersion(table_id) - 1;

  // Version in request denotes the update version that the row on server can
  // see. Which should be 1 less than the current version number.
  // raajay: per_worker_update_version_ is the latest version that is pushed to
  // the server from this client.
  // which means the request can be for one less than that?

  // (raajay) when ever an app thread requests for a row and it is not found
  // in the process storage, then the following lines are encountered.
  // Basically, a row request has to be sent to the server now. We also
  // remember that the app thread made a request for the row when the current
  // version of the model was blah.

  bool should_be_sent = row_request_oplog_mgr_->AddRowRequest(request_info, table_id, row_id);
  if (should_be_sent) {
      SendRowRequestToServer(row_request_msg);
  }
}/*}}}*/

/**
 */
void AbstractBgWorker::InsertUpdateRow(const int32_t table_id, const int32_t row_id, const void *data, const size_t row_update_size,/*{{{*/
        int32_t new_clock, int32_t global_row_version) {

    auto iter = tables_->find(table_id);
    CHECK(iter != tables_->end()) << "Cannot find table " << table_id;

    RowAccessor row_accessor;
    ClientRow *row = iter->second->get_process_storage().Find(row_id, &row_accessor);

    if(row == nullptr) {
        AbstractRow *row_data = ClassRegistry<AbstractRow>::GetRegistry().CreateObject(iter->second->get_row_type());
        row_data->Deserialize(data, row_update_size);
        row = CreateClientRow(new_clock, global_row_version, row_data);
        iter->second->get_process_storage().Insert(row_id, row);

    } else {
        row->GetRowDataPtr()->GetWriteLock();
        row->GetRowDataPtr()->ResetRowData(data, row_update_size);
        row->GetRowDataPtr()->ReleaseWriteLock();
        row->SetClock(new_clock);
        row->SetGlobalVersion(global_row_version);
    }

}/*}}}*/

/**
 * @brief Act on the response from the server.
 */
void AbstractBgWorker::HandleServerRowRequestReply(/*{{{*/
    int32_t server_id, ServerRowRequestReplyMsg &server_row_request_reply_msg) {

  int32_t table_id = server_row_request_reply_msg.get_table_id();
  int32_t row_id = server_row_request_reply_msg.get_row_id();
  int32_t new_clock = server_row_request_reply_msg.get_clock();
  int32_t global_row_version = server_row_request_reply_msg.get_global_row_version();

  const void *data = server_row_request_reply_msg.get_row_data();
  size_t row_size = server_row_request_reply_msg.get_row_size();

  InsertUpdateRow(table_id, row_id, data, row_size, new_clock, global_row_version);

  // populate app_thread_ids with the list of app threads whose request can be
  // satisfied with this update.
  std::vector<int32_t> app_thread_ids;
  int32_t clock_to_request = row_request_oplog_mgr_->InformReply(table_id,
          row_id, new_clock, GetUpdateVersion(table_id), &app_thread_ids);

  if (clock_to_request >= 0) {
    SendRowRequestToServer(table_id, row_id, clock_to_request);
  }

  for (int app_thread_id : app_thread_ids) {
      SendRowRequestReplyToApp(app_thread_id, table_id, row_id, new_clock);
  }
}/*}}}*/

/**
 * Send message to bg worker thread.
 */
size_t AbstractBgWorker::SendMsg(MsgBase *msg) {/*{{{*/
  size_t sent_size = comm_bus_->SendInProc(my_id_, msg->get_mem(), msg->get_size());
  return sent_size;
}/*}}}*/

/**
 * Recv message on bg worker thread
 */
void AbstractBgWorker::RecvMsg(zmq::message_t &zmq_msg) {/*{{{*/
  int32_t sender_id;
  comm_bus_->RecvInProc(&sender_id, &zmq_msg);
}/*}}}*/

/**
 */
void AbstractBgWorker::IncrementUpdateVersion(int32_t table_id) {/*{{{*/
      if (table_id == ALL_TABLES) {
          for (auto key : table_update_version_.GetKeys()) {
              table_update_version_.Increment(key, 1);
          }
      } else {
          table_update_version_.Increment(table_id, 1);
      }
}/*}}}*/

/**
 */
uint32_t AbstractBgWorker::GetUpdateVersion(int32_t table_id)  {/*{{{*/
    if (table_id == ALL_TABLES) {
        uint32_t minimum_version = UINT_MAX;
        for(auto it : table_update_version_.GetKeys()) {
            minimum_version = std::min(minimum_version, table_update_version_.Get(it));
        }
        CHECK_NE(minimum_version, UINT_MAX) << "Update version: " << table_update_version_.ToString();
        return minimum_version;
    } else {
        return table_update_version_.Get(table_id);
    }
}/*}}}*/

/**
 */
void AbstractBgWorker::SendRowRequestToServer(int32_t table_id, int32_t row_id, int32_t clock) {/*{{{*/
    RowRequestMsg msg;
    msg.get_table_id() = table_id;
    msg.get_row_id() = row_id;
    msg.get_clock() = clock;
    SendRowRequestToServer(msg);
}/*}}}*/

/**
 */
void AbstractBgWorker::SendRowRequestToServer(RowRequestMsg &msg) {/*{{{*/
    int32_t server_id = GlobalContext::GetPartitionServerID(msg.get_row_id(), my_comm_channel_idx_);
    VLOG(20) << "RR BgThread (" << my_id_
        << ") >>> ServerThread (" << server_id << ") "
        << petuum::GetTableRowStringId(msg.get_table_id(), msg.get_row_id());
    Send(&msg, server_id);
}/*}}}*/

/**
 */
void AbstractBgWorker::SendRowRequestReplyToApp(int32_t app_id,/*{{{*/
        int32_t table_id, int32_t row_id, int32_t row_clock) {
    VLOG(20) << "RRR BgThread (" << my_id_ << ") >>> App Thread ("
             << app_id << ") "
             << petuum::GetTableRowStringId(table_id, row_id)
             << " clock=" << row_clock;
    RowRequestReplyMsg msg;
    size_t sent_size = comm_bus_->SendInProc(app_id, msg.get_mem(), msg.get_size());
    CHECK_EQ(sent_size, msg.get_size());
}/*}}}*/

/**
 */
void AbstractBgWorker::PrepareBeforeInfiniteLoop() {/*{{{*/
  // for each table initialize a version counter
  for (auto &it : *tables_) {
      table_update_version_.Set(it.first, 0);
  }
}/*}}}*/

/**
 */
void AbstractBgWorker::HandleSchedulerResponseMsg(SchedulerResponseMsg *msg) {/*{{{*/
    int32_t oplog_id = msg->get_oplog_id();
    ClientSendOpLogMsg *oplog_msg = oplog_storage_->GetOplogMsg(oplog_id);
    MemTransfer::TransferMem(comm_bus_, msg->get_dest_id(), oplog_msg);
    oplog_storage_->EraseOplog(oplog_id);
}/*}}}*/

/**
 * The infinite loop
 */
void *AbstractBgWorker::operator()() {/*{{{*/
  STATS_REGISTER_THREAD(kBgThread);

  ThreadContext::RegisterThread(my_id_);

  SetupCommBus(my_id_);

  BgServerHandshake();

  pthread_barrier_wait(init_barrier_);

  int32_t num_connected_app_threads = 0;
  int32_t num_deregistered_app_threads = 0;
  int32_t num_shutdown_acked_servers = 0;
  int32_t num_ephemeral_app_threads = 0;

  VLOG(5) << "THREAD-" << my_id_ << ": Prepare to connect with app threads";
  RecvAppInitThreadConnection(&num_connected_app_threads);
  VLOG(5) << "THREAD-" << my_id_ << ": Bg Worker thread:" << my_id_
          << " connected with " << num_connected_app_threads << " app threads.";
  from_start_timer_.restart();

  if (my_comm_channel_idx_ == 0) {
    HandleCreateTables();
  }
  pthread_barrier_wait(create_table_barrier_);

  // Initialize data structures
  PrepareBeforeInfiniteLoop();

  zmq::message_t zmq_msg;
  int32_t sender_id;
  MsgType msg_type;
  void *msg_mem;
  bool destroy_mem = false;
  long timeout_milli = GlobalContext::get_bg_idle_milli();

  // here, the BgWorker runs an infinite loop and processes the different
  // messages
  // initiated by either the AppThread (eg. RequestRow) or server thread (e.g. )
  // Further, it also creates new handle clock messages I guess.
  while (true) {
    bool received = WaitMsg_(&sender_id, &zmq_msg, timeout_milli);

    if (!received) {
      timeout_milli = BgIdleWork();
      continue;
    } else {
      // (TODO): verify the difference in performance
      timeout_milli = ResetBgIdleMilli();
    }

    msg_type = MsgBase::get_msg_type(zmq_msg.data());
    destroy_mem = false;

    if (msg_type == kMemTransfer) {
      MemTransferMsg mem_transfer_msg(zmq_msg.data());
      msg_mem = mem_transfer_msg.get_mem_ptr();
      msg_type = MsgBase::get_msg_type(msg_mem);
      destroy_mem = true;
    } else {
      msg_mem = zmq_msg.data();
    }

    switch (msg_type) {

    case kAppConnect:
        ++num_connected_app_threads;

        CHECK(num_connected_app_threads <= GlobalContext::get_num_app_threads())
            << "num_connected_app_threads = " << num_connected_app_threads
            << " get_num_app_threads() = "
            << GlobalContext::get_num_app_threads();
        break;

    case kAppThreadDereg:
        ++num_deregistered_app_threads;
        // when all the app thread have de-registered, send a shut down message to
        // namenode,
        // scheduler and all the servers.
        if (num_deregistered_app_threads == GlobalContext::get_num_app_threads()) {
          ClientShutDownMsg msg;
          Send(&msg, GlobalContext::get_name_node_id());
          Send(&msg, GlobalContext::get_scheduler_recv_thread_id());
          // XXX(raajay): We do not send a shutdown to scheduler send thread.
          // The receiver thread is responsible for killing the scheduler send
          // thread.
          SendToAll(&msg, server_ids_);
        }
        break;

    case kSyncThreadConnect:
        ++num_ephemeral_app_threads;
        // (raajay) since ephemeral threads have an exclusive lock
        // on the tables, the number of such threads at any given
        // point should not exceed the number of tables.
        CHECK_LE(num_ephemeral_app_threads, GlobalContext::get_num_tables());
        break;

    case kSyncThreadDereg:
        --num_ephemeral_app_threads;
        CHECK_GE(num_ephemeral_app_threads, 0);
        break;

    case kServerShutDownAck:
        ++num_shutdown_acked_servers;
        // if all them ack your shutdown, only then de-register and terminate out
        // of the infinite loop
        if (num_shutdown_acked_servers == GlobalContext::get_num_server_clients() + 2) {
          comm_bus_->ThreadDeregister();
          STATS_DEREGISTER_THREAD();
          return nullptr;
        }
        break;

    case kApplicationThreadRowRequest:
        // app thread typically sends a row request, your job is to forward it to
        // the server.
        {
        RowRequestMsg row_request_msg(zmq_msg.data());
        CheckForwardRowRequestToServer(sender_id, row_request_msg);
        }
        break;

    case kServerRowRequestReply:
        // server responds with the information of rows requested
        {
        ServerRowRequestReplyMsg server_row_request_reply_msg(msg_mem);
        HandleServerRowRequestReply(sender_id, server_row_request_reply_msg);
        }
        break;

    case kBgClock:
      // clock message is sent from the app thread using the static function
      // defined in bgworkers.
        {
        BgClockMsg clock_msg(msg_mem);
        timeout_milli = HandleClockMsg(clock_msg.get_table_id(), true);
        ++worker_clock_;
        VLOG(5) << "THREAD-" << my_id_ << ": "
                << "Increment client clock in bgworker:" << my_id_ << " to "
                << worker_clock_;
        STATS_BG_CLOCK();
        }
        break;

    case kBgSendOpLog:
        {
        BgSendOpLogMsg oplog_msg(msg_mem);
        timeout_milli = HandleClockMsg(oplog_msg.get_table_id(), false);
        }
        break;

    case kServerOpLogAck:
        {
        STATS_MLFABRIC_CLIENT_PUSH_END(sender_id, acked_version);
        }
        break;

    case  kSchedulerResponse:
        {
        SchedulerResponseMsg msg(msg_mem);
        HandleSchedulerResponseMsg(&msg);
        }
        break;

    default:
      LOG(FATAL) << "Unrecognized type " << msg_type;

    }

    if (destroy_mem) {
      MemTransfer::DestroyTransferredMem(msg_mem);
    }
  }

  return 0;
}/*}}}*/
}

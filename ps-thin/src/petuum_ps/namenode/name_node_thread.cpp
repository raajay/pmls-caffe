#include <petuum_ps/namenode/name_node_thread.hpp>
#include <iostream>

namespace petuum {

NameNodeThread::NameNodeThread(pthread_barrier_t *init_barrier)
    : NonWorkerThread(GlobalContext::get_name_node_id(), init_barrier,
            GlobalContext::get_num_total_bg_threads()) {}


void NameNodeThread::InitNameNode() {
  int32_t num_bgs = 0;
  int32_t num_servers = 0;
  int32_t num_expected_conns = (GlobalContext::get_num_total_bg_threads() +
                                GlobalContext::get_num_total_server_threads());

  VLOG(5) << "Number of expected connections at name node="
          << num_expected_conns;

  int32_t nc;
  for (nc = 0; nc < num_expected_conns; ++nc) {
    int32_t sender_id = WaitForConnect();
    if(GlobalContext::is_worker_thread(sender_id)) {
        bg_worker_ids_[num_bgs] = sender_id;
        ++num_bgs;
    } else if (GlobalContext::is_server_thread(sender_id)) {
        ++num_servers;
    } else {
        LOG(FATAL) << "Got a connection from a entity other then server/worker";
    }
  }

  VLOG(5) << "Total connections received: " << nc;
  CHECK_EQ(num_bgs, GlobalContext::get_num_total_bg_threads());
  CHECK_EQ(num_servers, GlobalContext::get_num_total_server_threads());

  server_obj_.Init(0, bg_worker_ids_);

  // Note that we send two types of messages to the bg worker threads
  ConnectServerMsg connect_server_msg;
  VLOG(5) << "Name node - send connect server to all bg threads";
  SendToAll(reinterpret_cast<MsgBase *>(&connect_server_msg), bg_worker_ids_);

  ClientStartMsg client_start_msg;
  VLOG(5) << "Name node - send client start to all bg threads";
  SendToAll(reinterpret_cast<MsgBase *>(&client_start_msg), bg_worker_ids_);
}

bool NameNodeThread::HaveCreatedAllTables() {
  if ((int32_t)create_table_map_.size() < GlobalContext::get_num_tables())
    return false;

  for (const auto &create_table_pair : create_table_map_) {
    if (!create_table_pair.second.RepliedToAllClients()) {
      return false;
    }
  }
  return true;
}

/**
 * Once all the tables are created at the server, we notify
 * all the workers the success notification.
 */
void NameNodeThread::SendCreatedAllTablesMsg() {
  CreatedAllTablesMsg created_all_tables_msg;
  for (auto client_id : GlobalContext::get_worker_client_ids()) {
    int32_t head_bg_id = GlobalContext::get_head_bg_id(client_id);
    size_t sent_size = (comm_bus_->*(comm_bus_->SendAny_))(
        head_bg_id, created_all_tables_msg.get_mem(),
        created_all_tables_msg.get_size());
    CHECK_EQ(sent_size, created_all_tables_msg.get_size());
  }
}


void NameNodeThread::HandleCreateTable(int32_t sender_id,
                                       CreateTableMsg &create_table_msg) {
  int32_t table_id = create_table_msg.get_table_id();

  if (create_table_map_.count(table_id) == 0) {
    // create a new TableInfo if this the first request for the table.
    TableInfo table_info;
    table_info.table_staleness = create_table_msg.get_staleness();
    table_info.row_type = create_table_msg.get_row_type();
    table_info.row_capacity = create_table_msg.get_row_capacity();
    table_info.oplog_dense_serialized =
        create_table_msg.get_oplog_dense_serialized();
    table_info.row_oplog_type = create_table_msg.get_row_oplog_type();
    table_info.dense_row_oplog_capacity =
        create_table_msg.get_dense_row_oplog_capacity();

    VLOG(5) << "Calling CreateTable from instantiation of Server("
            << &server_obj_ << ") for table=" << table_id
            << " in name_node_thread";
    server_obj_.CreateTable(table_id, table_info);

    create_table_map_.insert(std::make_pair(
        table_id, CreateTableInfo())); // access it to call default constructor
    SendToAll(reinterpret_cast<MsgBase *>(&create_table_msg),
            GlobalContext::get_all_server_ids());
  }

  if (create_table_map_[table_id].ReceivedFromAllServers()) {

    // if the current table is already created, let the bg worker know that.
    CreateTableReplyMsg create_table_reply_msg;
    create_table_reply_msg.get_table_id() = create_table_msg.get_table_id();
    size_t sent_size = (comm_bus_->*(comm_bus_->SendAny_))(
        sender_id, create_table_reply_msg.get_mem(),
        create_table_reply_msg.get_size());
    CHECK_EQ(sent_size, create_table_reply_msg.get_size());

    ++create_table_map_[table_id].num_clients_replied_;

    if (HaveCreatedAllTables()) {
      SendCreatedAllTablesMsg();
    }

  } else {
    // to be sent later
    create_table_map_[table_id].bgs_to_reply_.push(sender_id);
  }
}

void NameNodeThread::HandleCreateTableReply(
    CreateTableReplyMsg &create_table_reply_msg) {

  int32_t table_id = create_table_reply_msg.get_table_id();
  ++create_table_map_[table_id].num_servers_replied_;

  if (create_table_map_[table_id].ReceivedFromAllServers()) {

    std::queue<int32_t> &bgs_to_reply =
        create_table_map_[table_id].bgs_to_reply_;
    while (!bgs_to_reply.empty()) {
      int32_t bg_id = bgs_to_reply.front();
      bgs_to_reply.pop();
      size_t sent_size = (comm_bus_->*(comm_bus_->SendAny_))(
          bg_id, create_table_reply_msg.get_mem(),
          create_table_reply_msg.get_size());
      CHECK_EQ(sent_size, create_table_reply_msg.get_size());
      ++create_table_map_[table_id].num_clients_replied_;
    }

    if (HaveCreatedAllTables()) {
      SendCreatedAllTablesMsg();
    }
  }
}


/**
 */
void *NameNodeThread::operator()() {
  ThreadContext::RegisterThread(my_id_);

  // set up thread-specific server context
  SetupCommBus(my_id_);

  pthread_barrier_wait(init_barrier_);
  VLOG(0) << "NameNode accepting connections";

  // wait for connections from client, server
  InitNameNode();

  zmq::message_t zmq_msg;
  int32_t sender_id;

  while (1) {
    (comm_bus_->*(comm_bus_->RecvAny_))(&sender_id, &zmq_msg);
    MsgType msg_type = MsgBase::get_msg_type(zmq_msg.data());

    switch (msg_type) {
    case kClientShutDown: {
      bool shutdown = HandleShutDownMsg();
      if (shutdown) {
        comm_bus_->ThreadDeregister();
        VLOG(0) << "Shutting down the name node thread";
        return 0;
      }
      break;
    }
    case kCreateTable: {
      CreateTableMsg create_table_msg(zmq_msg.data());
      HandleCreateTable(sender_id, create_table_msg);
      break;
    }
    case kCreateTableReply: {
      CreateTableReplyMsg create_table_reply_msg(zmq_msg.data());
      HandleCreateTableReply(create_table_reply_msg);
      break;
    }
    default:
      LOG(FATAL) << "Unrecognized message type " << msg_type
                 << " sender = " << sender_id;
    }
  }
}

} // end namespace -- petuum

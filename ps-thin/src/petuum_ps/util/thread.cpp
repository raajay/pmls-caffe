#include <petuum_ps/util/thread.hpp>
#include <petuum_ps/thread/ps_msgs.hpp>

namespace petuum {


    Thread::Thread(int32_t my_id, pthread_barrier_t *init_barrier)
      : my_id_(my_id),
        init_barrier_(init_barrier),
        comm_bus_(GlobalContext::comm_bus) {}

  /**
   */
  void Thread::SendToAll(MsgBase *msg, std::vector<int32_t> dest_ids) {
    for (const auto &id : dest_ids) {
      Send(msg, id);
    }
  }

  /**
   */
  void Thread::Send(MsgBase *msg, int32_t dest_id) {
    size_t sent_size = (comm_bus_->*(comm_bus_->SendAny_))(dest_id, msg->get_mem(), msg->get_size());
    CHECK_EQ(sent_size, msg->get_size());
  }

  /**
   */
  void Thread::ConnectTo(int32_t dest_id, int32_t my_id) {
      ConnectMsg connect_msg;
      connect_msg.get_client_id() = GlobalContext::get_client_id();
      connect_msg.get_thread_id() = my_id;

      void *msg = connect_msg.get_mem();
      int32_t msg_size = connect_msg.get_size();

      if (comm_bus_->IsLocalEntity(dest_id)) {
          comm_bus_->ConnectTo(dest_id, msg, msg_size);
      } else {
          HostInfo dest_info = GlobalContext::get_host_info(dest_id);
          std::string server_addr = dest_info.ip + ":" + dest_info.port;
          comm_bus_->ConnectTo(dest_id, server_addr, msg, msg_size);
      }
  }

  /**
   */
  int32_t Thread::WaitForConnect() {
    int32_t sender_id;
    zmq::message_t zmq_msg;
    (comm_bus_->*(comm_bus_->RecvAny_))(&sender_id, &zmq_msg);
    MsgType msg_type = MsgBase::get_msg_type(zmq_msg.data());

    CHECK_EQ(msg_type, kGenericConnect) << "Unknown type of connect message";

    ConnectMsg msg(zmq_msg.data());
    VLOG(5) << "Received connection from client_id=" << msg.get_client_id()
        << " and thread_id=" << msg.get_thread_id();

    return sender_id;
  }

  /**
   */
  void Thread::WaitForReply(int32_t dest_id, MsgType expected_msg_type) {
    zmq::message_t zmq_msg;
    int32_t sender_id;

    (comm_bus_->*(comm_bus_->RecvAny_))(&sender_id, &zmq_msg);
    MsgType msg_type = MsgBase::get_msg_type(zmq_msg.data());

    CHECK_EQ(msg_type, expected_msg_type);

    if (dest_id == GlobalContext::kAnyThreadId) {
        return;
    }

    CHECK_EQ(sender_id, dest_id);
  }

  /**
   */
  void Thread::SetupCommBus(int32_t thread_id) {
    CommBus::Config comm_config;
    comm_config.entity_id_ = thread_id;

    if (GlobalContext::get_num_clients() > 1
            && !GlobalContext::is_worker_thread(thread_id)) {
      comm_config.ltype_ = CommBus::kInProc | CommBus::kInterProc;
      HostInfo host_info = GlobalContext::get_host_info(thread_id);
      comm_config.network_addr_ = "*:" + host_info.port;
    } else {
      comm_config.ltype_ = CommBus::kInProc;
    }
    comm_bus_->ThreadRegister(comm_config);
  }

  /**
   */
  int32_t Thread::GetNextMessage(zmq::message_t &msg) {
    int32_t sender_id;
    (comm_bus_->*(comm_bus_->RecvAny_))(&sender_id, &msg);
    return sender_id;
  }

}

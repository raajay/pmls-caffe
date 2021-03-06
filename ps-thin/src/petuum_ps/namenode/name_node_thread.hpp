// name_node_thread.hpp
// author: jinliang

#pragma once

#include <vector>
#include <pthread.h>
#include <petuum_ps/util/pthread_barrier.hpp>
#include <queue>

#include <petuum_ps/util/non_worker_thread.hpp>
#include <petuum_ps/server/server.hpp>
#include <petuum_ps/thread/ps_msgs.hpp>
#include <petuum_ps/comm_bus/comm_bus.hpp>

namespace petuum {

class NameNodeThread : public NonWorkerThread {
public:
  NameNodeThread(pthread_barrier_t *init_barrier);
  ~NameNodeThread() {}

  virtual void *operator()();

  virtual void ShutDown() { Join(); }

private:
  struct CreateTableInfo {
    int32_t num_clients_replied_;
    int32_t num_servers_replied_;
    std::queue<int32_t> bgs_to_reply_;
    CreateTableInfo()
        : num_clients_replied_(0), num_servers_replied_(0), bgs_to_reply_() {}

    ~CreateTableInfo() {}

    CreateTableInfo &operator=(const CreateTableInfo &info_obj) {
      num_clients_replied_ = info_obj.num_clients_replied_;
      num_servers_replied_ = info_obj.num_servers_replied_;
      bgs_to_reply_ = info_obj.bgs_to_reply_;
      return *this;
    }

    bool ReceivedFromAllServers() const {
      return (num_servers_replied_ ==
              GlobalContext::get_num_total_server_threads());
    }

    bool RepliedToAllClients() const {
      return (num_clients_replied_ == GlobalContext::get_num_worker_clients());
    }
  };

  void InitNameNode();

  bool HaveCreatedAllTables();
  void SendCreatedAllTablesMsg();

  void HandleCreateTable(int32_t sender_id, CreateTableMsg &create_table_msg);
  void HandleCreateTableReply(CreateTableReplyMsg &create_table_reply_msg);

  // one bg per client is refered to as head bg
  std::map<int32_t, CreateTableInfo> create_table_map_;
  Server server_obj_;
};
}

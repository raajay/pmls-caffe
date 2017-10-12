#include <petuum_ps/storage/oplog_storage.hpp>
#include <glog/logging.h>

namespace petuum {

    OplogStorage::OplogStorage() {
        oplog_counter_ = 0;
    }

    void OplogStorage::Add(int32_t server_id, ClientSendOpLogMsg *msg) {
        ++oplog_counter_;
        if (server_oplog_ids_.find(server_id) == server_oplog_ids_.end()) {
            server_oplog_ids_.insert(std::make_pair(server_id, std::deque<int32_t>()));
        }
        auto iter = server_oplog_ids_.find(server_id);
        CHECK(iter != server_oplog_ids_.end());
        iter->second.push_back(oplog_counter_);
        oplogs_[oplog_counter_] = msg;
    }

    ClientSendOpLogMsg* OplogStorage::GetNextOplog(int32_t server_id) {
        auto iter = server_oplog_ids_.find(server_id);
        CHECK(iter != server_oplog_ids_.end());
        CHECK(!iter->second.empty());
        return Get(iter->second.front());
    }

    ClientSendOpLogMsg* OplogStorage::GetNextOplogAndErase(int32_t server_id) {
        auto iter = server_oplog_ids_.find(server_id);
        CHECK(iter != server_oplog_ids_.end());
        CHECK(!iter->second.empty());
        auto pMsg = GetAndErase(iter->second.front());
        iter->second.pop_front();
        return pMsg;
    }

    ClientSendOpLogMsg* OplogStorage::Get(int32_t oplog_id) {
        auto iter = oplogs_.find(oplog_id);
        CHECK(iter != oplogs_.end());
        return oplogs_[oplog_id];
    }

    ClientSendOpLogMsg* OplogStorage::GetAndErase(int32_t oplog_id) {
        auto pMsg = Get(oplog_id);
        oplogs_.erase(oplog_id);
        return pMsg;
    }

    int32_t OplogStorage::GetNumOplogs() {
        return oplogs_.size();
    }
}

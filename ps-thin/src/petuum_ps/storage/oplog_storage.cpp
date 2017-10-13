#include <petuum_ps/thread/context.hpp>
#include <petuum_ps/storage/oplog_storage.hpp>
#include <glog/logging.h>

namespace petuum {

    OplogStorage::OplogStorage(int32_t id)
        : worker_id_(id), oplog_counter_(0), num_oplogs_(0) {}


    int32_t OplogStorage::Add(int32_t server_id, ClientSendOpLogMsg *msg) {
        ++oplog_counter_;

        auto pMetaData = new OplogStorageMetaData();
        pMetaData->oplog_msg = msg;
        pMetaData->oplog_size = msg->get_size();
        pMetaData->oplog_unique_id = oplog_counter_;
        pMetaData->server_id = server_id;

        oplogs_[oplog_counter_] = pMetaData;
        ++num_oplogs_;
        return oplog_counter_;
    }

    void OplogStorage::SetTableId(int32_t oplog_id, int32_t table_id) {
        GetMetaData(oplog_id)->table_id = table_id;
    }

    void OplogStorage::SetClockAdvanced(int32_t oplog_id, bool clock_advanced) {
        GetMetaData(oplog_id)->clock_advanced = clock_advanced;
    }

    void OplogStorage::SetBgVersion(int32_t oplog_id, int32_t bg_version) {
        GetMetaData(oplog_id)->bg_version = bg_version;
    }

    void OplogStorage::SetClock(int32_t oplog_id, int32_t curr_clock) {
        GetMetaData(oplog_id)->clock_to_use = curr_clock;
    }

    void OplogStorage::SetModelVersion(int32_t oplog_id, int32_t model_version) {
        GetMetaData(oplog_id)->oplog_model_version = model_version;
    }

    int32_t OplogStorage::GetServerId(int32_t oplog_id) {
        return GetMetaData(oplog_id)->server_id;
    }

    void OplogStorage::EraseOplog(int32_t oplog_id) {
        auto pMetaData = GetMetaData(oplog_id);
        // delete the client oplog message
        delete pMetaData->oplog_msg;
        // delete the meta data object
        delete oplogs_[oplog_id];
        // delete the entry from oplogs map
        oplogs_.erase(oplog_id);
        num_oplogs_--;
    }


//      msg->get_table_id() = table_id;
//      msg->get_is_clock() = clock_advanced;
//      msg->get_client_id() = GlobalContext::get_client_id();
//      msg->get_version() = GetUpdateVersion(table_id);
//      msg->get_bg_clock() = clock_has_pushed_ + 1;
    ClientSendOpLogMsg* OplogStorage::GetOplogMsg(int32_t oplog_id) {
        auto pMetaData = GetMetaData(oplog_id);
        pMetaData->oplog_msg->get_table_id() = pMetaData->table_id;
        pMetaData->oplog_msg->get_is_clock() = pMetaData->clock_advanced;
        pMetaData->oplog_msg->get_client_id() = GlobalContext::get_client_id();
        pMetaData->oplog_msg->get_version() = pMetaData->bg_version;
        pMetaData->oplog_msg->get_bg_clock() = pMetaData->clock_to_use;
        return pMetaData->oplog_msg;
    }

    SchedulerRequestMsg* OplogStorage::GetOplogRequestMsg(int32_t oplog_id) {
        auto pData = GetMetaData(oplog_id);

        auto pMsg = new SchedulerRequestMsg();
        pMsg->get_dest_id() = pData->server_id;
        pMsg->get_source_id() = worker_id_;
        pMsg->get_oplog_id() = pData->oplog_unique_id;
        pMsg->get_oplog_size() = pData->oplog_size;
        pMsg->get_oplog_version() = pData->oplog_model_version;

        return pMsg;
    }

    int32_t OplogStorage::GetNumOplogs() {
        return num_oplogs_;
    }

    size_t OplogStorage::GetSize(int32_t oplog_id) {
        return oplogs_[oplog_id]->oplog_size;
    }
}

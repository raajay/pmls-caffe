#pragma once

#include <petuum_ps/util/OneDimStorage.hpp>
#include <petuum_ps/thread/ps_msgs.hpp>

namespace petuum {

    /**
     */
    class OplogStorageMetaData {
        public:

            OplogStorageMetaData() {
                table_id = -1;
                clock_advanced = false;
                bg_version = -1;
                clock_to_use = -1;
                oplog_size = 0;
                oplog_unique_id = -1;
                oplog_model_version = -1;
                oplog_msg = nullptr;
                server_id = -1;
                request_sent = false;
                oplog_sent = false;
            }


            // fields set explicitly through external APIs
            int32_t table_id;
            bool clock_advanced;
            int32_t bg_version;
            int32_t clock_to_use;
            int32_t oplog_model_version;


            // fields set implicitly
            ClientSendOpLogMsg *oplog_msg;
            int32_t server_id;
            size_t oplog_size;
            int32_t oplog_unique_id;
            bool request_sent;
            bool oplog_sent;
    };

    /**
     */
    class OplogStorage {
        public:
            OplogStorage(int32_t worker_id);

            ~OplogStorage() = default;

            int32_t Add(int32_t server_id, ClientSendOpLogMsg *msg);

            void SetTableId(int32_t oplog_id, int32_t table_id);

            void SetClockAdvanced(int32_t oplog_id, bool clock_advanced);

            void SetBgVersion(int32_t oplog_id, int32_t bg_version);

            void SetClock(int32_t oplog_id, int32_t curr_clock);

            void SetModelVersion(int32_t oplog_id, int32_t model_version);

            int32_t GetServerId(int32_t oplog_id);

            size_t GetSize(int32_t oplog_id);

            void EraseOplog(int32_t oplog_id);

            ClientSendOpLogMsg *GetOplogMsg(int32_t unique_id);

            SchedulerRequestMsg *GetOplogRequestMsg(int32_t oplog_id);

            int32_t GetNumOplogs();

        private:
            OplogStorageMetaData *GetMetaData(int32_t oplog_id) {
                auto iter = oplogs_.find(oplog_id);
                CHECK(iter != oplogs_.end());
                return oplogs_[oplog_id];
            }

            int32_t worker_id_;
            int32_t oplog_counter_;
            int32_t num_oplogs_;
            std::map<int32_t, OplogStorageMetaData*> oplogs_;
    };
}

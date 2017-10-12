#pragma once

#include <petuum_ps/util/OneDimStorage.hpp>
#include <petuum_ps/thread/ps_msgs.hpp>

namespace petuum {

    /**
     */
    class OplogStorage {
        public:
            OplogStorage();

            ~OplogStorage() = default;

            void Add(int32_t server_id, ClientSendOpLogMsg *msg);

            ClientSendOpLogMsg* GetNextOplog(int32_t server_id);

            ClientSendOpLogMsg* GetNextOplogAndErase(int32_t server_id);

            /**
             * @brief Return the oplog based on the unique oplog id
             */
            ClientSendOpLogMsg* Get(int32_t oplog_id);

            /**
             * @brief After erasing, subsequent Get will return a nullptr
             */
            ClientSendOpLogMsg* GetAndErase(int32_t oplog_id);

            int32_t GetNumOplogs();

            bool ContainsOpLog(int32_t server_id);

            int32_t GetNextOplogIdAndErase(int32_t server_id);

        private:
            int32_t oplog_counter_;
            std::map<int32_t, ClientSendOpLogMsg*> oplogs_;
            std::map<int32_t, std::deque<int32_t> > server_oplog_ids_;
    };
}

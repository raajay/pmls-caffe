#pragma once

#include <petuum_ps/thread/msg_base.hpp>
#include <petuum_ps/thread/ps_msgs.hpp>

namespace petuum {


    class MLFabricRequest {
        public:
            MLFabricRequest() = default;
            MLFabricRequest(SchedulerRequestMsg* msg)
                : InputDestinationId(msg->get_dest_id()),
                  SourceId(msg->get_source_id()),
                  OplogSize(msg->get_oplog_size()),
                  OplogId(msg->get_oplog_id()) {

                OutputDestinationId = msg->get_dest_id();

            }

            std::string toString() {
                std::stringstream ss;
                ss << "SourceId=" << SourceId;
                ss << " InputDestinationId=" << InputDestinationId;
                ss << " OplogId=" << OplogId;
                ss << " OplogSize=" << OplogSize;
                return ss.str();
            }

            // Input Fields
            const int32_t InputDestinationId;
            const int32_t SourceId;
            const int32_t OplogSize;
            const int32_t OplogId;

            // Output Fields
            int32_t OutputDestinationId;
    };


    class Scheduler {
        public:
            virtual ~Scheduler() {};
            virtual void AddRequest(MLFabricRequest *request) = 0;
            virtual MLFabricRequest* TakeRequest() = 0;
    };

}

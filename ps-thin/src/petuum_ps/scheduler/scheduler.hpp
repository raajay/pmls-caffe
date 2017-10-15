#pragma once

#include <petuum_ps/thread/msg_base.hpp>
#include <petuum_ps/thread/ps_msgs.hpp>
#include <petuum_ps/util/OneDimCounter.hpp>

namespace petuum {


    class MLFabricRequest {
        public:
            MLFabricRequest() = default;

            MLFabricRequest(SchedulerRequestMsg* msg)
                : InputDestinationId(msg->get_dest_id()),
                  SourceId(msg->get_source_id()),
                  OplogSize(msg->get_oplog_size()),
                  OplogId(msg->get_oplog_id()),
                  OplogVersion(msg->get_oplog_version()) {

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
            const int32_t OplogVersion;

            // Output Fields
            int32_t OutputDestinationId;
    };


    class Scheduler {
        public:
            Scheduler() {
                observed_delay_histogram_ = new OneDimCounter<int32_t, int32_t>();
            }

            virtual ~Scheduler() {
                delete observed_delay_histogram_;
            };

            virtual void AddRequest(MLFabricRequest *request) = 0;
            virtual MLFabricRequest* TakeRequest() = 0;
            OneDimCounter<int32_t, int32_t> *GetDelayHistogram() {
                return observed_delay_histogram_;
            }

        protected:
            OneDimCounter<int32_t, int32_t> *observed_delay_histogram_;
    };

}

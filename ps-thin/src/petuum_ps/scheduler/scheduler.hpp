#pragma once

namespace petuum {

    class Scheduler {
        public:
            virtual void AddRequest() = 0;
            virtual void TakeRequest() = 0;
    };

}

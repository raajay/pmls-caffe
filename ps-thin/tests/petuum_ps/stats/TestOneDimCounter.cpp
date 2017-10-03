#include <gtest/gtest.h>
#include <petuum_ps/stats/OneDimCounter.hpp>

namespace petuum {
TEST(TestOneDimCounter, Test1) {
    OneDimCounter<int32_t, int32_t> counter;
    counter.Reset();
}
}


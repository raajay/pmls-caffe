//
// Created by raajay on 10/3/17.
//

#include <gtest/gtest.h>
#include <petuum_ps/stats/TwoDimCounter.hpp>
namespace petuum {
TEST(TestTwoDimCounter, Test1) {

  TwoDimCounter<int32_t, int32_t, int32_t> counter;
  //counter.Increment(0, 1, 2);
  //counter.Increment(0, 1, 2);
  EXPECT_EQ(counter.Get(0), 4);
}
}
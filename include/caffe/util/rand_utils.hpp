#ifndef _CAFFE_UTIL_RAND_UTILS_HPP_
#define _CAFFE_UTIL_RAND_UTILS_HPP_

namespace caffe {

    int32_t GetUniformDelayInt(const int32_t low, const int32_t high);
    double GetUniformDelayReal(const double low, const double high);

}

#endif

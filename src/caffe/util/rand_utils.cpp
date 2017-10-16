#include <random>

namespace caffe {

    double GetUniformDelayReal(const double low, const double high) {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_real_distribution<> dis(low, high);
        return dis(gen);
    }

    int32_t GetUniformDelayInt(const int32_t low, const int32_t high) {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<int32_t> dis(low, high);
        return dis(gen);
    }
}

//
// Created by Raajay on 10/2/17.
//

#include "OneDimCounter.hpp"

OneDimCounter::OneDimCounter() = default;

OneDimCounter::~OneDimCounter() = default;

void OneDimCounter::Reset() {
  data_.clear();
}

template<class K, class V>
V OneDimCounter::Get(K key) {
  DataIter iter = data_.find(key);
  return (nullptr == iter) ? 0 : iter->second;
}

template<class K, class V>
V OneDimCounter::GetAll() {
  V return_val = 0;
  for (auto &it : data_) {
    return_val += it.second;
  }
  return return_val;
}

template<class K, class V>
void OneDimCounter::Increment(K key, V value) {
  DataIter iter = data_.find(key);
  if (nullptr == iter) {
    data_.emplace(key, value);
  } else {
    data_[key] += value;
  }
}

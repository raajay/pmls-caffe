//
// Created by Raajay on 10/2/17.
//

#ifndef CAFFE_BYTECOUNTER_HPP
#define CAFFE_BYTECOUNTER_HPP

#include <cstdint>
#include <boost/unordered/unordered_map.hpp>

template<class K, class V>
class OneDimCounter {
public:

  /**
   * Constructor
   */
  OneDimCounter();

  /**
   * Destructor
   */
  ~OneDimCounter();

  /**
   * Set values of all keys to zero
   */
  void Reset();

  /**
   * Get counter value or a key
   */
  V Get(K key);

  /**
   * Get cumulative counter
   */
  V GetAll();

  /**
   * Increment counter
   */
  void Increment(K key, V value);

private:

  boost::unordered::unordered_map<K, V> data_;
  typedef boost::unordered::unordered_map<K, V>::iterator DataIter;
};

#endif //CAFFE_BYTECOUNTER_HPP

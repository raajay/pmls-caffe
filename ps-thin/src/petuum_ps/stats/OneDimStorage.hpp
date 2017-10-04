//
// Created by Raajay on 10/3/17.
//

#ifndef CAFFE_ONEDIMSTORAGE_HPP
#define CAFFE_ONEDIMSTORAGE_HPP

#include <cstdint>
#include <boost/unordered/unordered_map.hpp>

namespace petuum {

    template<class K, class V>
    class OneDimStorage {
    public:
        OneDimStorage() = default;

        ~OneDimStorage() = default;

    private:
        boost::unordered::unordered_map<K, V> data_;
        typedef typename boost::unordered::unordered_map<K, V>::iterator DataIter;
};
}

#endif //CAFFE_ONEDIMSTORAGE_HPP

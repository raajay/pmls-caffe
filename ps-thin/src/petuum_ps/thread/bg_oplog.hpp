#pragma once

#include <map>
#include <boost/noncopyable.hpp>
#include <glog/logging.h>

#include <petuum_ps/thread/bg_oplog_partition.hpp>
#include <petuum_ps/thread/row_oplog_recycle.hpp>
#include <petuum_ps/util/macros.hpp>

namespace petuum {

class BgOpLog : boost::noncopyable {
public:
  BgOpLog() = default;

  ~BgOpLog() {
    for (auto &iter : table_oplog_map_) {
      delete iter.second;
    }
  }

  // takes ownership of the oplog partition
  void Add(int32_t table_id, BgOpLogPartition *bg_oplog_partition_ptr) {
    table_oplog_map_[table_id] = bg_oplog_partition_ptr;
  }

  BgOpLogPartition *Get(int32_t table_id) const {
    return table_oplog_map_.at(table_id);
  }

  size_t num_rows() {
    size_t num_rows = 0;
    for (auto &iter : table_oplog_map_) {
      num_rows += iter.second->num_rows();
    }
    return num_rows;
  }

  int32_t GetModelVersion() const {
      int32_t version = DEFAULT_GLOBAL_VERSION;
      for(auto &it : table_oplog_map_) {
          version = std::max(version, it.second->GetModelVersion());
      }
      return version;
  }

private:
  std::map<int32_t, BgOpLogPartition *> table_oplog_map_;
};
}

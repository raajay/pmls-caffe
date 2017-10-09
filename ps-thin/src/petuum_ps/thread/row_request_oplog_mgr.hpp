#pragma once

#include <map>
#include <vector>
#include <list>
#include <utility>
#include <boost/noncopyable.hpp>
#include <glog/logging.h>

#include <petuum_ps/thread/bg_oplog.hpp>

namespace petuum {

struct RowRequestInfo {
public:
  int32_t app_thread_id;
  int32_t clock;
  uint32_t version;
  bool sent;

  RowRequestInfo() : app_thread_id(0), clock(0), version(0) {}

  RowRequestInfo(const RowRequestInfo &other)
      : app_thread_id(other.app_thread_id), clock(other.clock),
        version(other.version) {}

  RowRequestInfo &operator=(const RowRequestInfo &other) {
    app_thread_id = other.app_thread_id;
    clock = other.clock;
    version = other.version;
    return *this;
  }
};

class RowRequestOpLogMgr : boost::noncopyable {
public:
  RowRequestOpLogMgr() {}

  virtual ~RowRequestOpLogMgr() {}

  // return true unless there's a previous request with lower or same clock
  // number
  virtual bool AddRowRequest(RowRequestInfo &request, int32_t table_id,
                             int32_t row_id) = 0;

  // Get a list of app thread ids that can be satisfied with this reply.
  // Corresponding row requests are removed upon returning.
  // If all row requests prior to some version are removed, those OpLogs are
  // removed as well.
  virtual int32_t InformReply(int32_t table_id, int32_t row_id, int32_t clock,
                              uint32_t curr_version,
                              std::vector<int32_t> *app_thread_ids) = 0;
};

// Keep track of row requests that are sent to server or that could
// be potentially sent to server (row does not exist in process cache
// but a request for that row has been sent out).

// When a requested row is not found in process cache, the bg worker
// checks with RowRequestMgr to see if it has sent out a request for
// that row. If not, send the current request; otherwise, wait for
// server response.

// When the bg worker, receives a reply for a row, it inserts that
// row into process cache and checks with RowRequestMgr to see how many
// row requests that reply may satisfy.

// After a row request is sent to server and before the bg worker receives the
// reply, the bg worker might have sent out multiple sets of updates to server.
// Since the server may buffer the row and reply later, those updates might or
// might not be applied to the row on server.

// Bg worker assgins a mononically-increasing version number for each set of
// updates. The locall version number denotes the latest version of updates
// that have been sent to server. When a row request is sent out, it contains
// the current local version number. Since server receives and processes
// messages in order, when a row request is processed by the server, all updates
// prior to that version (including that version) have been apply to the row on
// the server.

// When the server receives a set of updates from that client after it buffers
// the row request, it should increment the row request's version number
// accordingly. Thus when the client receives a reply for a row request, it
// knows which version of updates have been applied and it should apply the
// missing updates to that row before inserting it to process cache.

// RowRequestMgr is also responsible for keeping track of the sent oplogs. An
// oplog cannot be deleted until all row requests sent prior to its version
// (exclusive) have been replied.

class SSPRowRequestOpLogMgr : public RowRequestOpLogMgr {
public:
  SSPRowRequestOpLogMgr() = default;

  ~SSPRowRequestOpLogMgr() = default;

  // return true unless there's a previous request with lower or same clock
  // number
  bool AddRowRequest(RowRequestInfo &request, int32_t table_id, int32_t row_id);

  // Get a list of app thread ids that can be satisfied with this reply.
  // Corresponding row requests are removed upon returning.
  // If all row requests prior to some version are removed, those OpLogs are
  // removed as well.
  int32_t InformReply(int32_t table_id, int32_t row_id, int32_t clock,
                      uint32_t curr_version,
                      std::vector<int32_t> *app_thread_ids);
private:
  // map <table_id, row_id> to a list of requests
  // The list is in increasing order of clock.
  std::map<std::pair<int32_t, int32_t>, std::list<RowRequestInfo>>
      pending_row_requests_;
};

} // namespace petuum

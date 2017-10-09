#include <petuum_ps/thread/row_request_oplog_mgr.hpp>
#include <utility>
#include <glog/logging.h>
#include <list>
#include <vector>

namespace petuum {

/**
 * @brief Determines if a a row request has to be forwarded or not. A request
 * is forwarded only the there exists no other requests for the table and row
 * id with a lower clock.
 *
 * If such a request exists the, the current request is queued.
 */
bool SSPRowRequestOpLogMgr::AddRowRequest(RowRequestInfo &request_info,
        int32_t table_id, int32_t row_id) {

  request_info.sent = true;

  std::pair<int32_t, int32_t> request_key(table_id, row_id);

  // create an entry if none present
  if (pending_row_requests_.count(request_key) == 0) {
    pending_row_requests_.insert(
        std::make_pair(request_key, std::list<RowRequestInfo>()));
  }

  std::list<RowRequestInfo> &request_list = pending_row_requests_[request_key];

  bool request_info_added = false;

  // Requests are sorted in increasing order of clock number.  When a request
  // is to be inserted, start from the end as the request's clock is more
  // likely to be larger.

  for (auto iter = request_list.end(); iter != request_list.begin(); iter--) {
    auto iter_prev = std::prev(iter);
    int32_t request_clock = request_info.clock;

    if (request_clock >= iter_prev->clock) {
      // insert between iter_prev and iter
      request_info.sent = false;
      request_list.insert(iter, request_info);
      request_info_added = true;
      break;
    }
  }

  // push to the head if not added, this request has to be sent since there are
  // no other requests with a lower clock.
  if (!request_info_added) {
    request_list.push_front(request_info);
  }

  // if there is an earlier clock request the current request is not sent
  return request_info.sent;
}

/**
 * 1. Find out all possible app thread ids that can be satisfied with the
 * latest clocked update.
 * 2. Figure out if there are any pending requests, and return the minimum
 * clock among all pending requests.
 * 3. If a request is satisfied with an update with version "curr_version",
 * will out oplogs for all old versions.
 * 4. For those request, that have not been sent, update the version number
 * with which they should be replied to current version of the bg thread.
 */

int32_t SSPRowRequestOpLogMgr::InformReply(int32_t table_id,
        int32_t row_id, int32_t reply_clock, uint32_t curr_version, std::vector<int32_t> *app_thread_ids) {

  (*app_thread_ids).clear();

  std::pair<int32_t, int32_t> request_key(table_id, row_id);
  std::list<RowRequestInfo> &request_info_list = pending_row_requests_[request_key];

  int32_t clock_to_request = -1;

  while (!request_info_list.empty()) {

    RowRequestInfo &request_info = request_info_list.front();

    if (request_info.clock <= reply_clock) {
       // current server response satisfies the request remove the request
      app_thread_ids->push_back(request_info.app_thread_id);
      request_info_list.pop_front();

    } else {
      // there is a request for which the current update is not enough
      CHECK_EQ(request_info.sent, false);

      clock_to_request = request_info.clock;
      request_info.sent = true;

      // we break, because the requests are stored in sorted order of clock.
      break;
    }
  }

  // if there's no request in that list, remove the empty list
  if (request_info_list.empty()) {
    pending_row_requests_.erase(request_key);
  }
  return clock_to_request;
}

}

// author: jinliang

#include <petuum_ps/server/server.hpp>
#include <petuum_ps/server/serialized_oplog_reader.hpp>
#include <petuum_ps/util/class_register.hpp>
#include <petuum_ps/util/utils.hpp>

#include <climits>
#include <utility>
#include <fstream>
#include <map>

namespace petuum {

Server::Server() {}

Server::~Server() {}

/**
 * Each server thread has its own copy of Server object.
 */
void Server::Init(int32_t server_id, const std::vector<int32_t> &bg_ids,
                  bool is_replica) {
  // bg_clock is vector clock. We set it to be zero for all bg threads (one
  // from each worker client)
  for (auto bg : bg_ids) {
    // TODO(raajay) is there an easier way to copy vectors?
    bg_ids_.push_back(bg);
  }
  server_id_ = server_id;
  accum_oplog_count_ = 0;
  is_replica_ = is_replica;
  from_start_timer_.restart();
}

/**
 */
void Server::CreateTable(int32_t table_id, TableInfo &table_info) {
  // Each Server object is responsible of different parts of all tables. Thus,
  // a copy of all tables is maintained.
  auto ret = tables_.emplace(table_id, ServerTable(table_info));
  CHECK(ret.second);

  if (GlobalContext::get_resume_clock() > 0) {
    TableIter table_iter = tables_.find(table_id);
    table_iter->second.ReadSnapShot(GlobalContext::get_resume_dir(), server_id_,
                                    table_id,
                                    GlobalContext::get_resume_clock());
  }

  auto ret1 = table_vector_clock_.emplace(table_id, VectorClock());
  CHECK(ret1.second);

  // Initialize the vector clocks & per table bg version
  TableClockIter iter = table_vector_clock_.find(table_id);
  for (auto bg : bg_ids_) {
    iter->second.AddClock(bg, std::max(GlobalContext::get_resume_clock(), 0));
    table_bg_version_.Set(table_id, bg, -1);
  }
}

/**
 * Find a row indexed by table and row id. If no row exists, create a new row
 * and return.
 */
ServerRow *Server::FindCreateRow(int32_t table_id, int32_t row_id) {
  // access ServerTable via reference to avoid copying
  auto iter = tables_.find(table_id);
  CHECK(iter != tables_.end());
  return iter->second.FindCreateRow(row_id);
}

/**
 * Push the vector clock for a particular bg_id to the specified value. On
 * pushing the clock of a single bg_thread, if the min_value of th vector
 * clock changes, then return true. Also, see if we have to take a snapshot.
 */
bool Server::ClockAllTablesUntil(int32_t bg_id, int32_t clock) {
    bool did_overall_clock_move = false;
    // XXX(raajay) We move the overall clock if atleast one of the tables'
    // clock moves ahead. As long as tables are clocked together, this
    // assumptions should be fine.
    for(auto &it : table_vector_clock_) {
        bool did_table_clock_move = ClockTableUntil(it.first, bg_id, clock);
        did_overall_clock_move |= did_table_clock_move;
    }
    return did_overall_clock_move;
}

/**
 * Update an internal data structure to cache all row requests. One row
 * requests are cached, they are replied to only when the clock moves on an
 * update.
 */
void Server::AddRowRequest(int32_t bg_id, int32_t table_id, int32_t row_id,
                           int32_t clock) {

  ServerRowRequest server_row_request;
  server_row_request.bg_id = bg_id;
  server_row_request.table_id = table_id;
  server_row_request.row_id = row_id;
  server_row_request.clock = clock;

  if (clock_bg_row_requests_.count(clock) == 0) {
    clock_bg_row_requests_.insert(
        std::make_pair(clock, boost::unordered::unordered_map<
                                  int32_t, std::vector<ServerRowRequest>>()));
  }
  if (clock_bg_row_requests_[clock].count(bg_id) == 0) {
    clock_bg_row_requests_[clock].insert(
        std::make_pair(bg_id, std::vector<ServerRowRequest>()));
  }
  clock_bg_row_requests_[clock][bg_id].push_back(server_row_request);
}

/**
 * Look at the cache of row request, and return those that are satisfied upon
 * the clock moving.
 */
void Server::GetFulfilledRowRequests(int32_t clock, int32_t table_id,
        std::vector<ServerRowRequest> *requests) {
  requests->clear();

  auto it1 = clock_bg_row_requests_.find(clock);
  if (it1 == clock_bg_row_requests_.end()) { return; }

  for (auto &it2 : it1->second) {
      if (table_id != -1 && table_id != it2.first) {
          continue;
      }
      requests->insert(requests->end(), it2.second.begin(), it2.second.end());
  }

  if (table_id == -1) {
      // erase all requests for a clock
    clock_bg_row_requests_.erase(clock);
  } else {
      // erase only requests for specific table id
    it1->second.erase(table_id);
  }
}

/**
 * (raajay) This is a key function, one where the internal tables are updated
 * with values sent from the client. It is important to note that the client
 * message will contain updates to all the tables.
 */
int32_t Server::ApplyOpLogUpdateVersion(int32_t oplog_table_id, const void *oplog, size_t oplog_size,
                                     int32_t bg_id, uint32_t version,
                                     int32_t *observed_delay) {

    CheckUpdateVersion(oplog_table_id, bg_id, version);

  if (0 == oplog_size) { return 0; }

  SerializedOpLogReader oplog_reader(oplog, tables_);
  if (false == oplog_reader.Restart()) {
      return 0;
  }

  int32_t num_tables_updated = 1;

  *observed_delay = -1;

  int32_t table_id;
  int32_t row_id;
  int32_t model_version_for_update;
  const int32_t *column_ids;
  int32_t num_updates;
  bool started_new_table;

  // read the first few bytes of the message. It will populate the arguments.
  const void *updates =
      oplog_reader.Next(&table_id, &row_id, &model_version_for_update,
                        &column_ids, &num_updates, &started_new_table);
  CHECK_EQ(started_new_table, false);

  ServerTable *server_table;
  if (updates != 0) {
    server_table = GetServerTable(table_id);
  }

  while (updates != 0) {
    ++accum_oplog_count_;

    // TODO (raajay) use delayed based scaling.
    // 1. We have to decide if the scaling has to be determined per-row or on a
    // table basis.

    server_table->FindCreateRow(row_id);
    bool success = server_table->ApplyRowOpLog(row_id, column_ids, updates,
                                               num_updates, 1.0);
    CHECK_EQ(success, true) << "Row not found. "
                            << GetTableRowStringId(table_id, row_id);

    // get the next row update
    updates = oplog_reader.Next(&table_id, &row_id, &model_version_for_update,
                                &column_ids, &num_updates, &started_new_table);

    if (updates == 0) { break; }

    if (started_new_table) {
      server_table = GetServerTable(table_id);
      num_tables_updated++;
    }
  }

  // validations
  CHECK_EQ(oplog_reader.GetCurrentOffset(), oplog_size);
  if (oplog_table_id != ALL_TABLES) {
    CHECK_EQ(num_tables_updated, 1);
  }
  return num_tables_updated;
}

/**
 * Returns the least clock among all tables.
 */
int32_t Server::GetAllTablesMinClock() {
    int32_t min_table_clock = INT_MAX;
    for (auto &it : table_vector_clock_) {
        min_table_clock = std::min(min_table_clock, it.second.get_min_clock());
    }
    return min_table_clock;
}

/**
 * Returns the minimum bg version across all tables.
 */
int32_t Server::GetAllTableBgVersion(int32_t bg_thread_id) {
    int32_t all_table_bg_version  = INT_MAX;
    for(auto table_id : table_bg_version_.GetDim1Keys()) {
        all_table_bg_version = std::min(all_table_bg_version,
                table_bg_version_.Get(table_id, bg_thread_id));
    }
    CHECK_NE(all_table_bg_version, INT_MAX);
    return all_table_bg_version;
}

/**
 * Returns the current version of update applied for table_id from bg_id
 */
int32_t Server::GetTableBgVersion(int32_t table_id, int32_t bg_id) {
    return table_bg_version_.Get(table_id, bg_id);
}

/**
 * Set the latest version seen from a worker for all tables
 */
void Server::SetAllTableBgVersion(int32_t bg_id, uint32_t version) {
    for(auto table_id : table_bg_version_.GetDim1Keys()) {
        table_bg_version_.Set(table_id, bg_id, version);
    }
}

/**
 *
 */
void Server::SetTableBgVersion(int32_t table_id, int32_t bg_id, uint32_t version) {
    table_bg_version_.Set(table_id, bg_id, version);
}

/**
 */
double Server::GetElapsedTime() { return from_start_timer_.elapsed(); }

/**
 */
ServerTable *Server::GetServerTable(int32_t table_id) {
  auto table_iter = tables_.find(table_id);
  CHECK(table_iter != tables_.end()) << "Not found table_id = " << table_id;
  return &(table_iter->second);
}

/**
 */
void Server::TakeSnapShot(int32_t current_clock) {
  for (auto table_iter = tables_.begin(); table_iter != tables_.end();
       table_iter++) {
    table_iter->second.TakeSnapShot(GlobalContext::get_snapshot_dir(),
                                    server_id_, table_iter->first,
                                    current_clock);
  }
}

/**
 * Clock a single table.
 */
bool Server::ClockTableUntil(int32_t table_id, int32_t bg_id, int32_t clock) {
    VLOG(20) << "Clock table=" << table_id << " from bg_id=" << bg_id <<  " until " << clock;
  TableClockIter iter = table_vector_clock_.find(table_id);
  return (0 != iter->second.TickUntil(bg_id, clock));
}

/**
 * Return the min clock for an individual table
 */
int32_t Server::GetTableMinClock(int32_t table_id) {
  TableClockIter iter = table_vector_clock_.find(table_id);
  return iter->second.get_min_clock();
}

/**
 */
std::string Server::DisplayClock() {
    std::stringstream ss;
    ss << "Min clock=" << GetAllTablesMinClock() << " [";
    for (auto &it : table_vector_clock_) {
        ss << it.first << " : " << it.second.get_min_clock() << ", ";
    }
    ss << "]";
    return ss.str();
}

/**
 * Check if the version of the Oplog is valid before updating it.
 */
void Server::CheckUpdateVersion(int32_t table_id, int32_t bg_id, uint32_t version) {
    if (is_replica_) {
        return;
    }

    if (table_id != ALL_TABLES) {
        CHECK_EQ(GetTableBgVersion(table_id, bg_id) + 1, version);
        SetTableBgVersion(table_id, bg_id, version);
    } else {
        CHECK_EQ(GetAllTableBgVersion(bg_id) + 1, version);
        SetAllTableBgVersion(bg_id, version);
    }
}

}

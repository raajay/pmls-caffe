#include <petuum_ps/thread/ssp_bg_worker.hpp>
#include <petuum_ps/client/ssp_client_row.hpp>

namespace petuum {

SSPBgWorker::SSPBgWorker(int32_t id, int32_t comm_channel_idx,
                         std::map<int32_t, ClientTable *> *tables,
                         pthread_barrier_t *init_barrier,
                         pthread_barrier_t *create_table_barrier)
    : AbstractBgWorker(id, comm_channel_idx, tables, init_barrier,
                       create_table_barrier) {}

SSPBgWorker::~SSPBgWorker() { delete row_request_oplog_mgr_; }

void SSPBgWorker::CreateRowRequestOpLogMgr() {
  row_request_oplog_mgr_ = new SSPRowRequestOpLogMgr;
  VLOG(5) << "Create an OpLogMgr in BgWorker with (id=" << my_id_
          << ", comm_channel_idx=" << my_comm_channel_idx_ << ")";
}

bool SSPBgWorker::GetRowOpLog(AbstractOpLog &table_oplog, int32_t row_id,
                              AbstractRowOpLog **row_oplog_ptr) {
  return table_oplog.GetEraseOpLog(row_id, row_oplog_ptr);
}

long SSPBgWorker::ResetBgIdleMilli() { return 0; }

long SSPBgWorker::BgIdleWork() { return 0; }

ClientRow *SSPBgWorker::CreateClientRow(int32_t clock, int32_t global_version,
                                        AbstractRow *row_data) {
  return reinterpret_cast<ClientRow *>(
      new SSPClientRow(clock, global_version, row_data, true));
}


/**
 */
BgOpLog *SSPBgWorker::PrepareOpLogs(int32_t table_id) {

  auto *bg_oplog = new BgOpLog;
  ephemeral_server_table_size_counter_.Reset();
  ephemeral_server_oplog_msg_.Reset();

  // Prepare oplog for each table and add it to BgOpLog
  for (const auto &table_pair : (*tables_)) {
    int32_t curr_table_id = table_pair.first;

    if (table_id != -1 && table_id != curr_table_id) {
      continue;
    }

    auto oplog_type = table_pair.second->get_oplog_type();
    if(oplog_type != Sparse && oplog_type != Dense) {
      LOG(FATAL) << "Unknown oplog type = "
                 << table_pair.second->get_oplog_type();
    }

    // Reset the per-table data structure collecting per-table message size stats
    ephemeral_server_byte_counter_.Reset();

    // we add each table's oplog to the overall oplog
    bg_oplog->Add(curr_table_id, PrepareTableOpLogs(curr_table_id, table_pair.second));

    FinalizeTableOplogSize(curr_table_id);
  }
  return bg_oplog;
}


/**
 */
BgOpLogPartition *SSPBgWorker::PrepareTableOpLogs(int32_t table_id, ClientTable *table) {

  // Get OpLog index. The index will tell which rows have been modified. So the
  // function below, will query an oplog index -- maintained per table at the
  // process level -- and get all the rows that have been modified. Note that
  // it will find modified rows from among the rows for each table that the
  // current bg_thread is responsible for.

  cuckoohash_map<int32_t, bool> *new_table_oplog_index_ptr =
      table->GetAndResetOpLogIndex(my_comm_channel_idx_);

  // create an empty partition
  auto *bg_table_oplog = new BgOpLogPartition(table_id,
          table->get_sample_row()->get_update_size(), my_comm_channel_idx_);

  // iterate over all rows that are potentially modified
  for (auto oplog_index_iter = new_table_oplog_index_ptr->cbegin();
       !oplog_index_iter.is_end(); oplog_index_iter++) {

    int32_t row_id = oplog_index_iter->first;

    AbstractRowOpLog *row_oplog = nullptr;
    bool found = GetRowOpLog(table->get_oplog(), row_id, &row_oplog);

    if (!found || row_oplog == nullptr) {
      continue;
    }

    // get the size of the row depending on the oplog serialization type for the table
    size_t oplog_size = (table->oplog_dense_serialized()) ?
                             row_oplog->GetDenseSerializedSize() :
                             row_oplog->GetSparseSerializedSize();

    // 1. row id, 2. global version of the row, 3. serialized row size
    size_t serialized_size = sizeof(int32_t) + sizeof(int32_t) + oplog_size;
    int32_t server_id = GlobalContext::GetPartitionServerID(row_id, my_comm_channel_idx_);
    ephemeral_server_byte_counter_.Increment(server_id, serialized_size);
    bg_table_oplog->InsertOpLog(row_id, row_oplog);
  }

  // no one else points to this struct, see GetAndResetOpLogIndex function
  delete new_table_oplog_index_ptr;
  return bg_table_oplog;
}



void SSPBgWorker::TrackBgOpLog(int32_t table_id, BgOpLog *bg_oplog) {
  bool tracked = row_request_oplog_mgr_->AddOpLog(GetUpdateVersion(table_id), bg_oplog);

  IncrementUpdateVersion(table_id);
  // the below function does nothing.
  row_request_oplog_mgr_->InformVersionInc();

  if (!tracked) {
    delete bg_oplog;
  }
}
}

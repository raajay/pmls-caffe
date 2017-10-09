// author: jinliang

#pragma once

#include <stdint.h>
#include <map>
#include <vector>
#include <condition_variable>
#include <boost/unordered_map.hpp>

#include <petuum_ps/thread/abstract_bg_worker.hpp>
#include <petuum_ps/util/thread.hpp>
#include <petuum_ps/thread/row_request_oplog_mgr.hpp>
#include <petuum_ps/thread/bg_oplog.hpp>
#include <petuum_ps/thread/ps_msgs.hpp>
#include <petuum_ps/comm_bus/comm_bus.hpp>
#include <petuum_ps/include/configs.hpp>
#include <petuum_ps/client/client_table.hpp>

namespace petuum {
class SSPBgWorker : public AbstractBgWorker {
public:
  SSPBgWorker(int32_t id, int32_t comm_channel_idx,
              std::map<int32_t, ClientTable *> *tables,
              pthread_barrier_t *init_barrier,
              pthread_barrier_t *create_table_barrier);
  virtual ~SSPBgWorker();

protected:
  void CreateRowRequestOpLogMgr() override;

  virtual bool GetRowOpLog(AbstractOpLog &table_oplog, int32_t row_id,
                           AbstractRowOpLog **row_oplog_ptr);

  long ResetBgIdleMilli() override;
  long BgIdleWork() override;
  /* Functions Called From Main Loop -- END */

  ClientRow *CreateClientRow(int32_t clock, int32_t global_version,
                             AbstractRow *row_data) override;

  /* Handles Sending OpLogs -- BEGIN */
  BgOpLog *PrepareOpLogs(int32_t table_id) override;
  void TrackBgOpLog(int32_t table_id, BgOpLog *bg_oplog) override;
  virtual BgOpLogPartition *PrepareTableOpLogs(int32_t table_id,
                                               ClientTable *table);
  /* Handles Sending OpLogs -- END */

};

}

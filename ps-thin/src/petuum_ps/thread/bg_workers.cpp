#include <petuum_ps/thread/bg_workers.hpp>
#include <petuum_ps/include/configs.hpp>
#include <petuum_ps/thread/bg_worker_group.hpp>

namespace petuum {
BgWorkerGroup *BgWorkers::bg_worker_group_;

void BgWorkers::Start(std::map<int32_t, ClientTable *> *tables) {
  ConsistencyModel consistency_model = GlobalContext::get_consistency_model();
  switch (consistency_model) {
  case SSP:
    bg_worker_group_ = new BgWorkerGroup(tables);
    break;
  default:
    LOG(FATAL) << "Unknown consistency model = " << consistency_model;
  }
  bg_worker_group_->Start();
}

void BgWorkers::ShutDown() {
  bg_worker_group_->ShutDown();
  delete bg_worker_group_;
}

void BgWorkers::AppThreadRegister() { bg_worker_group_->AppThreadRegister(); }

void BgWorkers::AppThreadDeregister() {
  bg_worker_group_->AppThreadDeregister();
}

void BgWorkers::SyncThreadRegister() { bg_worker_group_->SyncThreadRegister(); }

void BgWorkers::SyncThreadDeregister() {
  bg_worker_group_->SyncThreadDeregister();
}

bool BgWorkers::CreateTable(int32_t table_id,
                            const ClientTableConfig &table_config) {
  return bg_worker_group_->CreateTable(table_id, table_config);
}

void BgWorkers::WaitCreateTable() { bg_worker_group_->WaitCreateTable(); }

bool BgWorkers::RequestRow(int32_t table_id, int32_t row_id, int32_t clock) {
  return bg_worker_group_->RequestRow(table_id, row_id, clock);
}

void BgWorkers::RequestRowAsync(int32_t table_id, int32_t row_id, int32_t clock,
                                bool forced) {
  return bg_worker_group_->RequestRowAsync(table_id, row_id, clock, forced);
}

void BgWorkers::GetAsyncRowRequestReply() {
  return bg_worker_group_->GetAsyncRowRequestReply();
}

void BgWorkers::ClockAllTables() { bg_worker_group_->ClockAllTables(); }

void BgWorkers::ClockTable(int32_t table_id) {
  bg_worker_group_->ClockTable(table_id);
}

void BgWorkers::SendOpLogsAllTables() {
  bg_worker_group_->SendOpLogsAllTables();
}

int32_t BgWorkers::GetSystemClock() {
  return bg_worker_group_->GetSystemClock();
}

void BgWorkers::WaitSystemClock(int32_t my_clock) {
  bg_worker_group_->WaitSystemClock(my_clock);
}
}

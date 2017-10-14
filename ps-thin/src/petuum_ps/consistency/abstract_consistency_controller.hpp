#pragma once

#include <petuum_ps/storage/abstract_process_storage.hpp>
#include <petuum_ps/include/configs.hpp>
#include <petuum_ps/include/abstract_row.hpp>
#include <petuum_ps/util/vector_clock_mt.hpp>

#include <boost/utility.hpp>
#include <cstdint>
#include <vector>

namespace petuum {

// Interface for consistency controller modules. For each table we associate a
// consistency controller (e.g., SSPController) that's essentially the "brain"
// that maintains a prescribed consistency policy upon each table action. All
// functions should be fully thread-safe.
class AbstractConsistencyController : boost::noncopyable {
public:
  // Controller modules rely on TableInfo to specify policy parameters (e.g.,
  // staleness in SSP). Does not take ownership of any pointer here.
  AbstractConsistencyController(int32_t table_id,
                                AbstractProcessStorage &process_storage,
                                const AbstractRow *sample_row)
      : process_storage_(process_storage),
        table_id_(table_id),
        sample_row_(sample_row),
        latest_row_version_(0) {}

  virtual ~AbstractConsistencyController() {}

  virtual void GetAsyncForced(int32_t row_id) = 0;
  virtual void GetAsync(int32_t row_id) = 0;
  virtual void WaitPendingAsnycGet() = 0;

  // Read a row in the table and is blocked until a valid row is obtained
  // (e.g., from server). A row is valid if, for example, it is sufficiently
  // fresh in SSP. The result is returned in row_accessor.
  virtual ClientRow *Get(int32_t row_id, RowAccessor *row_accessor,
                         int32_t clock) = 0;

  // Increment (update) an entry. Does not take ownership of input argument
  // delta, which should be of template type UPDATE in Table. This may trigger
  // synchronization (e.g., in value-bound) and is blocked until consistency
  // is ensured.
  virtual void Inc(int32_t row_id, int32_t column_id, const void *delta) = 0;

  // Increment column_ids.size() entries of a row. deltas points to an array.
  virtual void BatchInc(int32_t row_id, const int32_t *column_ids,
                        const void *updates, int32_t num_updates,
                        int32_t global_version = -1) = 0;

  // Increment a row with dense updates, i.e., all column ids
  virtual void DenseBatchInc(int32_t row_id, const void *updates,
          int32_t index_st, int32_t num_updates) = 0;

  virtual void FlushThreadCache() = 0;

  virtual void Clock() = 0;

protected:

  /**
   * @brief Not that this function will only be called from the application
   * threads (i.e., those that invoke the Get and BatchInc functions). In its
   * use with Caffe, we are guaranteed that each table at given point in time
   * is accessed by only one thread. Hence, updating the
   * latest_row_version_ state without explicit locking is okay.
   * XXX (raajay)
   */
  void UpdateLatestReadRowVersion(ClientRow *row) {
      latest_row_version_ = std::max(latest_row_version_,
              row->GetGlobalVersion());
  }

  // Process cache, highly concurrent.
  AbstractProcessStorage &process_storage_;
  int32_t table_id_;
  // We use sample_row_.AddUpdates(), SubstractUpdates() as static method.
  const AbstractRow *sample_row_;
  int32_t latest_row_version_;
};

} // namespace petuum

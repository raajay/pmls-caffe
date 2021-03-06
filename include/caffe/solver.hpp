#ifndef CAFFE_OPTIMIZATION_SOLVER_HPP_
#define CAFFE_OPTIMIZATION_SOLVER_HPP_

#include <string>
#include <vector>
#include <map>
#include <fstream>

#ifdef USE_PS_THIN
#include <petuum_ps/include/petuum_ps.hpp>
#else
#include <petuum_ps_common/include/petuum_ps.hpp>
#endif

#include "caffe/net.hpp"

namespace caffe {

/**
 * @brief An interface for classes that perform optimization on Net%s.
 *
 * Requires implementation of ComputeUpdateValue to compute a parameter update
 * given the current state of the Net parameters.
 */
template <typename Dtype> class Solver {
public:
  explicit Solver(const SolverParameter &param,
                  const map<string, vector<int>> *layer_blobs_global_idx_ptr,
                  const int thread_id);
  explicit Solver(const string &param_file,
                  const map<string, vector<int>> *layer_blobs_global_idx_ptr,
                  const int thread_id);
  void Init(const SolverParameter &param);
  void InitTrainNet();
  void InitTestNets();
  // The main entry of the solver function. In default, iter will be zero. Pass
  // in a non-zero iter number to resume training for a pre-trained net.
  virtual void Solve(const char *resume_file = NULL);
  inline void Solve(const string resume_file) { Solve(resume_file.c_str()); }
  virtual ~Solver() {}
  inline shared_ptr<Net<Dtype>> net() { return net_; }
  inline const vector<shared_ptr<Net<Dtype>>> &test_nets() {
    return test_nets_;
  }

  void PrintNetOutputs(const string &filename);

protected:
  // PreSolve is run before any solving iteration starts, allowing one to
  // put up some scaffold.
  virtual void PreSolve() {}
  virtual void InitSVB();
  virtual Dtype ForwardBackward(const vector<Blob<Dtype> *> &bottom);
  // Get the update value for the current iteration.
  virtual void ComputeUpdateValue(const int param_id) = 0;
  virtual void ComputeUpdateValue() = 0;
  virtual void ThreadSyncWithPS(const shared_ptr<Blob<Dtype>> &param,
                                const int param_id, const int param_owner,
                                const int clock
#ifdef USE_PS_THIN
                                ,
                                const int offset
#endif
                                );
  virtual void ThreadSyncWithSVB(const shared_ptr<Blob<Dtype>> &param,
                                 const int param_id,
                                 const shared_ptr<Layer<Dtype>> &layer,
                                 const int layer_id,
                                 const vector<Blob<Dtype> *> &top,
                                 const vector<Blob<Dtype> *> &bottom);
  virtual void JoinSyncThreads();

  // The Solver::Snapshot function implements the basic snapshotting utility
  // that stores the learned net. You should implement the SnapshotSolverState()
  // function that produces a SolverState protocol buffer that needs to be
  // written to disk together with the learned net.
  void Snapshot();
  // The test routine
  void TestAll();
  void Test(const int test_net_id = 0);
  virtual void SnapshotSolverState(SolverState *state) = 0;
  // The Restore function implements how one should restore the solver to a
  // previously snapshotted state. You should implement the RestoreSolverState()
  // function that restores the state from a SolverState protocol buffer.
  void Restore(const char *resume_file);
  virtual void RestoreSolverState(const SolverState &state) = 0;
  void DisplayOutputBlobs(const int net_id);
  void PrintOutputBlobs(shared_ptr<Net<Dtype>> &net, const bool trian,
                        std::ofstream &outfile);

  SolverParameter param_;
  int iter_;
  shared_ptr<Net<Dtype>> net_;
  vector<shared_ptr<Net<Dtype>>> test_nets_;

  int display_counter_;
  int test_counter_;
  int clock_counter_;
  int param_table_staleness_;
  // layer/net_name => vector of blobs' global indexes
  const map<string, vector<int>> *layer_blobs_global_idx_ptr_;

  vector<std::thread *> sync_threads_;
  int max_local_sv_updates_;
  int max_remote_sv_updates_;

  const int thread_id_;
  int client_id_;
  int num_threads_;
  int num_clients_;

  petuum::HighResolutionTimer total_timer_;

  DISABLE_COPY_AND_ASSIGN(Solver);
};

/**
 * @brief Optimizes the parameters of a Net using
 *        stochastic gradient descent (SGD) with momentum.
 */
template <typename Dtype> class SGDSolver : public Solver<Dtype> {
public:
  explicit SGDSolver(const SolverParameter &param,
                     const map<string, vector<int>> *layer_blobs_global_idx_ptr,
                     const int thread_id)
      : Solver<Dtype>(param, layer_blobs_global_idx_ptr, thread_id) {}
  explicit SGDSolver(const string &param_file,
                     const map<string, vector<int>> *layer_blobs_global_idx_ptr,
                     const int thread_id)
      : Solver<Dtype>(param_file, layer_blobs_global_idx_ptr, thread_id) {}

  const vector<shared_ptr<Blob<Dtype>>> &history() { return history_; }

protected:
  virtual void PreSolve();
  Dtype GetLearningRate();
  virtual void ComputeUpdateValue();
  virtual void ComputeUpdateValue(const int param_id);
  virtual void SnapshotSolverState(SolverState *state);
  virtual void RestoreSolverState(const SolverState &state);
  // history maintains the historical momentum data.
  // update maintains update related data and is not needed in snapshots.
  // temp maintains other information that might be needed in computation
  //   of gradients/updates and is not needed in snapshots
  vector<shared_ptr<Blob<Dtype>>> history_, update_, temp_;

  DISABLE_COPY_AND_ASSIGN(SGDSolver);
};

template <typename Dtype> class NesterovSolver : public SGDSolver<Dtype> {
public:
  explicit NesterovSolver(
      const SolverParameter &param,
      const map<string, vector<int>> *layer_blobs_global_idx_ptr,
      const int thread_id)
      : SGDSolver<Dtype>(param, layer_blobs_global_idx_ptr, thread_id) {}
  explicit NesterovSolver(
      const string &param_file,
      const map<string, vector<int>> *layer_blobs_global_idx_ptr,
      const int thread_id)
      : SGDSolver<Dtype>(param_file, layer_blobs_global_idx_ptr, thread_id) {}

protected:
  virtual void ComputeUpdateValue(const int param_id);
  virtual void ComputeUpdateValue();

  DISABLE_COPY_AND_ASSIGN(NesterovSolver);
};

template <typename Dtype> class AdaGradSolver : public SGDSolver<Dtype> {
public:
  explicit AdaGradSolver(
      const SolverParameter &param,
      const map<string, vector<int>> *layer_blobs_global_idx_ptr,
      const int thread_id)
      : SGDSolver<Dtype>(param, layer_blobs_global_idx_ptr, thread_id) {
    constructor_sanity_check();
  }
  explicit AdaGradSolver(
      const string &param_file,
      const map<string, vector<int>> *layer_blobs_global_idx_ptr,
      const int thread_id)
      : SGDSolver<Dtype>(param_file, layer_blobs_global_idx_ptr, thread_id) {
    constructor_sanity_check();
  }

protected:
  virtual void ComputeUpdateValue(const int param_id);
  virtual void ComputeUpdateValue();
  void constructor_sanity_check() {
    CHECK_EQ(0, this->param_.momentum())
        << "Momentum cannot be used with AdaGrad.";
  }

  DISABLE_COPY_AND_ASSIGN(AdaGradSolver);
};

template <typename Dtype>
Solver<Dtype> *
GetSolver(const SolverParameter &param,
          const map<string, vector<int>> *layer_blobs_global_idx_ptr,
          const int thread_id) {
  SolverParameter_SolverType type = param.solver_type();

  switch (type) {
  case SolverParameter_SolverType_SGD:
    return new SGDSolver<Dtype>(param, layer_blobs_global_idx_ptr, thread_id);
  case SolverParameter_SolverType_NESTEROV:
    return new NesterovSolver<Dtype>(param, layer_blobs_global_idx_ptr,
                                     thread_id);
  case SolverParameter_SolverType_ADAGRAD:
    return new AdaGradSolver<Dtype>(param, layer_blobs_global_idx_ptr,
                                    thread_id);
  default:
    LOG(FATAL) << "Unknown SolverType: " << type;
  }
  return (Solver<Dtype> *)NULL;
}

} // namespace caffe

#endif // CAFFE_OPTIMIZATION_SOLVER_HPP_

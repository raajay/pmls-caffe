#pragma once

#include <pthread.h>
#include <petuum_ps/util/pthread_barrier.hpp>
#include <petuum_ps/comm_bus/comm_bus.hpp>
#include <petuum_ps/comm_bus/zmq_util.hpp>
#include <petuum_ps/thread/ps_msgs.hpp>
#include <petuum_ps/thread/context.hpp>

namespace petuum {
class Thread {
public:
  Thread(int32_t my_id, pthread_barrier_t *init_barrier);

  virtual ~Thread() = default;

  virtual void *operator()() { return nullptr; }

  int Start() {
    InitWhenStart();
    // From the web: The pthread_create() function is used to create a new
    // thread,
    // with attributes specified by attr, within a process. If attr is NULL, the
    // default attributes are used. If the attributes specified by attr are
    // modified
    // later, the thread's attributes are not affected. Upon successful
    // completion,
    // pthread_create() stores the ID of the created thread in the location
    // referenced
    // by thread.  The thread is created executing start_routine with arg as its
    // sole
    // argument.
    return pthread_create(&thr_, nullptr, InternalStart, this);
  }

  void Join() { pthread_join(thr_, nullptr); }

protected:
  virtual void InitWhenStart() {}

  int32_t my_id_;
  pthread_barrier_t *init_barrier_;
  CommBus *const comm_bus_;
  // TODO(raajay) Move the foll. DS to base class
  // 1. my_id_
  // 2. init_barrier_

  /**
   */
  void SetupCommBus(int32_t thread_id);

  /**
   */
  void SendToAll(MsgBase *msg, std::vector<int32_t> dest_ids);

  /**
   */
  void Send(MsgBase *msg, int32_t dest_id);

  /**
   */
  void ConnectTo(int32_t dest_id, int32_t my_id);

  /**
   */
  int32_t WaitForConnect();

  /**
   */
  void WaitForReply(int32_t dest_id, MsgType msg_type);

  /**
   */
  int32_t GetNextMessage(zmq::message_t &msg);

private:
  static void *InternalStart(void *thread) {
    Thread *t = reinterpret_cast<Thread *>(thread);
    return t->operator()();
  }

  pthread_t thr_;
};
}

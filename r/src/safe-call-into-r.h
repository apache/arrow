
#ifndef SAFE_CALL_INTO_R_INCLUDED
#define SAFE_CALL_INTO_R_INCLUDED

#include "./arrow_types.h"

#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <unordered_set>

class MainRThreadTasks {
 public:
  class Task {
   public:
    virtual ~Task() {}
    virtual void run() {}
  };

  class EventLoop {
   public:
    EventLoop();
    ~EventLoop();
    std::thread::id thread() { return thread_; }

   private:
    std::thread::id thread_;
  };

  void Register(EventLoop* loop) { loop_ = loop; }

  void Unregister() { loop_ = nullptr; }

  EventLoop* Loop() { return loop_; }

 private:
  EventLoop* loop_;
};

void SafeCallIntoRBase(MainRThreadTasks::Task* task);

template <typename T>
T SafeCallIntoR(std::function<cpp11::sexp(void)> fun) {
  class TypedTask : public MainRThreadTasks::Task {
   public:
    TypedTask(std::function<cpp11::sexp(void)> fun) : fun_(fun){};

    void run() { result = cpp11::as_cpp<T>(fun_()); }

    T result;

   private:
    std::function<cpp11::sexp(void)> fun_;
  };

  TypedTask task(fun);
  SafeCallIntoRBase(&task);
  return task.result;
}

#endif

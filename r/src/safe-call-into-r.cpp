
#include "safe-call-into-r.h"
#include <functional>
#include <thread>

static MainRThreadTasks main_r_thread_tasks;

void SafeCallIntoRBase(MainRThreadTasks::Task* task) {
  if (main_r_thread_tasks.Loop() == nullptr) {
    throw std::runtime_error("Global R executor not registered");
  }

  if (main_r_thread_tasks.Loop()->thread() == std::this_thread::get_id()) {
    task->run();
  } else {
    throw std::runtime_error("Attempt to evaluate task on the non-R thread");
  }
}

MainRThreadTasks::EventLoop::EventLoop() {
  thread_ = std::this_thread::get_id();
  main_r_thread_tasks.Register(this);
}

MainRThreadTasks::EventLoop::~EventLoop() { main_r_thread_tasks.Unregister(); }

// [[arrow::export]]
cpp11::strings TestSafeCallIntoR(cpp11::list funs_that_return_a_string) {
  MainRThreadTasks::EventLoop loop;

  // pretending that this could be called from another thread
  std::vector<std::string> results;
  for (R_xlen_t i = 0; i < funs_that_return_a_string.size(); i++) {
    std::string result = SafeCallIntoR<std::string>([&]() {
        cpp11::function fun(funs_that_return_a_string[i]);
        return fun();
    });
    results.push_back(result);
  }

  // and then this would be back on the main thread just to make
  // sure the results are correct
  cpp11::writable::strings results_sexp;
  for (std::string& result : results) {
    results_sexp.push_back(result);
  }

  return results_sexp;
}

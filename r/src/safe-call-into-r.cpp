
#include "safe-call-into-r.h"
#include <thread>
#include <functional>

static MainRThreadTasks main_r_thread_tasks;

void SafeCallIntoRBase(MainRThreadTasks::Task* task) {
    main_r_thread_tasks.Add(task);
    while (!main_r_thread_tasks.is_finished(task)) {
        std::this_thread::yield();
        std::this_thread::sleep_for(std::chrono::nanoseconds(1000));
    }
}

MainRThreadTasks::EventLoop::EventLoop(): keep_looping_(false) {
    thread_ = std::this_thread::get_id();
    main_r_thread_tasks.Register(this);
}

MainRThreadTasks::EventLoop::~EventLoop() {
    main_r_thread_tasks.Unregister();
}

void MainRThreadTasks::EventLoop::start_looping() {
    // must be called from another thread but needs to evaluate this next
    // part on the main thread?
     while (keep_looping_) {
        try {
            main_r_thread_tasks.EvaluatePending();
        } catch (cpp11::unwind_exception& e) {
            // important that this this error makes its way back to the top
            // level and is caught in the auto-generated glue code
            throw e;
        }
        std::this_thread::sleep_for(std::chrono::nanoseconds(10000));
    }
}

void MainRThreadTasks::EventLoop::stop_looping() {
    keep_looping_ = false;
}

// [[arrow::export]]
cpp11::strings TestSafeCallIntoR(cpp11::list funs_that_return_a_string) {

    // pretending that this could be called from another thread
    std::vector<std::string> results;
    for (R_xlen_t i = 0; i < funs_that_return_a_string.size(); i++) {
        std::function<cpp11::sexp()> f = [&]() {
            cpp11::function fun (funs_that_return_a_string[i]);
            return fun();
        };

        std::string result = SafeCallIntoR<std::string>(f);
        results.push_back(result);
    }

    // and then this would be back on the main thread just to make
    // sure the results are correct
    cpp11::writable::strings results_sexp;
    for (std::string& result: results) {
        results_sexp.push_back(result);
    }

    return results_sexp;
}

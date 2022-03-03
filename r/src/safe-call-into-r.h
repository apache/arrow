
#ifndef SAFE_CALL_INTO_R_INCLUDED
#define SAFE_CALL_INTO_R_INCLUDED

#include "./arrow_types.h"

#include <deque>
#include <unordered_set>
#include <mutex>
#include <thread>
#include <functional>

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
        void start_looping();
        void stop_looping();
    private:
        std::thread::id thread_;
        bool keep_looping_;
    };

    void EvaluatePending() {
        lock_.lock();
        while (!tasks_.empty()) {
            Task* task = tasks_.back();
            tasks_.pop_back();
            task->run();
        }
        lock_.unlock();
    }

    void Add(Task* task) {
        lock_.lock();
        tasks_.push_front(task);
        lock_.unlock();
    }

    bool is_finished(Task* task) {
        lock_.lock();
        bool result = results_.find(task) != results_.end();
        lock_.unlock();
        return result;
    }

    void Register(EventLoop* loop) {
        if (loop_ == nullptr) {
            throw std::runtime_error("Event loop already registered");
        }
        loop_ = loop;
    }

    void Unregister() {
        loop_ = nullptr;
    }

private:
    std::deque<Task*> tasks_;
    std::unordered_set<Task*> results_;
    std::mutex lock_;
    EventLoop* loop_;
};


void SafeCallIntoRBase(MainRThreadTasks::Task* task);

template <typename T>
T SafeCallIntoR(std::function<cpp11::sexp(void)> fun) {
    class TypedTask: public MainRThreadTasks::Task {
    public:
        TypedTask(std::function<cpp11::sexp(void)> fun): fun_(fun) {};

        void run() {
            result = cpp11::as_cpp<T>(fun_());
        }

        T result;

    private:
        std::function<cpp11::sexp(void)> fun_;
    };

    TypedTask task(fun);
    // SafeCallIntoRBase(&task);
    task.run();
    return task.result;
}

#endif

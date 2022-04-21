#pragma once

#include <mutex>
#include <condition_variable>
#include <optional>
#include <vector>

#include <arrow/util/optional.h>

namespace arrow {
  namespace compute {

template <class T>
class concurrent_bounded_queue {
    size_t _remaining;
    std::vector<T> _buffer;
    mutable std::mutex _gate;
    std::condition_variable _not_full;
    std::condition_variable _not_empty;

    size_t _next_push=0;
    size_t _next_pop=0;
public:
    concurrent_bounded_queue(size_t capacity) : _remaining(capacity),_buffer(capacity) {
    }
    // Push new value to queue, waiting for capacity indefinitely.
    void push(const T &t) {
        std::unique_lock<std::mutex> lock(_gate);
        _not_full.wait(lock,[&]{return _remaining>0;});
        _buffer[_next_push++]=t;
        _next_push%=_buffer.size();
        --_remaining;
        _not_empty.notify_one();
    }
    // Get oldest value from queue, or wait indefinitely for it.
    T pop() {
        std::unique_lock<std::mutex> lock(_gate);
        _not_empty.wait(lock,[&]{return _remaining<_buffer.size();});
        T r=_buffer[_next_pop++];
        _next_pop%=_buffer.size();
        ++_remaining;
        _not_full.notify_one();
        return r;
    }
    // Try to pop the oldest value from the queue (or return nullopt if none)
    util::optional<T> try_pop() {
        std::unique_lock<std::mutex> lock(_gate);
        if(_remaining==_buffer.size()) return util::nullopt;
        T r=_buffer[_next_pop++];
        _next_pop%=_buffer.size();
        ++_remaining;
        _not_full.notify_one();
        return r;
    }

    // Test whether empty
    bool empty()const {
        std::unique_lock<std::mutex> lock(_gate);
        return _remaining==_buffer.size();
    }

    // Un-synchronized access to front
    // For this to be "safe":
    // 1) the caller logically guarantees that queue is not empty
    // 2) pop/try_pop cannot be called concurrently with this
    const T &unsync_front()const {
        return _buffer[_next_pop];
    }
};

  } // namespace compute
} // namespace arrow

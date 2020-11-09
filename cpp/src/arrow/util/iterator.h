// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cassert>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/compare.h"
#include "arrow/util/functional.h"
#include "arrow/util/future.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/mutex.h"
#include "arrow/util/optional.h"
#include "arrow/util/task_group.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/visibility.h"

namespace arrow {

template <typename T>
class Iterator;

template <typename T>
struct IterationTraits {
  /// \brief a reserved value which indicates the end of iteration. By
  /// default this is NULLPTR since most iterators yield pointer types.
  /// Specialize IterationTraits if different end semantics are required.
  static T End() { return T(NULLPTR); }
};

template <typename T>
struct IterationTraits<util::optional<T>> {
  /// \brief by default when iterating through a sequence of optional,
  /// nullopt indicates the end of iteration.
  /// Specialize IterationTraits if different end semantics are required.
  static util::optional<T> End() { return util::nullopt; }

  // TODO(bkietz) The range-for loop over Iterator<optional<T>> yields
  // Result<optional<T>> which is unnecessary (since only the unyielded end optional
  // is nullopt. Add IterationTraits::GetRangeElement() to handle this case
};

/// \brief A generic Iterator that can return errors
template <typename T>
class Iterator : public util::EqualityComparable<Iterator<T>> {
 public:
  /// \brief Iterator may be constructed from any type which has a member function
  /// with signature Status Next(T*);
  ///
  /// The argument is moved or copied to the heap and kept in a unique_ptr<void>. Only
  /// its destructor and its Next method (which are stored in function pointers) are
  /// referenced after construction.
  ///
  /// This approach is used to dodge MSVC linkage hell (ARROW-6244, ARROW-6558) when using
  /// an abstract template base class: instead of being inlined as usual for a template
  /// function the base's virtual destructor will be exported, leading to multiple
  /// definition errors when linking to any other TU where the base is instantiated.
  template <typename Wrapped>
  explicit Iterator(Wrapped has_next)
      : ptr_(new Wrapped(std::move(has_next)), Delete<Wrapped>), next_(Next<Wrapped>) {}

  Iterator() : ptr_(NULLPTR, [](void*) {}) {}

  /// \brief Return the next element of the sequence, IterationTraits<T>::End() when the
  /// iteration is completed. Calling this on a default constructed Iterator
  /// will result in undefined behavior.
  Result<T> Next() { return next_(ptr_.get()); }

  /// Pass each element of the sequence to a visitor. Will return any error status
  /// returned by the visitor, terminating iteration.
  template <typename Visitor>
  Status Visit(Visitor&& visitor) {
    const auto end = IterationTraits<T>::End();

    for (;;) {
      ARROW_ASSIGN_OR_RAISE(auto value, Next());

      if (value == end) break;

      ARROW_RETURN_NOT_OK(visitor(std::move(value)));
    }

    return Status::OK();
  }

  /// Iterators will only compare equal if they are both null.
  /// Equality comparability is required to make an Iterator of Iterators
  /// (to check for the end condition).
  bool Equals(const Iterator& other) const { return ptr_ == other.ptr_; }

  explicit operator bool() const { return ptr_ != NULLPTR; }

  class RangeIterator {
   public:
    RangeIterator() : value_(IterationTraits<T>::End()) {}

    explicit RangeIterator(Iterator i)
        : value_(IterationTraits<T>::End()),
          iterator_(std::make_shared<Iterator>(std::move(i))) {
      Next();
    }

    bool operator!=(const RangeIterator& other) const { return value_ != other.value_; }

    RangeIterator& operator++() {
      Next();
      return *this;
    }

    Result<T> operator*() {
      ARROW_RETURN_NOT_OK(value_.status());

      auto value = std::move(value_);
      value_ = IterationTraits<T>::End();
      return value;
    }

   private:
    void Next() {
      if (!value_.ok()) {
        value_ = IterationTraits<T>::End();
        return;
      }
      value_ = iterator_->Next();
    }

    Result<T> value_;
    std::shared_ptr<Iterator> iterator_;
  };

  RangeIterator begin() { return RangeIterator(std::move(*this)); }

  RangeIterator end() { return RangeIterator(); }

  /// \brief Move every element of this iterator into a vector.
  Result<std::vector<T>> ToVector() {
    std::vector<T> out;
    for (auto maybe_element : *this) {
      ARROW_ASSIGN_OR_RAISE(auto element, maybe_element);
      out.push_back(std::move(element));
    }
    // ARROW-8193: On gcc-4.8 without the explicit move it tries to use the
    // copy constructor, which may be deleted on the elements of type T
    return std::move(out);
  }

 private:
  /// Implementation of deleter for ptr_: Casts from void* to the wrapped type and
  /// deletes that.
  template <typename HasNext>
  static void Delete(void* ptr) {
    delete static_cast<HasNext*>(ptr);
  }

  /// Implementation of Next: Casts from void* to the wrapped type and invokes that
  /// type's Next member function.
  template <typename HasNext>
  static Result<T> Next(void* ptr) {
    return static_cast<HasNext*>(ptr)->Next();
  }

  /// ptr_ is a unique_ptr to void with a custom deleter: a function pointer which first
  /// casts from void* to a pointer to the wrapped type then deletes that.
  std::unique_ptr<void, void (*)(void*)> ptr_;

  /// next_ is a function pointer which first casts from void* to a pointer to the wrapped
  /// type then invokes its Next member function.
  Result<T> (*next_)(void*) = NULLPTR;
};

template <typename T>
struct IterationTraits<Iterator<T>> {
  // The end condition for an Iterator of Iterators is a default constructed (null)
  // Iterator.
  static Iterator<T> End() { return Iterator<T>(); }
};

template <typename Fn, typename T>
class FunctionIterator {
 public:
  explicit FunctionIterator(Fn fn) : fn_(std::move(fn)) {}

  Result<T> Next() { return fn_(); }

 private:
  Fn fn_;
};

/// \brief Construct an Iterator which invokes a callable on Next()
template <typename Fn,
          typename Ret = typename internal::call_traits::return_type<Fn>::ValueType>
Iterator<Ret> MakeFunctionIterator(Fn fn) {
  return Iterator<Ret>(FunctionIterator<Fn, Ret>(std::move(fn)));
}

template <typename T>
Iterator<T> MakeEmptyIterator() {
  return MakeFunctionIterator([]() -> Result<T> { return IterationTraits<T>::End(); });
}

template <typename T>
Iterator<T> MakeErrorIterator(Status s) {
  return MakeFunctionIterator([s]() -> Result<T> {
    ARROW_RETURN_NOT_OK(s);
    return IterationTraits<T>::End();
  });
}

/// \brief Simple iterator which yields the elements of a std::vector
template <typename T>
class VectorIterator {
 public:
  explicit VectorIterator(std::vector<T> v) : elements_(std::move(v)) {}

  Result<T> Next() {
    if (i_ == elements_.size()) {
      return IterationTraits<T>::End();
    }
    return std::move(elements_[i_++]);
  }

 private:
  std::vector<T> elements_;
  size_t i_ = 0;
};

template <typename T>
Iterator<T> MakeVectorIterator(std::vector<T> v) {
  return Iterator<T>(VectorIterator<T>(std::move(v)));
}

/// \brief Simple iterator which yields *pointers* to the elements of a std::vector<T>.
/// This is provided to support T where IterationTraits<T>::End is not specialized
template <typename T>
class VectorPointingIterator {
 public:
  explicit VectorPointingIterator(std::vector<T> v) : elements_(std::move(v)) {}

  Result<T*> Next() {
    if (i_ == elements_.size()) {
      return NULLPTR;
    }
    return &elements_[i_++];
  }

 private:
  std::vector<T> elements_;
  size_t i_ = 0;
};

template <typename T>
Iterator<T*> MakeVectorPointingIterator(std::vector<T> v) {
  return Iterator<T*>(VectorPointingIterator<T>(std::move(v)));
}

/// \brief MapIterator takes ownership of an iterator and a function to apply
/// on every element. The mapped function is not allowed to fail.
template <typename Fn, typename I, typename O>
class MapIterator {
 public:
  explicit MapIterator(Fn map, Iterator<I> it)
      : map_(std::move(map)), it_(std::move(it)) {}

  Result<O> Next() {
    ARROW_ASSIGN_OR_RAISE(I i, it_.Next());

    if (i == IterationTraits<I>::End()) {
      return IterationTraits<O>::End();
    }

    return map_(std::move(i));
  }

 private:
  Fn map_;
  Iterator<I> it_;
};

/// \brief MapIterator takes ownership of an iterator and a function to apply
/// on every element. The mapped function is not allowed to fail.
template <typename Fn, typename From = internal::call_traits::argument_type<0, Fn>,
          typename To = internal::call_traits::return_type<Fn>>
Iterator<To> MakeMapIterator(Fn map, Iterator<From> it) {
  return Iterator<To>(MapIterator<Fn, From, To>(std::move(map), std::move(it)));
}

/// \brief Like MapIterator, but where the function can fail.
template <typename Fn, typename From = internal::call_traits::argument_type<0, Fn>,
          typename To = typename internal::call_traits::return_type<Fn>::ValueType>
Iterator<To> MakeMaybeMapIterator(Fn map, Iterator<From> it) {
  return Iterator<To>(MapIterator<Fn, From, To>(std::move(map), std::move(it)));
}

struct FilterIterator {
  enum Action { ACCEPT, REJECT };

  template <typename To>
  static Result<std::pair<To, Action>> Reject() {
    return std::make_pair(IterationTraits<To>::End(), REJECT);
  }

  template <typename To>
  static Result<std::pair<To, Action>> Accept(To out) {
    return std::make_pair(std::move(out), ACCEPT);
  }

  template <typename To>
  static Result<std::pair<To, Action>> MaybeAccept(Result<To> maybe_out) {
    return std::move(maybe_out).Map(Accept<To>);
  }

  template <typename To>
  static Result<std::pair<To, Action>> Error(Status s) {
    return s;
  }

  template <typename Fn, typename From, typename To>
  class Impl {
   public:
    explicit Impl(Fn filter, Iterator<From> it) : filter_(filter), it_(std::move(it)) {}

    Result<To> Next() {
      To out = IterationTraits<To>::End();
      Action action;

      for (;;) {
        ARROW_ASSIGN_OR_RAISE(From i, it_.Next());

        if (i == IterationTraits<From>::End()) {
          return IterationTraits<To>::End();
        }

        ARROW_ASSIGN_OR_RAISE(std::tie(out, action), filter_(std::move(i)));

        if (action == ACCEPT) return out;
      }
    }

   private:
    Fn filter_;
    Iterator<From> it_;
  };
};

/// \brief Like MapIterator, but where the function can fail or reject elements.
template <
    typename Fn, typename From = typename internal::call_traits::argument_type<0, Fn>,
    typename Ret = typename internal::call_traits::return_type<Fn>::ValueType,
    typename To = typename std::tuple_element<0, Ret>::type,
    typename Enable = typename std::enable_if<std::is_same<
        typename std::tuple_element<1, Ret>::type, FilterIterator::Action>::value>::type>
Iterator<To> MakeFilterIterator(Fn filter, Iterator<From> it) {
  return Iterator<To>(
      FilterIterator::Impl<Fn, From, To>(std::move(filter), std::move(it)));
}

/// \brief FlattenIterator takes an iterator generating iterators and yields a
/// unified iterator that flattens/concatenates in a single stream.
template <typename T>
class FlattenIterator {
 public:
  explicit FlattenIterator(Iterator<Iterator<T>> it) : parent_(std::move(it)) {}

  Result<T> Next() {
    if (child_ == IterationTraits<Iterator<T>>::End()) {
      // Pop from parent's iterator.
      ARROW_ASSIGN_OR_RAISE(child_, parent_.Next());

      // Check if final iteration reached.
      if (child_ == IterationTraits<Iterator<T>>::End()) {
        return IterationTraits<T>::End();
      }

      return Next();
    }

    // Pop from child_ and check for depletion.
    ARROW_ASSIGN_OR_RAISE(T out, child_.Next());
    if (out == IterationTraits<T>::End()) {
      // Reset state such that we pop from parent on the recursive call
      child_ = IterationTraits<Iterator<T>>::End();

      return Next();
    }

    return out;
  }

 private:
  Iterator<Iterator<T>> parent_;
  Iterator<T> child_ = IterationTraits<Iterator<T>>::End();
};

template <typename T>
Iterator<T> MakeFlattenIterator(Iterator<Iterator<T>> it) {
  return Iterator<T>(FlattenIterator<T>(std::move(it)));
}

namespace detail {

// A type-erased promise object for ReadaheadQueue.
struct ARROW_EXPORT ReadaheadPromise {
  virtual ~ReadaheadPromise();
  virtual void Call() = 0;
};

template <typename T>
struct ReadaheadIteratorPromise : ReadaheadPromise {
  ~ReadaheadIteratorPromise() override {}

  explicit ReadaheadIteratorPromise(Iterator<T>* it) : it_(it) {}

  void Call() override {
    assert(!called_);
    out_ = it_->Next();
    called_ = true;
  }

  Iterator<T>* it_;
  Result<T> out_ = IterationTraits<T>::End();
  bool called_ = false;
};

class ARROW_EXPORT ReadaheadQueue {
 public:
  explicit ReadaheadQueue(int readahead_queue_size);
  ~ReadaheadQueue();

  Status Append(std::unique_ptr<ReadaheadPromise>);
  Status PopDone(std::unique_ptr<ReadaheadPromise>*);
  Status Pump(std::function<std::unique_ptr<ReadaheadPromise>()> factory);
  Status Shutdown();
  void EnsureShutdownOrDie();

 protected:
  class Impl;
  std::shared_ptr<Impl> impl_;
};

}  // namespace detail

/// \brief Readahead iterator that iterates on the underlying iterator in a
/// separate thread, getting up to N values in advance.
template <typename T>
class ReadaheadIterator {
  using PromiseType = typename detail::ReadaheadIteratorPromise<T>;

 public:
  // Public default constructor creates an empty iterator
  ReadaheadIterator() : done_(true) {}

  ~ReadaheadIterator() {
    if (queue_) {
      // Make sure the queue doesn't call any promises after this object
      // is destroyed.
      queue_->EnsureShutdownOrDie();
    }
  }

  ARROW_DEFAULT_MOVE_AND_ASSIGN(ReadaheadIterator);
  ARROW_DISALLOW_COPY_AND_ASSIGN(ReadaheadIterator);

  Result<T> Next() {
    if (done_) {
      return IterationTraits<T>::End();
    }

    std::unique_ptr<detail::ReadaheadPromise> promise;
    ARROW_RETURN_NOT_OK(queue_->PopDone(&promise));
    auto it_promise = static_cast<PromiseType*>(promise.get());

    ARROW_RETURN_NOT_OK(queue_->Append(MakePromise()));

    ARROW_ASSIGN_OR_RAISE(auto out, it_promise->out_);
    if (out == IterationTraits<T>::End()) {
      done_ = true;
    }
    return out;
  }

  static Result<Iterator<T>> Make(Iterator<T> it, int readahead_queue_size) {
    ReadaheadIterator rh(std::move(it), readahead_queue_size);
    ARROW_RETURN_NOT_OK(rh.Pump());
    return Iterator<T>(std::move(rh));
  }

 private:
  explicit ReadaheadIterator(Iterator<T> it, int readahead_queue_size)
      : it_(new Iterator<T>(std::move(it))),
        queue_(new detail::ReadaheadQueue(readahead_queue_size)) {}

  Status Pump() {
    return queue_->Pump([this]() { return MakePromise(); });
  }

  std::unique_ptr<detail::ReadaheadPromise> MakePromise() {
    return std::unique_ptr<detail::ReadaheadPromise>(new PromiseType{it_.get()});
  }

  // The underlying iterator is referenced by pointer in ReadaheadPromise,
  // so make sure it doesn't move.
  std::unique_ptr<Iterator<T>> it_;
  std::unique_ptr<detail::ReadaheadQueue> queue_;
  bool done_ = false;
};

template <typename T>
Result<Iterator<T>> MakeReadaheadIterator(Iterator<T> it, int readahead_queue_size) {
  return ReadaheadIterator<T>::Make(std::move(it), readahead_queue_size);
}

namespace detail {

/// This is the AsyncReadaheadIterator's equivalent of ReadaheadQueue.  I'm using the term
/// worker here so it is a bit more explicit that there is an actual thread running and it
/// is something that starts (Start) and stops (EnsureShutdownOrDie).
///
/// Unlike it's inspiration this class is not type-erased.  It probably could be but I'll
/// leave that as an exercise for future development.
template <typename T>
class ARROW_EXPORT AsyncReadaheadWorker
    : public std::enable_shared_from_this<AsyncReadaheadWorker<T>> {
 public:
  explicit AsyncReadaheadWorker(int readahead_queue_size)
      : max_readahead_(readahead_queue_size) {}

  ~AsyncReadaheadWorker() { EnsureShutdownOrDie(false); }

  void Start(std::shared_ptr<Iterator<T>> iterator) {
    DCHECK(!thread_.joinable());
    auto self = this->shared_from_this();
    thread_ = std::thread([self, iterator]() { self->DoWork(iterator); });
    DCHECK(thread_.joinable());
  }

  Future<T> AddRequest() {
    std::unique_lock<std::mutex> lock(mutex_);
    // If we have results in our readahead queue then we can immediately return a
    // completed future
    if (unconsumed_results_.size() > 0) {
      auto wake_up_after = unconsumed_results_.size() == max_readahead_;
      auto result = Future<T>::MakeFinished(unconsumed_results_.front());
      unconsumed_results_.pop_front();
      lock.unlock();
      if (wake_up_after) {
        worker_wakeup_.notify_one();
      }
      return result;
    }
    // Otherwise, we don't have a stored result, so we need to make a request for a new
    // result. Unless we're finished (hit the end of the iterator) in which case we can
    // just return End()
    if (please_shutdown_) {
      return Future<T>::MakeFinished(IterationTraits<T>::End());
    }
    auto result = Future<T>::Make();
    waiting_futures_.push_back(result);
    return result;
  }

  Status Shutdown(bool wait = true) {
    return ShutdownUnlocked(std::unique_lock<std::mutex>(mutex_), wait);
  }

  void EnsureShutdownOrDie(bool wait = true) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (thread_.joinable()) {
      ARROW_CHECK_OK(ShutdownUnlocked(std::move(lock), wait));
      DCHECK(!thread_.joinable());
    }
  }

  Status ShutdownUnlocked(std::unique_lock<std::mutex> lock, bool wait = true) {
    if (!please_shutdown_) {
      FinishUnlocked();
    }
    lock.unlock();
    worker_wakeup_.notify_one();
    if (wait) {
      thread_.join();
    } else {
      thread_.detach();
    }
    return Status::OK();
  }

  void FinishUnlocked() {
    please_shutdown_ = true;
    for (auto&& future : waiting_futures_) {
      future.MarkFinished(IterationTraits<T>::End());
    }
  }

  void Finish() {
    std::unique_lock<std::mutex> lock(mutex_);
    FinishUnlocked();
  }

  void DoWork(std::shared_ptr<Iterator<T>> iterator) {
    std::unique_lock<std::mutex> lock(mutex_);
    while (!please_shutdown_) {
      while (unconsumed_results_.size() < max_readahead_) {
        // Grab the next item, might be expensive (I/O) so we don't want to keep the lock
        lock.unlock();
        auto next_item = iterator->Next();
        lock.lock();
        // If we're done then we should deliver End() to all outstanding requests
        if (next_item == IterationTraits<T>::End()) {
          FinishUnlocked();
        } else {
          // If there are any outstanding requests then pop one off and deliver this item
          if (waiting_futures_.size() > 0) {
            auto next_future = waiting_futures_.front();
            waiting_futures_.pop_front();
            // Marking the future finished may trigger expensive callbacks so we don't
            // want to hold the lock while we do that.
            lock.unlock();
            next_future.MarkFinished(std::move(next_item));
            lock.lock();
            // Otherwise, if there are no oustanding requests, add the item to our
            // readahead queue
          } else {
            unconsumed_results_.push_back(std::move(next_item));
          }
        }
        // Exit eagerly
        if (please_shutdown_) {
          return;
        }
      }
      // Wait for more work to do
      worker_wakeup_.wait(lock);
    }
  }

 protected:
  std::deque<Result<T>> unconsumed_results_;
  std::deque<Future<T>> waiting_futures_;
  std::size_t max_readahead_;
  bool please_shutdown_ = false;

  std::thread thread_;
  std::mutex mutex_;
  std::condition_variable worker_wakeup_;
};

}  // namespace detail

/// \brief This iterates on the underlying iterator in a separate thread, getting up to
/// N values in advance.  Unlike ReadaheadIterator this is not an iterator in the
/// traditional sense.  Every call to NextFuture will generate a new future whether data
/// is ready or not.
///
/// This means, without some form of back pressure, you are likely to create an endless
/// supply of useless futures (which will eventually be fulfilled with
/// IterationTraits<T>::End()) if the underlying iterator completes before you explode the
/// heap/stack.
///
/// This iterator is instead meant to be passed to some kind of continuation aware
/// iteration method such as async::ForEach
template <typename T>
class AsyncReadaheadIterator {
 public:
  // Public default constructor creates an empty iterator
  AsyncReadaheadIterator() : done_(true) {}

  ~AsyncReadaheadIterator() {
    if (worker_ != NULLPTR) {
      worker_->EnsureShutdownOrDie();
    }
  }

  ARROW_DEFAULT_MOVE_AND_ASSIGN(AsyncReadaheadIterator);
  ARROW_DISALLOW_COPY_AND_ASSIGN(AsyncReadaheadIterator);

  Future<T> NextFuture() {
    if (done_) {
      return Future<T>::MakeFinished(IterationTraits<T>::End());
    }
    return worker_->AddRequest();
  }

  static Result<AsyncReadaheadIterator<T>> Make(Iterator<T> it,
                                                int readahead_queue_size) {
    return AsyncReadaheadIterator<T>(std::move(it), readahead_queue_size);
  }

 private:
  explicit AsyncReadaheadIterator(Iterator<T> it, int readahead_queue_size)
      : worker_(std::make_shared<detail::AsyncReadaheadWorker<T>>(readahead_queue_size)) {
    worker_->Start(std::make_shared<Iterator<T>>(std::move(it)));
  }

  std::shared_ptr<detail::AsyncReadaheadWorker<T>> worker_;
  bool done_ = false;
};

template <typename T>
Result<AsyncReadaheadIterator<T>> MakeAsyncReadaheadIterator(Iterator<T> it,
                                                             int readahead_queue_size) {
  return AsyncReadaheadIterator<T>::Make(std::move(it), readahead_queue_size);
}

namespace async {

namespace internal {

// This is a recursive function.  The base cases are when the iterator returns a failed
// future (e.g. an I/O error) or when the underlying iterator is completed.
//
// Whenever recursion is performed on an iterator care should be taken to avoid a stack
// overflow.  This picture is a bit more complicated however because `Then` won't
// neccesarily execute immediately.
template <typename T>
Future<> AsyncForEachHelper(std::shared_ptr<AsyncReadaheadIterator<T>> it,
                            std::function<Status(const T& t)> func,
                            arrow::internal::Executor* executor = NULLPTR) {
  auto future = it->NextFuture();
  if (executor != NULLPTR) {
    auto transferred_future = executor->Transfer(future);
    if (!transferred_future.ok()) {
      return Future<>::MakeFinished(transferred_future.status());
    }
    future = transferred_future.ValueUnsafe();
  }
  return future.Then([it, func](const Result<T>& t) {
    if (!t.ok()) {
      return Future<>::MakeFinished(t.status());
    }
    auto value = t.ValueUnsafe();
    if (value == IterationTraits<T>::End()) {
      return Future<>::MakeFinished();
    }
    auto cb_result = func(std::move(value));
    if (!cb_result.ok()) {
      return Future<>::MakeFinished(cb_result);
    }
    return AsyncForEachHelper(std::move(it), func);
  });
}

}  // namespace internal

/// \brief Provides a safe and convenient method to visit an async iterator
///
/// This will take ownership of `it`
///
/// The num_workers argument can be used to allow for multiple processing tasks to run in
/// parallel.  This will only have an effect if executor is specified.
///
/// If `executor` is supplied then `func` will be submitted on the executor.  If it is not
/// specified (The default) then `func` will be run on the same thread the async iterator
/// uses to complete the future (for AsyncReadaheadIterator this will be the created
/// readahead thread)
///
/// The caller is responsible for guaranteeing that the `executor` pointer is valid for
/// the duration of this iteration.
///
/// If any errors are encountered by the iterator this iteration will stop immediately and
/// the future returned by this method will be marked failed with the error.  Otherwise,
/// the future returned by this method will be marked complete with an OK status when
/// `func` has been applied to every item the underlying iterator produces.
template <typename T>
Future<> AsyncForEach(AsyncReadaheadIterator<T> it,
                      std::function<Status(const T& t)> func, int num_workers = 1,
                      arrow::internal::Executor* executor = NULLPTR) {
  if (num_workers <= 0) {
    return Future<>::MakeFinished(Status::Invalid(
        "The num_workers argument to AsyncForEach should be a positive integer"));
  }
  auto task_group = arrow::internal::TaskGroup::MakeThreaded(NULLPTR);
  std::shared_ptr<AsyncReadaheadIterator<T>> it_ptr(
      new AsyncReadaheadIterator<T>(std::move(it)));

  auto n_remaining = std::make_shared<std::atomic<int>>();
  n_remaining->store(num_workers);

  std::vector<Future<>> workers(num_workers);
  for (int i = 0; i < num_workers; ++i) {
    workers[i] = internal::AsyncForEachHelper(it_ptr, func, executor);

    task_group->Append(workers[i]);

    workers[i].Then([n_remaining, task_group](const Status&) {
      if (n_remaining->fetch_sub(1) == 1) {
        return task_group->Finish();
      }
      return Status::OK();
    });
  }

  return task_group->FinishAsync();
}

}  // namespace async
}  // namespace arrow

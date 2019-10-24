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
#include <functional>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/compare.h"
#include "arrow/util/functional.h"
#include "arrow/util/macros.h"
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

  Iterator() : ptr_(NULLPTR, NoopDelete) {}

  /// \brief Return the next element of the sequence, IterationTraits<T>::End() when the
  /// iteration is completed. Calling this on a default constructed Iterator
  /// will result in undefined behavior.
  Status Next(T* out) { return next_(ptr_.get(), out); }

  /// Pass each element of the sequence to a visitor. Will return any error status
  /// returned by the visitor, terminating iteration.
  template <typename Visitor>
  Status Visit(Visitor&& visitor) {
    Status status;

    for (T value, end = IterationTraits<T>::End();;) {
      status = Next(&value);

      if (!status.ok()) return status;

      if (value == end) break;

      ARROW_RETURN_NOT_OK(visitor(std::move(value)));
    }

    return status;
  }

  /// Iterators will only compare equal if they are both null.
  /// Equality comparability is required to make an Iterator of Iterators
  /// (to check for the end condition).
  bool Equals(const Iterator& other) const { return ptr_ == other.ptr_; }

  explicit operator bool() const { return ptr_ != NULLPTR; }

  class RangeIterator {
   public:
    RangeIterator() = default;

    explicit RangeIterator(Iterator i) : iterator_(std::move(i)) { Next(); }

    bool operator!=(const RangeIterator& other) const {
      return !(status_ == other.status_ && value_ == other.value_);
    }

    RangeIterator& operator++() {
      Next();
      return *this;
    }

    Result<T> operator*() {
      if (status_.ok()) {
        return std::move(value_);
      }
      return status_;
    }

   private:
    void Next() {
      if (!status_.ok()) {
        status_ = Status::OK();
        value_ = IterationTraits<T>::End();
        return;
      }
      status_ = iterator_.Next(&value_);
    }

    T value_ = IterationTraits<T>::End();
    Iterator iterator_;
    Status status_;
  };

  RangeIterator begin() { return RangeIterator(std::move(*this)); }

  RangeIterator end() { return RangeIterator(); }

 private:
  /// Implementation of deleter for ptr_: Casts from void* to the wrapped type and
  /// deletes that.
  template <typename HasNext>
  static void Delete(void* ptr) {
    delete static_cast<HasNext*>(ptr);
  }

  /// Noop delete, used only by the default constructed case where ptr_ is null and
  /// nothing must be deleted.
  static void NoopDelete(void*) {}

  /// Implementation of Next: Casts from void* to the wrapped type and invokes that
  /// type's Next member function.
  template <typename HasNext>
  static Status Next(void* ptr, T* out) {
    return static_cast<HasNext*>(ptr)->Next(out);
  }

  /// ptr_ is a unique_ptr to void with a custom deleter: a function pointer which first
  /// casts from void* to a pointer to the wrapped type then deletes that.
  std::unique_ptr<void, void (*)(void*)> ptr_;

  /// next_ is a function pointer which first casts from void* to a pointer to the wrapped
  /// type then invokes its Next member function.
  Status (*next_)(void*, T*) = NULLPTR;
};

template <typename T>
struct IterationTraits<Iterator<T>> {
  // The end condition for an Iterator of Iterators is a default constructed (null)
  // Iterator.
  static Iterator<T> End() { return Iterator<T>(); }
};

template <typename Ptr, typename T>
class PointerIterator {
 public:
  explicit PointerIterator(Ptr ptr) : ptr_(std::move(ptr)) {}

  Status Next(T* out) { return ptr_->Next(out); }

 private:
  Ptr ptr_;
};

/// \brief Construct an Iterator which dereferences a (possibly smart) pointer
/// to invoke its Next function
template <typename Ptr,
          typename Pointed = typename std::decay<decltype(*std::declval<Ptr>())>::type,
          typename T = typename std::remove_pointer<typename decltype(
              internal::member_function_argument_type<0>(&Pointed::Next))::type>::type>
Iterator<T> MakePointerIterator(Ptr ptr) {
  return Iterator<T>(PointerIterator<Ptr, T>(std::move(ptr)));
}

template <typename Fn, typename T>
class FunctionIterator {
 public:
  explicit FunctionIterator(Fn fn) : fn_(std::move(fn)) {}

  Status Next(T* out) { return fn_(out); }

 private:
  Fn fn_;
};

/// \brief Construct an Iterator which invokes a callable on Next()
template <typename Fn, typename T = typename std::remove_pointer<
                           internal::call_traits::argument_type<0, Fn>>::type>
Iterator<T> MakeFunctionIterator(Fn fn) {
  return Iterator<T>(FunctionIterator<Fn, T>(std::move(fn)));
}

template <typename T>
Iterator<T> MakeEmptyIterator() {
  return MakeFunctionIterator([](T* out) {
    *out = IterationTraits<T>::End();
    return Status::OK();
  });
}

template <typename T>
Iterator<T> MakeErrorIterator(Status s) {
  return MakeFunctionIterator([s](T* out) {
    *out = IterationTraits<T>::End();
    return s;
  });
}

/// \brief Simple iterator which yields the elements of a std::vector
template <typename T>
class VectorIterator {
 public:
  explicit VectorIterator(std::vector<T> v) : elements_(std::move(v)) {}

  Status Next(T* out) {
    *out =
        i_ == elements_.size() ? IterationTraits<T>::End() : std::move(elements_[i_++]);
    return Status::OK();
  }

 private:
  std::vector<T> elements_;
  size_t i_ = 0;
};

template <typename T>
Iterator<T> MakeVectorIterator(std::vector<T> v) {
  return Iterator<T>(VectorIterator<T>(std::move(v)));
}

/// \brief MapIterator takes ownership of an iterator and a function to apply
/// on every element. The mapped function is not allowed to fail.
template <typename Fn,
          typename I = typename std::remove_pointer<
              internal::call_traits::argument_type<0, Fn>>::type,
          typename O = typename std::result_of<Fn(I)>::type>
class MapIterator {
 public:
  explicit MapIterator(Fn map, Iterator<I> it)
      : map_(std::move(map)), it_(std::move(it)) {}

  Status Next(O* out) {
    I i;

    ARROW_RETURN_NOT_OK(it_.Next(&i));
    // Ensure loops exit.
    *out =
        i == IterationTraits<I>::End() ? IterationTraits<O>::End() : map_(std::move(i));

    return Status::OK();
  }

 private:
  Fn map_;
  Iterator<I> it_;
};

template <typename Fn,
          typename I = typename std::remove_pointer<
              internal::call_traits::argument_type<0, Fn>>::type,
          typename O = typename std::result_of<Fn(I)>::type>
Iterator<O> MakeMapIterator(Fn map, Iterator<I> it) {
  return Iterator<O>(MapIterator<Fn, I, O>(std::move(map), std::move(it)));
}

/// \brief Like MapIterator, but where the function can fail.
template <
    typename Fn,
    typename I =
        typename std::remove_pointer<internal::call_traits::argument_type<0, Fn>>::type,
    typename O =
        typename std::remove_pointer<internal::call_traits::argument_type<1, Fn>>::typ>
class MaybeMapIterator {
 public:
  explicit MaybeMapIterator(Fn map, Iterator<I> it) : map_(map), it_(std::move(it)) {}

  Status Next(O* out) {
    I i;

    ARROW_RETURN_NOT_OK(it_.Next(&i));
    if (i == IterationTraits<I>::End()) {
      *out = IterationTraits<O>::End();
      return Status::OK();
    }

    return map_(std::move(i), out);
  }

 private:
  Fn map_;
  Iterator<I> it_;
};

template <
    typename Fn,
    typename I =
        typename std::remove_pointer<internal::call_traits::argument_type<0, Fn>>::type,
    typename O =
        typename std::remove_pointer<internal::call_traits::argument_type<1, Fn>>::type>
Iterator<O> MakeMaybeMapIterator(Fn map, Iterator<I> it) {
  return Iterator<O>(MaybeMapIterator<Fn, I, O>(std::move(map), std::move(it)));
}

/// \brief Like MapIterator, but where the function can fail or reject elements.
template <
    typename Fn,
    typename I =
        typename std::remove_pointer<internal::call_traits::argument_type<0, Fn>>::type,
    typename O =
        typename std::remove_pointer<internal::call_traits::argument_type<1, Fn>>::type>
class FilterIterator {
 public:
  explicit FilterIterator(Fn filter, Iterator<I> it)
      : filter_(filter), it_(std::move(it)) {}

  Status Next(O* out) {
    bool accept = true;
    do {
      I i;

      ARROW_RETURN_NOT_OK(it_.Next(&i));
      if (i == IterationTraits<I>::End()) {
        *out = IterationTraits<O>::End();
        return Status::OK();
      }

      ARROW_RETURN_NOT_OK(filter_(std::move(i), out, &accept));
    } while (!accept);

    return Status::OK();
  }

 private:
  Fn filter_;
  Iterator<I> it_;
};

template <
    typename Fn,
    typename I =
        typename std::remove_pointer<internal::call_traits::argument_type<0, Fn>>::type,
    typename O =
        typename std::remove_pointer<internal::call_traits::argument_type<1, Fn>>::type,
    typename Enable = typename std::enable_if<std::is_same<
        bool, typename std::remove_pointer<
                  internal::call_traits::argument_type<2, Fn>>::type>::value>::type>
Iterator<O> MakeFilterIterator(Fn filter, Iterator<I> it) {
  return Iterator<O>(FilterIterator<Fn, I, O>(std::move(filter), std::move(it)));
}

/// \brief FlattenIterator takes an iterator generating iterators and yields a
/// unified iterator that flattens/concatenates in a single stream.
template <typename T>
class FlattenIterator {
 public:
  explicit FlattenIterator(Iterator<Iterator<T>> it) : parent_(std::move(it)) {}

  Status Next(T* out) {
    if (done_) {
      *out = IterationTraits<T>::End();
      return Status::OK();
    }

    if (child_ == IterationTraits<Iterator<T>>::End()) {
      // Pop from parent's iterator.
      ARROW_RETURN_NOT_OK(parent_.Next(&child_));
      // Check if final iteration reached.
      done_ = (child_ == IterationTraits<Iterator<T>>::End());
      return Next(out);
    }

    // Pop from child_ and lookout for depletion.
    ARROW_RETURN_NOT_OK(child_.Next(out));
    if (*out == IterationTraits<T>::End()) {
      // Reset state such that we pop from parent on the recursive call
      child_ = IterationTraits<Iterator<T>>::End();
      return Next(out);
    }

    return Status::OK();
  }

 private:
  Iterator<Iterator<T>> parent_;
  Iterator<T> child_ = IterationTraits<Iterator<T>>::End();
  // The usage of done_ could be avoided by setting parent_ to null, but this
  // would hamper debugging.
  bool done_ = false;
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
    status_ = it_->Next(&out_);
    called_ = true;
  }

  Iterator<T>* it_;
  Status status_;
  T out_ = IterationTraits<T>::End();
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

  Status Next(T* out) {
    if (done_) {
      *out = IterationTraits<T>::End();
      return Status::OK();
    }

    std::unique_ptr<detail::ReadaheadPromise> promise;
    ARROW_RETURN_NOT_OK(queue_->PopDone(&promise));
    auto it_promise = static_cast<PromiseType*>(promise.get());

    ARROW_RETURN_NOT_OK(queue_->Append(MakePromise()));

    ARROW_RETURN_NOT_OK(it_promise->status_);
    *out = std::move(it_promise->out_);
    if (*out == IterationTraits<T>::End()) {
      done_ = true;
    }
    return Status::OK();
  }

  static Status Make(Iterator<T> it, int readahead_queue_size, ReadaheadIterator* out) {
    ReadaheadIterator rh(std::move(it), readahead_queue_size);
    ARROW_RETURN_NOT_OK(rh.Pump());
    *out = std::move(rh);
    return Status::OK();
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
Status MakeReadaheadIterator(Iterator<T> it, int readahead_queue_size, Iterator<T>* out) {
  ReadaheadIterator<T> rh;
  ARROW_RETURN_NOT_OK(
      ReadaheadIterator<T>::Make(std::move(it), readahead_queue_size, &rh));
  *out = Iterator<T>(std::move(rh));
  return Status::OK();
}

}  // namespace arrow

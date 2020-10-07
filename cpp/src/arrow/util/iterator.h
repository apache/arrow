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
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/compare.h"
#include "arrow/util/functional.h"
#include "arrow/util/macros.h"
#include "arrow/util/optional.h"
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

}  // namespace arrow

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

#include <iostream>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/status.h"
#include "arrow/util/functional.h"
#include "arrow/util/macros.h"

namespace arrow {

template <typename T>
class Iterator;

/// \brief IteratorEnd returns a reserved value which indicates the end of iteration. By
/// default this is NULLPTR since most iterators yield pointer types. Specialize/overload
/// if different end semantics are required.
template <typename T>
static T IteratorEnd(internal::type_constant<T>) {
  return T(NULLPTR);
}

/// Convenience function, overload the type_constant version above and not this one.
template <typename T>
static T IteratorEnd() {
  return IteratorEnd(internal::type_constant<T>{});
}

/// \brief A generic Iterator that can return errors
template <typename T>
class Iterator {
 public:
  /// \brief Iterator may be constructed from any type which has member function
  /// Status HasNext::Next(T*);
  template <typename HasNext>
  explicit Iterator(HasNext has_next)
      : ptr_(new HasNext(std::move(has_next)), Delete<HasNext>), next_(Next<HasNext>) {}

  Iterator() : ptr_(NULLPTR, NoopDelete) {}

  /// \brief Return the next element of the sequence, IteratorEnd<T>() when the
  /// iteration is completed. Calling this on a default constructed Iterator
  /// will result in undefined behavior.
  Status Next(T* out) { return next_(ptr_.get(), out); }

  /// Pass each element of the sequence to a visitor. Will return any error status
  /// returned by the visitor, terminating iteration.
  template <typename Visitor>
  Status Visit(Visitor&& visitor) {
    Status status;

    for (T value, end = IteratorEnd<T>();;) {
      status = Next(&value);

      if (!status.ok()) return status;

      if (value == end) break;

      ARROW_RETURN_NOT_OK(visitor(std::move(value)));
    }

    return status;
  }

  bool operator==(const Iterator& other) const { return ptr_ == other.ptr_; }

  explicit operator bool() const { return ptr_ == NULLPTR; }

 private:
  template <typename HasNext>
  static void Delete(void* ptr) {
    delete static_cast<HasNext*>(ptr);
  }

  static void NoopDelete(void*) {}

  template <typename HasNext>
  static Status Next(void* ptr, T* out) {
    return static_cast<HasNext*>(ptr)->Next(out);
  }

  std::unique_ptr<void, void (*)(void*)> ptr_;
  Status (*next_)(void*, T*) = NULLPTR;

  friend Iterator IteratorEnd(internal::type_constant<Iterator>) {
    // end condition for an Iterator of Iterators is a default constructed (null) iterator
    return Iterator();
  }
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
          typename Fn = decltype(std::mem_fun(&Pointed::Next)),
          typename T = typename std::remove_pointer<
              internal::call_traits::argument_type<1, Fn>>::type>
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
Iterator<T> MakeEmptyIterator(Status s = Status::OK()) {
  return MakeFunctionIterator([s](T* out) {
    *out = IteratorEnd<T>();
    return s;
  });
}

/// \brief Simple iterator which yields the elements of a std::vector
template <typename T>
class VectorIterator {
 public:
  explicit VectorIterator(std::vector<T> v) : elements_(std::move(v)) {}

  Status Next(T* out) {
    *out = i_ == elements_.size() ? IteratorEnd<T>() : std::move(elements_[i_++]);
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
    *out = i == IteratorEnd<I>() ? IteratorEnd<O>() : map_(std::move(i));

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
    if (i == IteratorEnd<I>()) {
      *out = IteratorEnd<O>();
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

/// \brief FlattenIterator takes an iterator generating iterators and yields a
/// unified iterator that flattens/concatenates in a single stream.
template <typename T>
class FlattenIterator {
 public:
  explicit FlattenIterator(Iterator<Iterator<T>> it) : parent_(std::move(it)) {}

  Status Next(T* out) {
    if (done_) {
      *out = IteratorEnd<T>();
      return Status::OK();
    }

    if (child_ == IteratorEnd<Iterator<T>>()) {
      // Pop from parent's iterator.
      ARROW_RETURN_NOT_OK(parent_.Next(&child_));
      // Check if final iteration reached.
      done_ = (child_ == IteratorEnd<Iterator<T>>());
      return Next(out);
    }

    // Pop from child_ and lookout for depletion.
    ARROW_RETURN_NOT_OK(child_.Next(out));
    if (*out == IteratorEnd<T>()) {
      // Reset state such that we pop from parent on the recursive call
      child_ = IteratorEnd<Iterator<T>>();
      return Next(out);
    }

    return Status::OK();
  }

 private:
  Iterator<Iterator<T>> parent_;
  Iterator<T> child_ = IteratorEnd<Iterator<T>>();
  // The usage of done_ could be avoided by setting parent_ to null, but this
  // would hamper debugging.
  bool done_ = false;
};

template <typename T>
Iterator<T> MakeFlattenIterator(Iterator<Iterator<T>> it) {
  return Iterator<T>(FlattenIterator<T>(std::move(it)));
}

}  // namespace arrow

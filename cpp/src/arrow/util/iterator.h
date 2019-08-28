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

#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/status.h"
#include "arrow/util/functional.h"
#include "arrow/util/macros.h"

namespace arrow {

/// \brief A generic Iterator that can return errors
template <typename T>
class Iterator {
 public:
  static_assert(std::is_assignable<T, std::nullptr_t>::value,
                "NULL is used to signal completion");

  virtual ~Iterator() = default;

  /// \brief Return the next element of the sequence, nullptr when the
  /// iteration is completed
  virtual Status Next(T* out) = 0;

  /// Pass each element of the sequence to a visitor. Will return any error status
  /// returned by the visitor, terminating iteration.
  template <typename Visitor>
  Status Visit(Visitor&& visitor) {
    Status status;
    T value;

    for (;;) {
      status = Next(&value);

      if (!status.ok()) return status;

      if (value == NULLPTR) break;

      ARROW_RETURN_NOT_OK(visitor(std::move(value)));
    }

    return status;
  }
};

/// \brief Simple iterator which yields the elements of a std::vector
template <typename T>
class VectorIterator : public Iterator<T> {
 public:
  explicit VectorIterator(std::vector<T> v) : elements_(std::move(v)) {}

  Status Next(T* out) override {
    *out = i_ == elements_.size() ? NULLPTR : elements_[i_++];
    return Status::OK();
  }

 private:
  std::vector<T> elements_;
  size_t i_ = 0;
};

template <typename T>
std::unique_ptr<Iterator<T>> MakeVectorIterator(std::vector<T> v) {
  return std::unique_ptr<VectorIterator<T>>(new VectorIterator<T>(std::move(v)));
}

/// \brief MapIterator takes ownership of an iterator and a function to apply
/// on every element.
template <typename Fn, typename I, typename O = typename std::result_of<Fn(I)>::type>
class MapIterator : public Iterator<O> {
 public:
  explicit MapIterator(Fn map, std::unique_ptr<Iterator<I>> it)
      : map_(std::move(map)), it_(std::move(it)) {}

  Status Next(O* out) override {
    I i;

    ARROW_RETURN_NOT_OK(it_->Next(&i));
    // Ensure loops exit.
    *out = (i == NULLPTR) ? NULLPTR : map_(std::move(i));

    return Status::OK();
  }

 private:
  Fn map_;
  std::unique_ptr<Iterator<I>> it_;
};

template <typename Fn, typename I, typename O = typename std::result_of<Fn(I)>::type>
std::unique_ptr<Iterator<O>> MakeMapIterator(Fn map, std::unique_ptr<Iterator<I>> it) {
  return std::unique_ptr<MapIterator<Fn, I, O>>(
      new MapIterator<Fn, I, O>(std::move(map), std::move(it)));
}

template <typename T>
class EmptyIterator : public Iterator<T> {
 public:
  explicit EmptyIterator(Status s = Status::OK()) : status_(s) {}

  Status Next(T* out) override {
    *out = NULLPTR;
    return status_;
  }

 private:
  Status status_;
};

template <typename Fn, typename T>
class FunctionIterator : public Iterator<T> {
 public:
  using IteratorType = Iterator<T>;

  explicit FunctionIterator(Fn fn) : fn_(std::move(fn)) {}

  Status Next(T* out) override { return fn_(out); }

 private:
  Fn fn_;
};

template <typename Fn, typename T = typename std::remove_pointer<
                           internal::call_traits::argument_type<0, Fn>>::type>
std::unique_ptr<FunctionIterator<Fn, T>> MakeFunctionIterator(Fn fn) {
  return std::unique_ptr<FunctionIterator<Fn, T>>(
      new FunctionIterator<Fn, T>(std::move(fn)));
}

}  // namespace arrow

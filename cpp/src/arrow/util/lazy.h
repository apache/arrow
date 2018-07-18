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
#include <iterator>
#include <utility>

namespace arrow {
namespace internal {

/// Create a range from a callable which takes a single index parameter
/// and returns the value of iterator on each call and a length.
/// Only iterators obtained from the same range should be compared, the
/// behaviour generally similar to other STL containers.
template <typename Generator>
class LazyRange {
 private:
  // callable which generates the values
  // has to be defined at the beginning of the class for type deduction
  const Generator gen_;
  // the length of the range
  int64_t length_;

 public:
  // the return type of the container
  using return_type = decltype(gen_(length_ - 1));

  /// Construct a new range from a callable and length
  LazyRange(Generator gen, int64_t length) : gen_(gen), length_(length) {}

  // Class of the dependent iterator, created implicitly by begin and end
  class RangeIter {
   public:
    using difference_type = int64_t;
    using value_type = return_type;
    using reference = value_type&;
    using pointer = value_type*;
    using iterator_category = std::forward_iterator_tag;

    RangeIter(const LazyRange<Generator>& range, int64_t index)
        : range_(range), index_(index) {}

    return_type operator*() { return range_.gen_(index_); }

    // pre-increment
    RangeIter& operator++() {
      ++index_;
      return *this;
    }

    // post-increment
    RangeIter operator++(int) {
      auto copy = RangeIter(*this);
      ++index_;
      return copy;
    }

    bool operator==(const typename LazyRange<Generator>::RangeIter& other) const {
      return this->index_ == other.index_ && &this->range_ == &other.range_;
    }

    bool operator!=(const typename LazyRange<Generator>::RangeIter& other) const {
      return this->index_ != other.index_ || &this->range_ != &other.range_;
    }

    int64_t operator-(const typename LazyRange<Generator>::RangeIter& other) {
      return this->index_ - other.index_;
    }

   private:
    // parent range reference
    const LazyRange& range_;
    // current index
    int64_t index_;
  };

  friend class RangeIter;

  // Create a new begin iterator
  RangeIter begin() { return RangeIter(*this, 0); }

  // Create a new end iterator
  RangeIter end() { return RangeIter(*this, length_); }
};

/// Helper function to create a lazy range from a callable (e.g. lambda) and length
template <typename Generator>
LazyRange<Generator> MakeLazyRange(Generator&& gen, int64_t length) {
  return LazyRange<Generator>(std::forward<Generator>(gen), length);
}

}  // namespace internal
}  // namespace arrow

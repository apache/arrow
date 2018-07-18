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

namespace arrow { namespace internal {

template<typename Generator>
class LazyIter {

    private:
        const Generator& gen_;
        int64_t index_;
        int64_t length_;
        const LazyIter<Generator>* parent_;
    public:
    

    using difference_type = int64_t;
    using value_type = decltype(gen_(index_));
    using reference = value_type&;
    using pointer = value_type*;
    using iterator_category = std::input_iterator_tag;
    static constexpr int64_t* end_reached = nullptr;

    LazyIter(const Generator& gen, int64_t length) : gen_(gen), index_(0), length_(length), parent_(nullptr) { }
    LazyIter(const LazyIter<Generator>& other) : gen_(other.gen_), index_(other.index_), length_(other.length_), parent_(other.parent_) { }
    LazyIter& operator=(const LazyIter<Generator>& other) {
        this->gen_ = other.gen_;
        this->index_ = other.index_;
        this->length_ = other.length_;
        this->parent_ = other.parent_;
        return *this;
    }
    
    decltype(gen_(index_)) operator* () { return gen_(index_); }

    void operator++ () { ++index_; }

    LazyIter<Generator> operator++(int) {
      auto copy = LazyIter<Generator>(*this);
      ++index_;
      return copy;
    }

    bool operator==(const LazyIter<Generator>& other) const {
      return at_end() && other.at_end();
    }

    bool operator!=(const LazyIter<Generator>& other) const {
      return !at_end() || !other.at_end();
    }

    int64_t operator-(const LazyIter<Generator> other) {
        if (at_end()) {
            return other.remaining();
        }
        if (other.at_end()) {
            return 0;
        }
        return length_ - other.index();
    }
    const int64_t index() const { return index_; }
    const int64_t length() const { return length_; }
    const int64_t remaining() const { return index_ == -1 ? 0 : length_ - index_; }
    const Generator& gen() const { return gen_; }
    LazyIter<Generator> make_end() const { return LazyIter<Generator>(this); }
    bool end_of(const LazyIter<Generator>* parent) const {
        return parent_ != nullptr && parent_ == parent;
    }
    bool at_end() const { return parent_ != nullptr || index_ == length_; }

    private:
        LazyIter(const LazyIter<Generator>* parent) : gen_(parent->gen_), index_(parent->length_), length_(parent->length_), parent_(parent) { }
};

template<typename Generator>
int64_t distance(LazyIter<Generator> first, LazyIter<Generator> last)
{
    return last - first;
}

template<typename Generator>
LazyIter<Generator> makeLazyIter(const Generator& gen, int64_t length)
{
    return LazyIter<Generator>(gen, length);
}
}}
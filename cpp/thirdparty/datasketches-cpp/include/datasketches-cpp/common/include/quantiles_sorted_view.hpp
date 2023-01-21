/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef QUANTILES_SORTED_VIEW_HPP_
#define QUANTILES_SORTED_VIEW_HPP_

#include <functional>
#include <cmath>

#include "common_defs.hpp"

namespace datasketches {

template<
  typename T,
  typename Comparator, // strict weak ordering function (see C++ named requirements: Compare)
  typename Allocator
>
class quantiles_sorted_view {
public:
  using Entry = typename std::conditional<std::is_arithmetic<T>::value, std::pair<T, uint64_t>, std::pair<const T*, uint64_t>>::type;
  using AllocEntry = typename std::allocator_traits<Allocator>::template rebind_alloc<Entry>;
  using Container = std::vector<Entry, AllocEntry>;

  quantiles_sorted_view(uint32_t num, const Comparator& comparator, const Allocator& allocator);

  template<typename Iterator>
  void add(Iterator begin, Iterator end, uint64_t weight);

  void convert_to_cummulative();

  class const_iterator;
  const_iterator begin() const;
  const_iterator end() const;

  size_t size() const;

  double get_rank(const T& item, bool inclusive = true) const;

  using quantile_return_type = typename std::conditional<std::is_arithmetic<T>::value, T, const T&>::type;
  quantile_return_type get_quantile(double rank, bool inclusive = true) const;

  using vector_double = std::vector<double, typename std::allocator_traits<Allocator>::template rebind_alloc<double>>;
  vector_double get_CDF(const T* split_points, uint32_t size, bool inclusive = true) const;
  vector_double get_PMF(const T* split_points, uint32_t size, bool inclusive = true) const;

private:
  Comparator comparator_;
  uint64_t total_weight_;
  Container entries_;

  static inline const T& deref_helper(const T* t) { return *t; }
  static inline T deref_helper(T t) { return t; }

  struct compare_pairs_by_first {
    explicit compare_pairs_by_first(const Comparator& comparator): comparator_(comparator) {}
    bool operator()(const Entry& a, const Entry& b) const {
      return comparator_(deref_helper(a.first), deref_helper(b.first));
    }
    Comparator comparator_;
  };

  struct compare_pairs_by_second {
    bool operator()(const Entry& a, const Entry& b) const {
      return a.second < b.second;
    }
  };

  template<typename TT = T, typename std::enable_if<std::is_arithmetic<TT>::value, int>::type = 0>
  static inline T ref_helper(const T& t) { return t; }

  template<typename TT = T, typename std::enable_if<!std::is_arithmetic<TT>::value, int>::type = 0>
  static inline const T* ref_helper(const T& t) { return std::addressof(t); }

  template<typename TT = T, typename std::enable_if<std::is_arithmetic<TT>::value, int>::type = 0>
  static inline Entry make_dummy_entry(uint64_t weight) { return Entry(0, weight); }

  template<typename TT = T, typename std::enable_if<!std::is_arithmetic<TT>::value, int>::type = 0>
  static inline Entry make_dummy_entry(uint64_t weight) { return Entry(nullptr, weight); }

  template<typename TT = T, typename std::enable_if<std::is_floating_point<TT>::value, int>::type = 0>
  static inline void check_split_points(const T* items, uint32_t size) {
    for (uint32_t i = 0; i < size ; i++) {
      if (std::isnan(items[i])) {
        throw std::invalid_argument("Values must not be NaN");
      }
      if ((i < (size - 1)) && !(Comparator()(items[i], items[i + 1]))) {
        throw std::invalid_argument("Values must be unique and monotonically increasing");
      }
    }
  }

  template<typename TT = T, typename std::enable_if<!std::is_floating_point<TT>::value, int>::type = 0>
  static inline void check_split_points(const T* items, uint32_t size) {
    for (uint32_t i = 0; i < size ; i++) {
      if ((i < (size - 1)) && !(Comparator()(items[i], items[i + 1]))) {
        throw std::invalid_argument("Items must be unique and monotonically increasing");
      }
    }
  }
};

template<typename T, typename C, typename A>
class quantiles_sorted_view<T, C, A>::const_iterator: public quantiles_sorted_view<T, C, A>::Container::const_iterator {
public:
  using Base = typename quantiles_sorted_view<T, C, A>::Container::const_iterator;
  using value_type = typename std::conditional<std::is_arithmetic<T>::value, typename Base::value_type, std::pair<const T&, const uint64_t>>::type;

  const_iterator(const Base& it, const Base& begin): Base(it), begin(begin) {}

  template<typename TT = T, typename std::enable_if<std::is_arithmetic<TT>::value, int>::type = 0>
  const value_type operator*() const { return Base::operator*(); }

  template<typename TT = T, typename std::enable_if<!std::is_arithmetic<TT>::value, int>::type = 0>
  const value_type operator*() const { return value_type(*(Base::operator*().first), Base::operator*().second); }

  template<typename TT = T, typename std::enable_if<std::is_arithmetic<TT>::value, int>::type = 0>
  const value_type* operator->() const { return Base::operator->(); }

  template<typename TT = T, typename std::enable_if<!std::is_arithmetic<TT>::value, int>::type = 0>
  const return_value_holder<value_type> operator->() const { return **this; }

  uint64_t get_weight() const {
    if (*this == begin) return Base::operator*().second;
    return Base::operator*().second - (*this - 1).operator*().second;
  }

  uint64_t get_cumulative_weight(bool inclusive = true) const {
    return inclusive ? Base::operator*().second : Base::operator*().second - get_weight();
  }

private:
  Base begin;
};

} /* namespace datasketches */

#include "quantiles_sorted_view_impl.hpp"

#endif

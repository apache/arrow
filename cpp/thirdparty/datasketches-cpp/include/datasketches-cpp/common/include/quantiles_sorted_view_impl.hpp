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

#ifndef QUANTILES_SORTED_VIEW_IMPL_HPP_
#define QUANTILES_SORTED_VIEW_IMPL_HPP_

#include <algorithm>
#include <stdexcept>
#include <cmath>

namespace datasketches {

template<typename T, typename C, typename A>
quantiles_sorted_view<T, C, A>::quantiles_sorted_view(uint32_t num, const C& comparator, const A& allocator):
comparator_(comparator),
total_weight_(0),
entries_(allocator)
{
  entries_.reserve(num);
}

template<typename T, typename C, typename A>
template<typename Iterator>
void quantiles_sorted_view<T, C, A>::add(Iterator first, Iterator last, uint64_t weight) {
  const size_t size_before = entries_.size();
  for (auto it = first; it != last; ++it) entries_.push_back(Entry(ref_helper(*it), weight));
  if (size_before > 0) {
    Container tmp(entries_.get_allocator());
    tmp.reserve(entries_.capacity());
    std::merge(
        entries_.begin(), entries_.begin() + size_before,
        entries_.begin() + size_before, entries_.end(),
        std::back_inserter(tmp), compare_pairs_by_first(comparator_)
    );
    std::swap(tmp, entries_);
  }
}

template<typename T, typename C, typename A>
void quantiles_sorted_view<T, C, A>::convert_to_cummulative() {
  for (auto& entry: entries_) {
    total_weight_ += entry.second;
    entry.second = total_weight_;
  }
}

template<typename T, typename C, typename A>
double quantiles_sorted_view<T, C, A>::get_rank(const T& item, bool inclusive) const {
  if (entries_.empty()) throw std::runtime_error("operation is undefined for an empty sketch");
  auto it = inclusive ?
      std::upper_bound(entries_.begin(), entries_.end(), Entry(ref_helper(item), 0), compare_pairs_by_first(comparator_))
    : std::lower_bound(entries_.begin(), entries_.end(), Entry(ref_helper(item), 0), compare_pairs_by_first(comparator_));
  // we need item just before
  if (it == entries_.begin()) return 0;
  --it;
  return static_cast<double>(it->second) / total_weight_;
}

template<typename T, typename C, typename A>
auto quantiles_sorted_view<T, C, A>::get_quantile(double rank, bool inclusive) const -> quantile_return_type {
  if (entries_.empty()) throw std::runtime_error("operation is undefined for an empty sketch");
  uint64_t weight = inclusive ? std::ceil(rank * total_weight_) : rank * total_weight_;
  auto it = inclusive ?
      std::lower_bound(entries_.begin(), entries_.end(), make_dummy_entry<T>(weight), compare_pairs_by_second())
    : std::upper_bound(entries_.begin(), entries_.end(), make_dummy_entry<T>(weight), compare_pairs_by_second());
  if (it == entries_.end()) return deref_helper(entries_[entries_.size() - 1].first);
  return deref_helper(it->first);
}

template<typename T, typename C, typename A>
auto quantiles_sorted_view<T, C, A>::get_CDF(const T* split_points, uint32_t size, bool inclusive) const -> vector_double {
  if (entries_.empty()) throw std::runtime_error("operation is undefined for an empty sketch");
  vector_double buckets(entries_.get_allocator());
  if (entries_.size() == 0) return buckets;
  check_split_points(split_points, size);
  buckets.reserve(size + 1);
  for (uint32_t i = 0; i < size; ++i) buckets.push_back(get_rank(split_points[i], inclusive));
  buckets.push_back(1);
  return buckets;
}

template<typename T, typename C, typename A>
auto quantiles_sorted_view<T, C, A>::get_PMF(const T* split_points, uint32_t size, bool inclusive) const -> vector_double {
  auto buckets = get_CDF(split_points, size, inclusive);
  if (buckets.size() == 0) return buckets;
  for (uint32_t i = size; i > 0; --i) {
    buckets[i] -= buckets[i - 1];
  }
  return buckets;
}

template<typename T, typename C, typename A>
auto quantiles_sorted_view<T, C, A>::begin() const -> const_iterator {
  return const_iterator(entries_.begin(), entries_.begin());
}

template<typename T, typename C, typename A>
auto quantiles_sorted_view<T, C, A>::end() const -> const_iterator {
  return const_iterator(entries_.end(), entries_.begin());
}

template<typename T, typename C, typename A>
size_t quantiles_sorted_view<T, C, A>::size() const {
  return entries_.size();
}

} /* namespace datasketches */

#endif

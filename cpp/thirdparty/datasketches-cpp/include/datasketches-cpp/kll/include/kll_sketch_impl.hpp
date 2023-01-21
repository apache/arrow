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

#ifndef KLL_SKETCH_IMPL_HPP_
#define KLL_SKETCH_IMPL_HPP_

#include <iostream>
#include <iomanip>
#include <sstream>
#include <stdexcept>

#include "conditional_forward.hpp"
#include "count_zeros.hpp"
#include "memory_operations.hpp"
#include "kll_helper.hpp"

namespace datasketches {

template<typename T, typename C, typename A>
kll_sketch<T, C, A>::kll_sketch(uint16_t k, const C& comparator, const A& allocator):
comparator_(comparator),
allocator_(allocator),
k_(k),
m_(DEFAULT_M),
min_k_(k),
num_levels_(1),
is_level_zero_sorted_(false),
n_(0),
levels_(2, 0, allocator),
items_(nullptr),
items_size_(k_),
min_item_(nullptr),
max_item_(nullptr),
sorted_view_(nullptr)
{
  if (k < MIN_K || k > MAX_K) {
    throw std::invalid_argument("K must be >= " + std::to_string(MIN_K) + " and <= " + std::to_string(MAX_K) + ": " + std::to_string(k));
  }
  levels_[0] = levels_[1] = k;
  items_ = allocator_.allocate(items_size_);
}

template<typename T, typename C, typename A>
kll_sketch<T, C, A>::kll_sketch(const kll_sketch& other):
comparator_(other.comparator_),
allocator_(other.allocator_),
k_(other.k_),
m_(other.m_),
min_k_(other.min_k_),
num_levels_(other.num_levels_),
is_level_zero_sorted_(other.is_level_zero_sorted_),
n_(other.n_),
levels_(other.levels_),
items_(nullptr),
items_size_(other.items_size_),
min_item_(nullptr),
max_item_(nullptr),
sorted_view_(nullptr)
{
  items_ = allocator_.allocate(items_size_);
  for (auto i = levels_[0]; i < levels_[num_levels_]; ++i) new (&items_[i]) T(other.items_[i]);
  if (other.min_item_ != nullptr) min_item_ = new (allocator_.allocate(1)) T(*other.min_item_);
  if (other.max_item_ != nullptr) max_item_ = new (allocator_.allocate(1)) T(*other.max_item_);
}

template<typename T, typename C, typename A>
kll_sketch<T, C, A>::kll_sketch(kll_sketch&& other) noexcept:
comparator_(std::move(other.comparator_)),
allocator_(std::move(other.allocator_)),
k_(other.k_),
m_(other.m_),
min_k_(other.min_k_),
num_levels_(other.num_levels_),
is_level_zero_sorted_(other.is_level_zero_sorted_),
n_(other.n_),
levels_(std::move(other.levels_)),
items_(other.items_),
items_size_(other.items_size_),
min_item_(other.min_item_),
max_item_(other.max_item_),
sorted_view_(nullptr)
{
  other.items_ = nullptr;
  other.min_item_ = nullptr;
  other.max_item_ = nullptr;
}

template<typename T, typename C, typename A>
kll_sketch<T, C, A>& kll_sketch<T, C, A>::operator=(const kll_sketch& other) {
  kll_sketch copy(other);
  std::swap(comparator_, copy.comparator_);
  std::swap(allocator_, copy.allocator_);
  std::swap(k_, copy.k_);
  std::swap(m_, copy.m_);
  std::swap(min_k_, copy.min_k_);
  std::swap(num_levels_, copy.num_levels_);
  std::swap(is_level_zero_sorted_, copy.is_level_zero_sorted_);
  std::swap(n_, copy.n_);
  std::swap(levels_, copy.levels_);
  std::swap(items_, copy.items_);
  std::swap(items_size_, copy.items_size_);
  std::swap(min_item_, copy.min_item_);
  std::swap(max_item_, copy.max_item_);
  reset_sorted_view();
  return *this;
}

template<typename T, typename C, typename A>
kll_sketch<T, C, A>& kll_sketch<T, C, A>::operator=(kll_sketch&& other) {
  std::swap(comparator_, other.comparator_);
  std::swap(allocator_, other.allocator_);
  std::swap(k_, other.k_);
  std::swap(m_, other.m_);
  std::swap(min_k_, other.min_k_);
  std::swap(num_levels_, other.num_levels_);
  std::swap(is_level_zero_sorted_, other.is_level_zero_sorted_);
  std::swap(n_, other.n_);
  std::swap(levels_, other.levels_);
  std::swap(items_, other.items_);
  std::swap(items_size_, other.items_size_);
  std::swap(min_item_, other.min_item_);
  std::swap(max_item_, other.max_item_);
  reset_sorted_view();
  return *this;
}

template<typename T, typename C, typename A>
kll_sketch<T, C, A>::~kll_sketch() {
  if (items_ != nullptr) {
    const uint32_t begin = levels_[0];
    const uint32_t end = levels_[num_levels_];
    for (uint32_t i = begin; i < end; i++) items_[i].~T();
    allocator_.deallocate(items_, items_size_);
  }
  if (min_item_ != nullptr) {
    min_item_->~T();
    allocator_.deallocate(min_item_, 1);
  }
  if (max_item_ != nullptr) {
    max_item_->~T();
    allocator_.deallocate(max_item_, 1);
  }
  reset_sorted_view();
}

template<typename T, typename C, typename A>
template<typename TT, typename CC, typename AA>
kll_sketch<T, C, A>::kll_sketch(const kll_sketch<TT, CC, AA>& other, const C& comparator, const A& allocator):
comparator_(comparator),
allocator_(allocator),
k_(other.k_),
m_(other.m_),
min_k_(other.min_k_),
num_levels_(other.num_levels_),
is_level_zero_sorted_(other.is_level_zero_sorted_),
n_(other.n_),
levels_(other.levels_, allocator_),
items_(nullptr),
items_size_(other.items_size_),
min_item_(nullptr),
max_item_(nullptr),
sorted_view_(nullptr)
{
  static_assert(
    std::is_constructible<T, TT>::value,
    "Type converting constructor requires new type to be constructible from existing type"
  );
  items_ = allocator_.allocate(items_size_);
  for (auto i = levels_[0]; i < levels_[num_levels_]; ++i) new (&items_[i]) T(other.items_[i]);
  if (other.min_item_ != nullptr) min_item_ = new (allocator_.allocate(1)) T(*other.min_item_);
  if (other.max_item_ != nullptr) max_item_ = new (allocator_.allocate(1)) T(*other.max_item_);
  check_sorting();
}

template<typename T, typename C, typename A>
template<typename FwdT>
void kll_sketch<T, C, A>::update(FwdT&& item) {
  if (!check_update_item(item)) { return; }
  update_min_max(item);
  const uint32_t index = internal_update();
  new (&items_[index]) T(std::forward<FwdT>(item));
  reset_sorted_view();
}

template<typename T, typename C, typename A>
void kll_sketch<T, C, A>::update_min_max(const T& item) {
  if (is_empty()) {
    min_item_ = new (allocator_.allocate(1)) T(item);
    max_item_ = new (allocator_.allocate(1)) T(item);
  } else {
    if (comparator_(item, *min_item_)) *min_item_ = item;
    if (comparator_(*max_item_, item)) *max_item_ = item;
  }
}

template<typename T, typename C, typename A>
uint32_t kll_sketch<T, C, A>::internal_update() {
  if (levels_[0] == 0) compress_while_updating();
  n_++;
  is_level_zero_sorted_ = false;
  return --levels_[0];
}

template<typename T, typename C, typename A>
template<typename FwdSk>
void kll_sketch<T, C, A>::merge(FwdSk&& other) {
  if (other.is_empty()) return;
  if (m_ != other.m_) {
    throw std::invalid_argument("incompatible M: " + std::to_string(m_) + " and " + std::to_string(other.m_));
  }
  if (is_empty()) {
    min_item_ = new (allocator_.allocate(1)) T(conditional_forward<FwdSk>(*other.min_item_));
    max_item_ = new (allocator_.allocate(1)) T(conditional_forward<FwdSk>(*other.max_item_));
  } else {
    if (comparator_(*other.min_item_, *min_item_)) *min_item_ = conditional_forward<FwdSk>(*other.min_item_);
    if (comparator_(*max_item_, *other.max_item_)) *max_item_ = conditional_forward<FwdSk>(*other.max_item_);
  }
  const uint64_t final_n = n_ + other.n_;
  for (uint32_t i = other.levels_[0]; i < other.levels_[1]; i++) {
    const uint32_t index = internal_update();
    new (&items_[index]) T(conditional_forward<FwdSk>(other.items_[i]));
  }
  if (other.num_levels_ >= 2) merge_higher_levels(other, final_n);
  n_ = final_n;
  if (other.is_estimation_mode()) min_k_ = std::min(min_k_, other.min_k_);
  assert_correct_total_weight();
  reset_sorted_view();
}

template<typename T, typename C, typename A>
bool kll_sketch<T, C, A>::is_empty() const {
  return n_ == 0;
}

template<typename T, typename C, typename A>
uint16_t kll_sketch<T, C, A>::get_k() const {
  return k_;
}

template<typename T, typename C, typename A>
uint64_t kll_sketch<T, C, A>::get_n() const {
  return n_;
}

template<typename T, typename C, typename A>
uint32_t kll_sketch<T, C, A>::get_num_retained() const {
  return levels_[num_levels_] - levels_[0];
}

template<typename T, typename C, typename A>
bool kll_sketch<T, C, A>::is_estimation_mode() const {
  return num_levels_ > 1;
}

template<typename T, typename C, typename A>
T kll_sketch<T, C, A>::get_min_item() const {
  if (is_empty()) throw std::runtime_error("operation is undefined for an empty sketch");
  return *min_item_;
}

template<typename T, typename C, typename A>
T kll_sketch<T, C, A>::get_max_item() const {
  if (is_empty()) throw std::runtime_error("operation is undefined for an empty sketch");
  return *max_item_;
}

template<typename T, typename C, typename A>
C kll_sketch<T, C, A>::get_comparator() const {
  return comparator_;
}

template<typename T, typename C, typename A>
A kll_sketch<T, C, A>::get_allocator() const {
  return allocator_;
}

template<typename T, typename C, typename A>
double kll_sketch<T, C, A>::get_rank(const T& item, bool inclusive) const {
  if (is_empty()) throw std::runtime_error("operation is undefined for an empty sketch");
  setup_sorted_view();
  return sorted_view_->get_rank(item, inclusive);
}

template<typename T, typename C, typename A>
auto kll_sketch<T, C, A>::get_PMF(const T* split_points, uint32_t size, bool inclusive) const -> vector_double {
  if (is_empty()) throw std::runtime_error("operation is undefined for an empty sketch");
  setup_sorted_view();
  return sorted_view_->get_PMF(split_points, size, inclusive);
}

template<typename T, typename C, typename A>
auto kll_sketch<T, C, A>::get_CDF(const T* split_points, uint32_t size, bool inclusive) const -> vector_double {
  if (is_empty()) throw std::runtime_error("operation is undefined for an empty sketch");
  setup_sorted_view();
  return sorted_view_->get_CDF(split_points, size, inclusive);
}

template<typename T, typename C, typename A>
auto kll_sketch<T, C, A>::get_quantile(double rank, bool inclusive) const -> quantile_return_type {
  if (is_empty()) throw std::runtime_error("operation is undefined for an empty sketch");
  if ((rank < 0.0) || (rank > 1.0)) {
    throw std::invalid_argument("normalized rank cannot be less than zero or greater than 1.0");
  }
  // may have a side effect of sorting level zero if needed
  setup_sorted_view();
  return sorted_view_->get_quantile(rank, inclusive);
}

template<typename T, typename C, typename A>
std::vector<T, A> kll_sketch<T, C, A>::get_quantiles(const double* ranks, uint32_t size, bool inclusive) const {
  if (is_empty()) throw std::runtime_error("operation is undefined for an empty sketch");
  std::vector<T, A> quantiles(allocator_);
  quantiles.reserve(size);

  // may have a side effect of sorting level zero if needed
  setup_sorted_view();

  for (uint32_t i = 0; i < size; i++) {
    const double rank = ranks[i];
    if ((rank < 0.0) || (rank > 1.0)) {
      throw std::invalid_argument("normalized rank cannot be less than 0 or greater than 1");
    }
    quantiles.push_back(sorted_view_->get_quantile(rank, inclusive));
  }
  return quantiles;
}

template<typename T, typename C, typename A>
std::vector<T, A> kll_sketch<T, C, A>::get_quantiles(uint32_t num, bool inclusive) const {
  if (is_empty()) throw std::runtime_error("operation is undefined for an empty sketch");
  if (num == 0) {
    throw std::invalid_argument("num must be > 0");
  }
  vector_double ranks(num, 0, allocator_);
  ranks[0] = 0.0;
  for (size_t i = 1; i < num; i++) {
    ranks[i] = static_cast<double>(i) / (num - 1);
  }
  if (num > 1) {
    ranks[num - 1] = 1.0;
  }
  return get_quantiles(ranks.data(), num, inclusive);
}

template<typename T, typename C, typename A>
double kll_sketch<T, C, A>::get_normalized_rank_error(bool pmf) const {
  return get_normalized_rank_error(min_k_, pmf);
}

// implementation for fixed-size arithmetic types (integral and floating point)
template<typename T, typename C, typename A>
template<typename TT, typename SerDe, typename std::enable_if<std::is_arithmetic<TT>::value, int>::type>
size_t kll_sketch<T, C, A>::get_serialized_size_bytes(const SerDe&) const {
  if (is_empty()) { return EMPTY_SIZE_BYTES; }
  if (num_levels_ == 1 && get_num_retained() == 1) {
    return DATA_START_SINGLE_ITEM + sizeof(TT);
  }
  // the last integer in the levels_ array is not serialized because it can be derived
  return DATA_START + num_levels_ * sizeof(uint32_t) + (get_num_retained() + 2) * sizeof(TT);
}

// implementation for all other types
template<typename T, typename C, typename A>
template<typename TT, typename SerDe, typename std::enable_if<!std::is_arithmetic<TT>::value, int>::type>
size_t kll_sketch<T, C, A>::get_serialized_size_bytes(const SerDe& sd) const {
  if (is_empty()) { return EMPTY_SIZE_BYTES; }
  if (num_levels_ == 1 && get_num_retained() == 1) {
    return DATA_START_SINGLE_ITEM + sd.size_of_item(items_[levels_[0]]);
  }
  // the last integer in the levels_ array is not serialized because it can be derived
  size_t size = DATA_START + num_levels_ * sizeof(uint32_t);
  size += sd.size_of_item(*min_item_);
  size += sd.size_of_item(*max_item_);
  for (auto it: *this) size += sd.size_of_item(it.first);
  return size;
}

// implementation for fixed-size arithmetic types (integral and floating point)
template<typename T, typename C, typename A>
template<typename TT, typename std::enable_if<std::is_arithmetic<TT>::value, int>::type>
size_t kll_sketch<T, C, A>::get_max_serialized_size_bytes(uint16_t k, uint64_t n) {
  const uint8_t num_levels = kll_helper::ub_on_num_levels(n);
  const uint32_t max_num_retained = kll_helper::compute_total_capacity(k, DEFAULT_M, num_levels);
  // the last integer in the levels_ array is not serialized because it can be derived
  return DATA_START + num_levels * sizeof(uint32_t) + (max_num_retained + 2) * sizeof(TT);
}

// implementation for all other types
template<typename T, typename C, typename A>
template<typename TT, typename std::enable_if<!std::is_arithmetic<TT>::value, int>::type>
size_t kll_sketch<T, C, A>::get_max_serialized_size_bytes(uint16_t k, uint64_t n, size_t max_item_size_bytes) {
  const uint8_t num_levels = kll_helper::ub_on_num_levels(n);
  const uint32_t max_num_retained = kll_helper::compute_total_capacity(k, DEFAULT_M, num_levels);
  // the last integer in the levels_ array is not serialized because it can be derived
  return DATA_START + num_levels * sizeof(uint32_t) + (max_num_retained + 2) * max_item_size_bytes;
}

template<typename T, typename C, typename A>
template<typename SerDe>
void kll_sketch<T, C, A>::serialize(std::ostream& os, const SerDe& sd) const {
  const bool is_single_item = n_ == 1;
  const uint8_t preamble_ints(is_empty() || is_single_item ? PREAMBLE_INTS_SHORT : PREAMBLE_INTS_FULL);
  write(os, preamble_ints);
  const uint8_t serial_version(is_single_item ? SERIAL_VERSION_2 : SERIAL_VERSION_1);
  write(os, serial_version);
  const uint8_t family(FAMILY);
  write(os, family);
  const uint8_t flags_byte(
      (is_empty() ? 1 << flags::IS_EMPTY : 0)
    | (is_level_zero_sorted_ ? 1 << flags::IS_LEVEL_ZERO_SORTED : 0)
    | (is_single_item ? 1 << flags::IS_SINGLE_ITEM : 0)
  );
  write(os, flags_byte);
  write(os, k_);
  write(os, m_);
  const uint8_t unused = 0;
  write(os, unused);
  if (is_empty()) return;
  if (!is_single_item) {
    write(os, n_);
    write(os, min_k_);
    write(os, num_levels_);
    write(os, unused);
    write(os, levels_.data(), sizeof(levels_[0]) * num_levels_);
    sd.serialize(os, min_item_, 1);
    sd.serialize(os, max_item_, 1);
  }
  sd.serialize(os, &items_[levels_[0]], get_num_retained());
}

template<typename T, typename C, typename A>
template<typename SerDe>
auto kll_sketch<T, C, A>::serialize(unsigned header_size_bytes, const SerDe& sd) const -> vector_bytes {
  const bool is_single_item = n_ == 1;
  const size_t size = header_size_bytes + get_serialized_size_bytes(sd);
  vector_bytes bytes(size, 0, allocator_);
  uint8_t* ptr = bytes.data() + header_size_bytes;
  const uint8_t* end_ptr = ptr + size;
  const uint8_t preamble_ints(is_empty() || is_single_item ? PREAMBLE_INTS_SHORT : PREAMBLE_INTS_FULL);
  ptr += copy_to_mem(preamble_ints, ptr);
  const uint8_t serial_version(is_single_item ? SERIAL_VERSION_2 : SERIAL_VERSION_1);
  ptr += copy_to_mem(serial_version, ptr);
  const uint8_t family(FAMILY);
  ptr += copy_to_mem(family, ptr);
  const uint8_t flags_byte(
      (is_empty() ? 1 << flags::IS_EMPTY : 0)
    | (is_level_zero_sorted_ ? 1 << flags::IS_LEVEL_ZERO_SORTED : 0)
    | (is_single_item ? 1 << flags::IS_SINGLE_ITEM : 0)
  );
  ptr += copy_to_mem(flags_byte, ptr);
  ptr += copy_to_mem(k_, ptr);
  ptr += copy_to_mem(m_, ptr);
  ptr += sizeof(uint8_t); // unused
  if (!is_empty()) {
    if (!is_single_item) {
      ptr += copy_to_mem(n_, ptr);
      ptr += copy_to_mem(min_k_, ptr);
      ptr += copy_to_mem(num_levels_, ptr);
      ptr += sizeof(uint8_t); // unused
      ptr += copy_to_mem(levels_.data(), ptr, sizeof(levels_[0]) * num_levels_);
      ptr += sd.serialize(ptr, end_ptr - ptr, min_item_, 1);
      ptr += sd.serialize(ptr, end_ptr - ptr, max_item_, 1);
    }
    const size_t bytes_remaining = end_ptr - ptr;
    ptr += sd.serialize(ptr, bytes_remaining, &items_[levels_[0]], get_num_retained());
  }
  const size_t delta = ptr - bytes.data();
  if (delta != size) throw std::logic_error("serialized size mismatch: " + std::to_string(delta)
      + " != " + std::to_string(size));
  return bytes;
}

template<typename T, typename C, typename A>
template<typename SerDe>
kll_sketch<T, C, A> kll_sketch<T, C, A>::deserialize(std::istream& is, const SerDe& sd,
    const C& comparator, const A& allocator) {
  const auto preamble_ints = read<uint8_t>(is);
  const auto serial_version = read<uint8_t>(is);
  const auto family_id = read<uint8_t>(is);
  const auto flags_byte = read<uint8_t>(is);
  const auto k = read<uint16_t>(is);
  const auto m = read<uint8_t>(is);
  read<uint8_t>(is); // skip unused byte

  check_m(m);
  check_preamble_ints(preamble_ints, flags_byte);
  check_serial_version(serial_version);
  check_family_id(family_id);

  if (!is.good()) throw std::runtime_error("error reading from std::istream");
  const bool is_empty(flags_byte & (1 << flags::IS_EMPTY));
  if (is_empty) return kll_sketch(k, comparator, allocator);

  uint64_t n;
  uint16_t min_k;
  uint8_t num_levels;
  const bool is_single_item(flags_byte & (1 << flags::IS_SINGLE_ITEM)); // used in serial version 2
  if (is_single_item) {
    n = 1;
    min_k = k;
    num_levels = 1;
  } else {
    n = read<uint64_t>(is);
    min_k = read<uint16_t>(is);
    num_levels = read<uint8_t>(is);
    read<uint8_t>(is); // skip unused byte
  }
  vector_u32 levels(num_levels + 1, 0, allocator);
  const uint32_t capacity(kll_helper::compute_total_capacity(k, m, num_levels));
  if (is_single_item) {
    levels[0] = capacity - 1;
  } else {
    // the last integer in levels_ is not serialized because it can be derived
    read(is, levels.data(), sizeof(levels[0]) * num_levels);
  }
  levels[num_levels] = capacity;
  A alloc(allocator);
  auto item_buffer_deleter = [&alloc](T* ptr) { alloc.deallocate(ptr, 1); };
  std::unique_ptr<T, decltype(item_buffer_deleter)> min_item_buffer(alloc.allocate(1), item_buffer_deleter);
  std::unique_ptr<T, decltype(item_buffer_deleter)> max_item_buffer(alloc.allocate(1), item_buffer_deleter);
  std::unique_ptr<T, item_deleter> min_item(nullptr, item_deleter(allocator));
  std::unique_ptr<T, item_deleter> max_item(nullptr, item_deleter(allocator));
  if (!is_single_item) {
    sd.deserialize(is, min_item_buffer.get(), 1);
    // serde call did not throw, repackage with destrtuctor
    min_item = std::unique_ptr<T, item_deleter>(min_item_buffer.release(), item_deleter(allocator));
    sd.deserialize(is, max_item_buffer.get(), 1);
    // serde call did not throw, repackage with destrtuctor
    max_item = std::unique_ptr<T, item_deleter>(max_item_buffer.release(), item_deleter(allocator));
  }
  auto items_buffer_deleter = [capacity, &alloc](T* ptr) { alloc.deallocate(ptr, capacity); };
  std::unique_ptr<T, decltype(items_buffer_deleter)> items_buffer(alloc.allocate(capacity), items_buffer_deleter);
  const auto num_items = levels[num_levels] - levels[0];
  sd.deserialize(is, &items_buffer.get()[levels[0]], num_items);
  // serde call did not throw, repackage with destrtuctors
  std::unique_ptr<T, items_deleter> items(items_buffer.release(), items_deleter(levels[0], capacity, allocator));
  const bool is_level_zero_sorted = (flags_byte & (1 << flags::IS_LEVEL_ZERO_SORTED)) > 0;
  if (is_single_item) {
    new (min_item_buffer.get()) T(items.get()[levels[0]]);
    // copy did not throw, repackage with destrtuctor
    min_item = std::unique_ptr<T, item_deleter>(min_item_buffer.release(), item_deleter(allocator));
    new (max_item_buffer.get()) T(items.get()[levels[0]]);
    // copy did not throw, repackage with destrtuctor
    max_item = std::unique_ptr<T, item_deleter>(max_item_buffer.release(), item_deleter(allocator));
  }
  if (!is.good())
    throw std::runtime_error("error reading from std::istream");
  return kll_sketch(k, min_k, n, num_levels, std::move(levels), std::move(items), capacity,
      std::move(min_item), std::move(max_item), is_level_zero_sorted, comparator);
}

template<typename T, typename C, typename A>
template<typename SerDe>
kll_sketch<T, C, A> kll_sketch<T, C, A>::deserialize(const void* bytes, size_t size, const SerDe& sd,
    const C& comparator, const A& allocator) {
  ensure_minimum_memory(size, 8);
  const char* ptr = static_cast<const char*>(bytes);
  uint8_t preamble_ints;
  ptr += copy_from_mem(ptr, preamble_ints);
  uint8_t serial_version;
  ptr += copy_from_mem(ptr, serial_version);
  uint8_t family_id;
  ptr += copy_from_mem(ptr, family_id);
  uint8_t flags_byte;
  ptr += copy_from_mem(ptr, flags_byte);
  uint16_t k;
  ptr += copy_from_mem(ptr, k);
  uint8_t m;
  ptr += copy_from_mem(ptr, m);
  ptr += sizeof(uint8_t); // skip unused byte

  check_m(m);
  check_preamble_ints(preamble_ints, flags_byte);
  check_serial_version(serial_version);
  check_family_id(family_id);
  ensure_minimum_memory(size, preamble_ints * sizeof(uint32_t));

  const bool is_empty(flags_byte & (1 << flags::IS_EMPTY));
  if (is_empty) return kll_sketch(k, comparator, allocator);

  uint64_t n;
  uint16_t min_k;
  uint8_t num_levels;
  const bool is_single_item(flags_byte & (1 << flags::IS_SINGLE_ITEM)); // used in serial version 2
  const char* end_ptr = static_cast<const char*>(bytes) + size;
  if (is_single_item) {
    n = 1;
    min_k = k;
    num_levels = 1;
  } else {
    ptr += copy_from_mem(ptr, n);
    ptr += copy_from_mem(ptr, min_k);
    ptr += copy_from_mem(ptr, num_levels);
    ptr += sizeof(uint8_t); // skip unused byte
  }
  vector_u32 levels(num_levels + 1, 0, allocator);
  const uint32_t capacity(kll_helper::compute_total_capacity(k, m, num_levels));
  if (is_single_item) {
    levels[0] = capacity - 1;
  } else {
    // the last integer in levels_ is not serialized because it can be derived
    ptr += copy_from_mem(ptr, levels.data(), sizeof(levels[0]) * num_levels);
  }
  levels[num_levels] = capacity;
  A alloc(allocator);
  auto item_buffer_deleter = [&alloc](T* ptr) { alloc.deallocate(ptr, 1); };
  std::unique_ptr<T, decltype(item_buffer_deleter)> min_item_buffer(alloc.allocate(1), item_buffer_deleter);
  std::unique_ptr<T, decltype(item_buffer_deleter)> max_item_buffer(alloc.allocate(1), item_buffer_deleter);
  std::unique_ptr<T, item_deleter> min_item(nullptr, item_deleter(allocator));
  std::unique_ptr<T, item_deleter> max_item(nullptr, item_deleter(allocator));
  if (!is_single_item) {
    ptr += sd.deserialize(ptr, end_ptr - ptr, min_item_buffer.get(), 1);
    // serde call did not throw, repackage with destrtuctor
    min_item = std::unique_ptr<T, item_deleter>(min_item_buffer.release(), item_deleter(allocator));
    ptr += sd.deserialize(ptr, end_ptr - ptr, max_item_buffer.get(), 1);
    // serde call did not throw, repackage with destrtuctor
    max_item = std::unique_ptr<T, item_deleter>(max_item_buffer.release(), item_deleter(allocator));
  }
  auto items_buffer_deleter = [capacity, &alloc](T* ptr) { alloc.deallocate(ptr, capacity); };
  std::unique_ptr<T, decltype(items_buffer_deleter)> items_buffer(alloc.allocate(capacity), items_buffer_deleter);
  const auto num_items = levels[num_levels] - levels[0];
  ptr += sd.deserialize(ptr, end_ptr - ptr, &items_buffer.get()[levels[0]], num_items);
  // serde call did not throw, repackage with destrtuctors
  std::unique_ptr<T, items_deleter> items(items_buffer.release(), items_deleter(levels[0], capacity, allocator));
  const size_t delta = ptr - static_cast<const char*>(bytes);
  if (delta != size) throw std::logic_error("deserialized size mismatch: " + std::to_string(delta) + " != " + std::to_string(size));
  const bool is_level_zero_sorted = (flags_byte & (1 << flags::IS_LEVEL_ZERO_SORTED)) > 0;
  if (is_single_item) {
    new (min_item_buffer.get()) T(items.get()[levels[0]]);
    // copy did not throw, repackage with destrtuctor
    min_item = std::unique_ptr<T, item_deleter>(min_item_buffer.release(), item_deleter(allocator));
    new (max_item_buffer.get()) T(items.get()[levels[0]]);
    // copy did not throw, repackage with destrtuctor
    max_item = std::unique_ptr<T, item_deleter>(max_item_buffer.release(), item_deleter(allocator));
  }
  return kll_sketch(k, min_k, n, num_levels, std::move(levels), std::move(items), capacity,
      std::move(min_item), std::move(max_item), is_level_zero_sorted, comparator);
}

/*
 * Gets the normalized rank error given k and pmf.
 * k - the configuration parameter
 * pmf - if true, returns the "double-sided" normalized rank error for the get_PMF() function.
 * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
 * Constants were derived as the best fit to 99 percentile empirically measured max error in thousands of trials
 */
template<typename T, typename C, typename A>
double kll_sketch<T, C, A>::get_normalized_rank_error(uint16_t k, bool pmf) {
  return pmf
      ? 2.446 / pow(k, 0.9433)
      : 2.296 / pow(k, 0.9723);
}

// for deserialization
template<typename T, typename C, typename A>
kll_sketch<T, C, A>::kll_sketch(uint16_t k, uint16_t min_k, uint64_t n, uint8_t num_levels, vector_u32&& levels,
    std::unique_ptr<T, items_deleter> items, uint32_t items_size, std::unique_ptr<T, item_deleter> min_item,
    std::unique_ptr<T, item_deleter> max_item, bool is_level_zero_sorted, const C& comparator):
comparator_(comparator),
allocator_(levels.get_allocator()),
k_(k),
m_(DEFAULT_M),
min_k_(min_k),
num_levels_(num_levels),
is_level_zero_sorted_(is_level_zero_sorted),
n_(n),
levels_(std::move(levels)),
items_(items.release()),
items_size_(items_size),
min_item_(min_item.release()),
max_item_(max_item.release()),
sorted_view_(nullptr)
{}

// The following code is only valid in the special case of exactly reaching capacity while updating.
// It cannot be used while merging, while reducing k, or anything else.
template<typename T, typename C, typename A>
void kll_sketch<T, C, A>::compress_while_updating(void) {
  const uint8_t level = find_level_to_compact();

  // It is important to add the new top level right here. Be aware that this operation
  // grows the buffer and shifts the data and also the boundaries of the data and grows the
  // levels array and increments num_levels_
  if (level == (num_levels_ - 1)) {
    add_empty_top_level_to_completely_full_sketch();
  }

  const uint32_t raw_beg = levels_[level];
  const uint32_t raw_lim = levels_[level + 1];
  // +2 is OK because we already added a new top level if necessary
  const uint32_t pop_above = levels_[level + 2] - raw_lim;
  const uint32_t raw_pop = raw_lim - raw_beg;
  const bool odd_pop = kll_helper::is_odd(raw_pop);
  const uint32_t adj_beg = odd_pop ? raw_beg + 1 : raw_beg;
  const uint32_t adj_pop = odd_pop ? raw_pop - 1 : raw_pop;
  const uint32_t half_adj_pop = adj_pop / 2;
  const uint32_t destroy_beg = levels_[0];

  // level zero might not be sorted, so we must sort it if we wish to compact it
  // sort_level_zero() is not used here because of the adjustment for odd number of items
  if ((level == 0) && !is_level_zero_sorted_) {
    std::sort(items_ + adj_beg, items_ + adj_beg + adj_pop, comparator_);
  }
  if (pop_above == 0) {
    kll_helper::randomly_halve_up(items_, adj_beg, adj_pop);
  } else {
    kll_helper::randomly_halve_down(items_, adj_beg, adj_pop);
    kll_helper::merge_sorted_arrays<T, C>(items_, adj_beg, half_adj_pop, raw_lim, pop_above, adj_beg + half_adj_pop);
  }
  levels_[level + 1] -= half_adj_pop; // adjust boundaries of the level above
  if (odd_pop) {
    levels_[level] = levels_[level + 1] - 1; // the current level now contains one item
    if (levels_[level] != raw_beg) items_[levels_[level]] = std::move(items_[raw_beg]); // namely this leftover guy
  } else {
    levels_[level] = levels_[level + 1]; // the current level is now empty
  }

  // verify that we freed up half_adj_pop array slots just below the current level
  if (levels_[level] != (raw_beg + half_adj_pop)) throw std::logic_error("compaction error");

  // finally, we need to shift up the data in the levels below
  // so that the freed-up space can be used by level zero
  if (level > 0) {
    const uint32_t amount = raw_beg - levels_[0];
    std::move_backward(items_ + levels_[0], items_ + levels_[0] + amount, items_ + levels_[0] + half_adj_pop + amount);
    for (uint8_t lvl = 0; lvl < level; lvl++) levels_[lvl] += half_adj_pop;
  }
  for (uint32_t i = 0; i < half_adj_pop; i++) items_[i + destroy_beg].~T();
}

template<typename T, typename C, typename A>
uint8_t kll_sketch<T, C, A>::find_level_to_compact() const {
  uint8_t level = 0;
  while (true) {
    if (level >= num_levels_) throw std::logic_error("capacity calculation error");
    const uint32_t pop = levels_[level + 1] - levels_[level];
    const uint32_t cap = kll_helper::level_capacity(k_, num_levels_, level, m_);
    if (pop >= cap) {
      return level;
    }
    level++;
  }
}

template<typename T, typename C, typename A>
void kll_sketch<T, C, A>::add_empty_top_level_to_completely_full_sketch() {
  const uint32_t cur_total_cap = levels_[num_levels_];

  // make sure that we are following a certain growth scheme
  if (levels_[0] != 0) throw std::logic_error("full sketch expected");
  if (items_size_ != cur_total_cap) throw std::logic_error("current capacity mismatch");

  // note that merging MIGHT over-grow levels_, in which case we might not have to grow it here
  const uint8_t new_levels_size = num_levels_ + 2;
  if (levels_.size() < new_levels_size) {
    levels_.resize(new_levels_size);
  }

  const uint32_t delta_cap = kll_helper::level_capacity(k_, num_levels_ + 1, 0, m_);
  const uint32_t new_total_cap = cur_total_cap + delta_cap;

  // move (and shift) the current data into the new buffer
  T* new_buf = allocator_.allocate(new_total_cap);
  kll_helper::move_construct<T>(items_, 0, cur_total_cap, new_buf, delta_cap, true);
  allocator_.deallocate(items_, items_size_);
  items_ = new_buf;
  items_size_ = new_total_cap;

  // this loop includes the old "extra" index at the top
  for (uint8_t i = 0; i <= num_levels_; i++) {
    levels_[i] += delta_cap;
  }

  if (levels_[num_levels_] != new_total_cap) throw std::logic_error("new capacity mismatch");

  num_levels_++;
  levels_[num_levels_] = new_total_cap; // initialize the new "extra" index at the top
}

template<typename T, typename C, typename A>
void kll_sketch<T, C, A>::sort_level_zero() {
  if (!is_level_zero_sorted_) {
    std::sort(items_ + levels_[0], items_ + levels_[1], comparator_);
    is_level_zero_sorted_ = true;
  }
}

template<typename T, typename C, typename A>
void kll_sketch<T, C, A>::check_sorting() const {
  // not checking level 0
  for (uint8_t level = 1; level < num_levels_; ++level) {
    const auto from = items_ + levels_[level];
    const auto to = items_ + levels_[level + 1];
    if (!std::is_sorted(from, to, comparator_)) {
      throw std::logic_error("levels must be sorted");
    }
  }
}

template<typename T, typename C, typename A>
quantiles_sorted_view<T, C, A> kll_sketch<T, C, A>::get_sorted_view() const {
  const_cast<kll_sketch*>(this)->sort_level_zero(); // allow this side effect
  quantiles_sorted_view<T, C, A> view(get_num_retained(), comparator_, allocator_);
  for (uint8_t level = 0; level < num_levels_; ++level) {
    const auto from = items_ + levels_[level];
    const auto to = items_ + levels_[level + 1]; // exclusive
    view.add(from, to, 1 << level);
  }
  view.convert_to_cummulative();
  return view;
}

template<typename T, typename C, typename A>
template<typename O>
void kll_sketch<T, C, A>::merge_higher_levels(O&& other, uint64_t final_n) {
  const uint32_t tmp_num_items = get_num_retained() + other.get_num_retained_above_level_zero();
  A alloc(allocator_);
  auto tmp_items_deleter = [tmp_num_items, &alloc](T* ptr) { alloc.deallocate(ptr, tmp_num_items); }; // no destructor needed
  const std::unique_ptr<T, decltype(tmp_items_deleter)> workbuf(allocator_.allocate(tmp_num_items), tmp_items_deleter);
  const uint8_t ub = kll_helper::ub_on_num_levels(final_n);
  const size_t work_levels_size = ub + 2; // ub+1 does not work
  vector_u32 worklevels(work_levels_size, 0, allocator_);
  vector_u32 outlevels(work_levels_size, 0, allocator_);

  const uint8_t provisional_num_levels = std::max(num_levels_, other.num_levels_);

  populate_work_arrays(std::forward<O>(other), workbuf.get(), worklevels.data(), provisional_num_levels);

  const kll_helper::compress_result result = kll_helper::general_compress<T, C>(k_, m_, provisional_num_levels, workbuf.get(),
      worklevels.data(), outlevels.data(), is_level_zero_sorted_);

  // ub can sometimes be much bigger
  if (result.final_num_levels > ub) throw std::logic_error("merge error");

  // now we need to transfer the results back into "this" sketch
  if (result.final_capacity != items_size_) {
    allocator_.deallocate(items_, items_size_);
    items_size_ = result.final_capacity;
    items_ = allocator_.allocate(items_size_);
  }
  const uint32_t free_space_at_bottom = result.final_capacity - result.final_num_items;
  kll_helper::move_construct<T>(workbuf.get(), outlevels[0], outlevels[0] + result.final_num_items, items_, free_space_at_bottom, true);

  const size_t new_levels_size = result.final_num_levels + 1;
  if (levels_.size() < new_levels_size) {
    levels_.resize(new_levels_size);
  }
  const uint32_t offset = free_space_at_bottom - outlevels[0];
  for (uint8_t lvl = 0; lvl < levels_.size(); lvl++) { // includes the "extra" index
    levels_[lvl] = outlevels[lvl] + offset;
  }
  num_levels_ = result.final_num_levels;
}

// this leaves items_ uninitialized (all objects moved out and destroyed)
template<typename T, typename C, typename A>
template<typename FwdSk>
void kll_sketch<T, C, A>::populate_work_arrays(FwdSk&& other, T* workbuf, uint32_t* worklevels, uint8_t provisional_num_levels) {
  worklevels[0] = 0;

  // the level zero data from "other" was already inserted into "this"
  kll_helper::move_construct<T>(items_, levels_[0], levels_[1], workbuf, 0, true);
  worklevels[1] = safe_level_size(0);

  for (uint8_t lvl = 1; lvl < provisional_num_levels; lvl++) {
    const uint32_t self_pop = safe_level_size(lvl);
    const uint32_t other_pop = other.safe_level_size(lvl);
    worklevels[lvl + 1] = worklevels[lvl] + self_pop + other_pop;

    if ((self_pop > 0) && (other_pop == 0)) {
      kll_helper::move_construct<T>(items_, levels_[lvl], levels_[lvl] + self_pop, workbuf, worklevels[lvl], true);
    } else if ((self_pop == 0) && (other_pop > 0)) {
      for (auto i = other.levels_[lvl], j = worklevels[lvl]; i < other.levels_[lvl] + other_pop; ++i, ++j) {
        new (&workbuf[j]) T(conditional_forward<FwdSk>(other.items_[i]));
      }
    } else if ((self_pop > 0) && (other_pop > 0)) {
      kll_helper::merge_sorted_arrays<T, C>(items_, levels_[lvl], self_pop, other.items_, other.levels_[lvl], other_pop, workbuf, worklevels[lvl]);
    }
  }
}

template<typename T, typename C, typename A>
void kll_sketch<T, C, A>::assert_correct_total_weight() const {
  const uint64_t total(kll_helper::sum_the_sample_weights(num_levels_, levels_.data()));
  if (total != n_) {
    throw std::logic_error("Total weight does not match N");
  }
}

template<typename T, typename C, typename A>
uint32_t kll_sketch<T, C, A>::safe_level_size(uint8_t level) const {
  if (level >= num_levels_) return 0;
  return levels_[level + 1] - levels_[level];
}

template<typename T, typename C, typename A>
uint32_t kll_sketch<T, C, A>::get_num_retained_above_level_zero() const {
  if (num_levels_ == 1) return 0;
  return levels_[num_levels_] - levels_[1];
}

template<typename T, typename C, typename A>
void kll_sketch<T, C, A>::check_m(uint8_t m) {
  if (m != DEFAULT_M) {
    throw std::invalid_argument("Possible corruption: M must be " + std::to_string(DEFAULT_M)
        + ": " + std::to_string(m));
  }
}

template<typename T, typename C, typename A>
void kll_sketch<T, C, A>::check_preamble_ints(uint8_t preamble_ints, uint8_t flags_byte) {
  const bool is_empty(flags_byte & (1 << flags::IS_EMPTY));
  const bool is_single_item(flags_byte & (1 << flags::IS_SINGLE_ITEM));
  if (is_empty || is_single_item) {
    if (preamble_ints != PREAMBLE_INTS_SHORT) {
      throw std::invalid_argument("Possible corruption: preamble ints must be "
          + std::to_string(PREAMBLE_INTS_SHORT) + " for an empty or single item sketch: " + std::to_string(preamble_ints));
    }
  } else {
    if (preamble_ints != PREAMBLE_INTS_FULL) {
      throw std::invalid_argument("Possible corruption: preamble ints must be "
          + std::to_string(PREAMBLE_INTS_FULL) + " for a sketch with more than one item: " + std::to_string(preamble_ints));
    }
  }
}

template<typename T, typename C, typename A>
void kll_sketch<T, C, A>::check_serial_version(uint8_t serial_version) {
  if (serial_version != SERIAL_VERSION_1 && serial_version != SERIAL_VERSION_2) {
    throw std::invalid_argument("Possible corruption: serial version mismatch: expected "
        + std::to_string(SERIAL_VERSION_1) + " or " + std::to_string(SERIAL_VERSION_2)
        + ", got " + std::to_string(serial_version));
  }
}

template<typename T, typename C, typename A>
void kll_sketch<T, C, A>::check_family_id(uint8_t family_id) {
  if (family_id != FAMILY) {
    throw std::invalid_argument("Possible corruption: family mismatch: expected "
        + std::to_string(FAMILY) + ", got " + std::to_string(family_id));
  }
}

template <typename T, typename C, typename A>
string<A> kll_sketch<T, C, A>::to_string(bool print_levels, bool print_items) const {
  // Using a temporary stream for implementation here does not comply with AllocatorAwareContainer requirements.
  // The stream does not support passing an allocator instance, and alternatives are complicated.
  std::ostringstream os;
  os << "### KLL sketch summary:" << std::endl;
  os << "   K              : " << k_ << std::endl;
  os << "   min K          : " << min_k_ << std::endl;
  os << "   M              : " << (unsigned int) m_ << std::endl;
  os << "   N              : " << n_ << std::endl;
  os << "   Epsilon        : " << std::setprecision(3) << get_normalized_rank_error(false) * 100 << "%" << std::endl;
  os << "   Epsilon PMF    : " << get_normalized_rank_error(true) * 100 << "%" << std::endl;
  os << "   Empty          : " << (is_empty() ? "true" : "false") << std::endl;
  os << "   Estimation mode: " << (is_estimation_mode() ? "true" : "false") << std::endl;
  os << "   Levels         : " << (unsigned int) num_levels_ << std::endl;
  os << "   Sorted         : " << (is_level_zero_sorted_ ? "true" : "false") << std::endl;
  os << "   Capacity items : " << items_size_ << std::endl;
  os << "   Retained items : " << get_num_retained() << std::endl;
  if (!is_empty()) {
    os << "   Min item      : " << *min_item_ << std::endl;
    os << "   Max item      : " << *max_item_ << std::endl;
  }
  os << "### End sketch summary" << std::endl;

  if (print_levels) {
    os << "### KLL sketch levels:" << std::endl;
    os << "   index: nominal capacity, actual size" << std::endl;
    for (uint8_t i = 0; i < num_levels_; i++) {
      os << "   " << (unsigned int) i << ": " << kll_helper::level_capacity(k_, num_levels_, i, m_) << ", " << safe_level_size(i) << std::endl;
    }
    os << "### End sketch levels" << std::endl;
  }

  if (print_items) {
    os << "### KLL sketch data:" << std::endl;
    uint8_t level = 0;
    while (level < num_levels_) {
      const uint32_t from_index = levels_[level];
      const uint32_t to_index = levels_[level + 1]; // exclusive
      if (from_index < to_index) {
        os << " level " << (unsigned int) level << ":" << std::endl;
      }
      for (uint32_t i = from_index; i < to_index; i++) {
        os << "   " << items_[i] << std::endl;
      }
      level++;
    }
    os << "### End sketch data" << std::endl;
  }
  return string<A>(os.str().c_str(), allocator_);
}

template <typename T, typename C, typename A>
typename kll_sketch<T, C, A>::const_iterator kll_sketch<T, C, A>::begin() const {
  return kll_sketch<T, C, A>::const_iterator(items_, levels_.data(), num_levels_);
}

template <typename T, typename C, typename A>
typename kll_sketch<T, C, A>::const_iterator kll_sketch<T, C, A>::end() const {
  return kll_sketch<T, C, A>::const_iterator(nullptr, levels_.data(), num_levels_);
}

template<typename T, typename C, typename A>
class kll_sketch<T, C, A>::item_deleter {
  public:
  item_deleter(const A& allocator): allocator_(allocator) {}
  void operator() (T* ptr) {
    if (ptr != nullptr) {
      ptr->~T();
      allocator_.deallocate(ptr, 1);
    }
  }
  private:
  A allocator_;
};

template<typename T, typename C, typename A>
class kll_sketch<T, C, A>::items_deleter {
  public:
  items_deleter(uint32_t start, uint32_t num, const A& allocator):
    allocator_(allocator), start_(start), num_(num) {}
  void operator() (T* ptr) {
    if (ptr != nullptr) {
      for (uint32_t i = start_; i < num_; ++i) ptr[i].~T();
      allocator_.deallocate(ptr, num_);
    }
  }
  private:
  A allocator_;
  uint32_t start_;
  uint32_t num_;
};

template<typename T, typename C, typename A>
void kll_sketch<T, C, A>::setup_sorted_view() const {
  if (sorted_view_ == nullptr) {
    using AllocSortedView = typename std::allocator_traits<A>::template rebind_alloc<quantiles_sorted_view<T, C, A>>;
    sorted_view_ = new (AllocSortedView(allocator_).allocate(1)) quantiles_sorted_view<T, C, A>(get_sorted_view());
  }
}

template<typename T, typename C, typename A>
void kll_sketch<T, C, A>::reset_sorted_view() {
  if (sorted_view_ != nullptr) {
    sorted_view_->~quantiles_sorted_view();
    using AllocSortedView = typename std::allocator_traits<A>::template rebind_alloc<quantiles_sorted_view<T, C, A>>;
    AllocSortedView(allocator_).deallocate(sorted_view_, 1);
    sorted_view_ = nullptr;
  }
}

// kll_sketch::const_iterator implementation

template<typename T, typename C, typename A>
kll_sketch<T, C, A>::const_iterator::const_iterator(const T* items, const uint32_t* levels, const uint8_t num_levels):
items(items), levels(levels), num_levels(num_levels), index(items == nullptr ? levels[num_levels] : levels[0]), level(items == nullptr ? num_levels : 0), weight(1)
{}

template<typename T, typename C, typename A>
typename kll_sketch<T, C, A>::const_iterator& kll_sketch<T, C, A>::const_iterator::operator++() {
  ++index;
  if (index == levels[level + 1]) { // go to the next non-empty level
    do {
      ++level;
      weight *= 2;
    } while (level < num_levels && levels[level] == levels[level + 1]);
  }
  return *this;
}

template<typename T, typename C, typename A>
typename kll_sketch<T, C, A>::const_iterator& kll_sketch<T, C, A>::const_iterator::operator++(int) {
  const_iterator tmp(*this);
  operator++();
  return tmp;
}

template<typename T, typename C, typename A>
bool kll_sketch<T, C, A>::const_iterator::operator==(const const_iterator& other) const {
  return index == other.index;
}

template<typename T, typename C, typename A>
bool kll_sketch<T, C, A>::const_iterator::operator!=(const const_iterator& other) const {
  return !operator==(other);
}

template<typename T, typename C, typename A>
auto kll_sketch<T, C, A>::const_iterator::operator*() const -> const value_type {
  return value_type(items[index], weight);
}

template<typename T, typename C, typename A>
auto kll_sketch<T, C, A>::const_iterator::operator->() const -> const return_value_holder<value_type> {
  return **this;
}

} /* namespace datasketches */

#endif

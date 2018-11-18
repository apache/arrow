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

#include "arrow/compute/kernels/hash.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <exception>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/compute/context.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/util-internal.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/hash-util.h"
#include "arrow/util/hash.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

namespace arrow {

class MemoryPool;

using internal::checked_cast;

namespace compute {

// TODO(wesm): Enable top-level dispatch to SSE4 hashing if it is enabled
#define HASH_USE_SSE false

namespace {

enum class SIMDMode : char { NOSIMD, SSE4, AVX2 };

#define CHECK_IMPLEMENTED(KERNEL, FUNCNAME, TYPE)                  \
  if (!KERNEL) {                                                   \
    std::stringstream ss;                                          \
    ss << FUNCNAME << " not implemented for " << type->ToString(); \
    return Status::NotImplemented(ss.str());                       \
  }

// This is a slight design concession -- some hash actions have the possibility
// of failure. Rather than introduce extra error checking into all actions, we
// will raise an internal exception so that only the actions where errors can
// occur will experience the extra overhead
class HashException : public std::exception {
 public:
  explicit HashException(const std::string& msg, StatusCode code = StatusCode::Invalid)
      : msg_(msg), code_(code) {}

  ~HashException() throw() override {}

  const char* what() const throw() override;

  StatusCode code() const { return code_; }

 private:
  std::string msg_;
  StatusCode code_;
};

const char* HashException::what() const throw() { return msg_.c_str(); }

class HashTable {
 public:
  HashTable(const std::shared_ptr<DataType>& type, MemoryPool* pool)
      : type_(type),
        pool_(pool),
        initialized_(false),
        hash_table_(nullptr),
        hash_slots_(nullptr),
        hash_table_size_(0),
        mod_bitmask_(0) {}

  virtual ~HashTable() {}

  virtual Status Append(const ArrayData& input) = 0;
  virtual Status Flush(Datum* out) = 0;
  virtual Status GetDictionary(std::shared_ptr<ArrayData>* out) = 0;

 protected:
  Status Init(int64_t elements);

  std::shared_ptr<DataType> type_;
  MemoryPool* pool_;
  bool initialized_;

  // The hash table contains integer indices that reference the set of observed
  // distinct values
  std::shared_ptr<Buffer> hash_table_;
  hash_slot_t* hash_slots_;

  /// Size of the table. Must be a power of 2.
  int64_t hash_table_size_;

  /// Size at which we decide to resize
  int64_t hash_table_load_threshold_;

  // Store hash_table_size_ - 1, so that j & mod_bitmask_ is equivalent to j %
  // hash_table_size_, but uses far fewer CPU cycles
  int64_t mod_bitmask_;
};

Status HashTable::Init(int64_t elements) {
  DCHECK_EQ(elements, BitUtil::NextPower2(elements));
  RETURN_NOT_OK(internal::NewHashTable(elements, pool_, &hash_table_));
  hash_slots_ = reinterpret_cast<hash_slot_t*>(hash_table_->mutable_data());
  hash_table_size_ = elements;
  hash_table_load_threshold_ =
      static_cast<int64_t>(static_cast<double>(elements) * kMaxHashTableLoad);
  mod_bitmask_ = elements - 1;
  initialized_ = true;
  return Status::OK();
}

template <typename Type, typename Action, typename Enable = void>
class HashTableKernel : public HashTable {};

// Types of hash actions
//
// unique: append to dictionary when not found, no-op with slot
// dictionary-encode: append to dictionary when not found, append slot #
// match: raise or set null when not found, otherwise append slot #
// isin: set false when not found, otherwise true
// value counts: append to dictionary when not found, increment count for slot

template <typename Type, typename Enable = void>
class HashDictionary {};

// ----------------------------------------------------------------------
// Hash table pass for nulls

template <typename Type, typename Action>
class HashTableKernel<Type, Action, enable_if_null<Type>> : public HashTable {
 public:
  using HashTable::HashTable;

  Status Init() {
    // No-op, do not even need to initialize hash table
    return Status::OK();
  }

  Status Append(const ArrayData& arr) override {
    if (!initialized_) {
      RETURN_NOT_OK(Init());
    }
    auto action = checked_cast<Action*>(this);
    RETURN_NOT_OK(action->Reserve(arr.length));
    for (int64_t i = 0; i < arr.length; ++i) {
      action->ObserveNull();
    }
    return Status::OK();
  }

  Status GetDictionary(std::shared_ptr<ArrayData>* out) override {
    // TODO(wesm): handle null being a valid dictionary value
    auto null_array = std::make_shared<NullArray>(0);
    *out = null_array->data();
    return Status::OK();
  }
};

// ----------------------------------------------------------------------
// Hash table pass for primitive types

template <typename Type>
struct HashDictionary<Type, enable_if_has_c_type<Type>> {
  using T = typename Type::c_type;

  explicit HashDictionary(MemoryPool* pool) : pool(pool), size(0), capacity(0) {}

  Status Init() {
    this->size = 0;
    RETURN_NOT_OK(AllocateResizableBuffer(this->pool, 0, &this->buffer));
    return Resize(kInitialHashTableSize);
  }

  Status DoubleSize() { return Resize(this->size * 2); }

  Status Resize(const int64_t elements) {
    RETURN_NOT_OK(this->buffer->Resize(elements * sizeof(T)));

    this->capacity = elements;
    this->values = reinterpret_cast<T*>(this->buffer->mutable_data());
    return Status::OK();
  }

  MemoryPool* pool;
  std::shared_ptr<ResizableBuffer> buffer;
  T* values;
  int64_t size;
  int64_t capacity;
};

#define GENERIC_HASH_PASS(HASH_INNER_LOOP)                                               \
  if (arr.null_count != 0) {                                                             \
    internal::BitmapReader valid_reader(arr.buffers[0]->data(), arr.offset, arr.length); \
    for (int64_t i = 0; i < arr.length; ++i) {                                           \
      const bool is_null = valid_reader.IsNotSet();                                      \
      valid_reader.Next();                                                               \
                                                                                         \
      if (is_null) {                                                                     \
        action->ObserveNull();                                                           \
        continue;                                                                        \
      }                                                                                  \
                                                                                         \
      HASH_INNER_LOOP();                                                                 \
    }                                                                                    \
  } else {                                                                               \
    for (int64_t i = 0; i < arr.length; ++i) {                                           \
      HASH_INNER_LOOP();                                                                 \
    }                                                                                    \
  }

template <typename Type, typename Action>
class HashTableKernel<
    Type, Action,
    typename std::enable_if<has_c_type<Type>::value && !is_8bit_int<Type>::value>::type>
    : public HashTable {
 public:
  using T = typename Type::c_type;

  HashTableKernel(const std::shared_ptr<DataType>& type, MemoryPool* pool)
      : HashTable(type, pool), dict_(pool) {}

  Status Init() {
    RETURN_NOT_OK(dict_.Init());
    return HashTable::Init(kInitialHashTableSize);
  }

  Status Append(const ArrayData& arr) override {
    if (!initialized_) {
      RETURN_NOT_OK(Init());
    }

    const T* values = GetValues<T>(arr, 1);
    auto action = checked_cast<Action*>(this);

    RETURN_NOT_OK(action->Reserve(arr.length));

#define HASH_INNER_LOOP()                                               \
  const T value = values[i];                                            \
  int64_t j = HashValue(value) & mod_bitmask_;                          \
  hash_slot_t slot = hash_slots_[j];                                    \
                                                                        \
  while (kHashSlotEmpty != slot && dict_.values[slot] != value) {       \
    ++j;                                                                \
    if (ARROW_PREDICT_FALSE(j == hash_table_size_)) {                   \
      j = 0;                                                            \
    }                                                                   \
    slot = hash_slots_[j];                                              \
  }                                                                     \
                                                                        \
  if (slot == kHashSlotEmpty) {                                         \
    if (!Action::allow_expand) {                                        \
      throw HashException("Encountered new dictionary value");          \
    }                                                                   \
                                                                        \
    slot = static_cast<hash_slot_t>(dict_.size);                        \
    hash_slots_[j] = slot;                                              \
    dict_.values[dict_.size++] = value;                                 \
                                                                        \
    action->ObserveNotFound(slot);                                      \
                                                                        \
    if (ARROW_PREDICT_FALSE(dict_.size > hash_table_load_threshold_)) { \
      RETURN_NOT_OK(action->DoubleSize());                              \
    }                                                                   \
  } else {                                                              \
    action->ObserveFound(slot);                                         \
  }

    GENERIC_HASH_PASS(HASH_INNER_LOOP);

#undef HASH_INNER_LOOP

    return Status::OK();
  }

  Status GetDictionary(std::shared_ptr<ArrayData>* out) override {
    // TODO(wesm): handle null being in the dictionary
    auto dict_data = dict_.buffer;
    RETURN_NOT_OK(dict_data->Resize(dict_.size * sizeof(T), false));
    dict_data->ZeroPadding();

    *out = ArrayData::Make(type_, dict_.size, {nullptr, dict_data}, 0);
    return Status::OK();
  }

 protected:
  int64_t HashValue(const T& value) const {
    // TODO(wesm): Use faster hash function for C types
    return HashUtil::Hash<HASH_USE_SSE>(&value, sizeof(T), 0);
  }

  Status DoubleTableSize() {
#define PRIMITIVE_INNER_LOOP           \
  const T value = dict_.values[index]; \
  int64_t j = HashValue(value) & new_mod_bitmask;

    DOUBLE_TABLE_SIZE(, PRIMITIVE_INNER_LOOP);

#undef PRIMITIVE_INNER_LOOP

    return dict_.Resize(hash_table_size_);
  }

  HashDictionary<Type> dict_;
};

// ----------------------------------------------------------------------
// Hash table for boolean types

template <typename Type, typename Action>
class HashTableKernel<Type, Action, enable_if_boolean<Type>> : public HashTable {
 public:
  HashTableKernel(const std::shared_ptr<DataType>& type, MemoryPool* pool)
      : HashTable(type, pool) {
    std::fill(table_, table_ + 2, kHashSlotEmpty);
  }

  Status Append(const ArrayData& arr) override {
    auto action = checked_cast<Action*>(this);

    RETURN_NOT_OK(action->Reserve(arr.length));

    internal::BitmapReader value_reader(arr.buffers[1]->data(), arr.offset, arr.length);

#define HASH_INNER_LOOP()                                      \
  if (slot == kHashSlotEmpty) {                                \
    if (!Action::allow_expand) {                               \
      throw HashException("Encountered new dictionary value"); \
    }                                                          \
    table_[j] = slot = static_cast<hash_slot_t>(dict_.size()); \
    dict_.push_back(value);                                    \
    action->ObserveNotFound(slot);                             \
  } else {                                                     \
    action->ObserveFound(slot);                                \
  }

    if (arr.null_count != 0) {
      internal::BitmapReader valid_reader(arr.buffers[0]->data(), arr.offset, arr.length);
      for (int64_t i = 0; i < arr.length; ++i) {
        const bool is_null = valid_reader.IsNotSet();
        valid_reader.Next();
        if (is_null) {
          value_reader.Next();
          action->ObserveNull();
          continue;
        }
        const bool value = value_reader.IsSet();
        value_reader.Next();
        const int j = value ? 1 : 0;
        hash_slot_t slot = table_[j];
        HASH_INNER_LOOP();
      }
    } else {
      for (int64_t i = 0; i < arr.length; ++i) {
        const bool value = value_reader.IsSet();
        value_reader.Next();
        const int j = value ? 1 : 0;
        hash_slot_t slot = table_[j];
        HASH_INNER_LOOP();
      }
    }

#undef HASH_INNER_LOOP

    return Status::OK();
  }

  Status GetDictionary(std::shared_ptr<ArrayData>* out) override {
    BooleanBuilder builder(pool_);
    for (const bool value : dict_) {
      RETURN_NOT_OK(builder.Append(value));
    }
    return builder.FinishInternal(out);
  }

 private:
  hash_slot_t table_[2];
  std::vector<bool> dict_;
};

// ----------------------------------------------------------------------
// Hash table pass for variable-length binary types

template <typename Type, typename Action>
class HashTableKernel<Type, Action, enable_if_binary<Type>> : public HashTable {
 public:
  HashTableKernel(const std::shared_ptr<DataType>& type, MemoryPool* pool)
      : HashTable(type, pool), dict_offsets_(pool), dict_data_(pool), dict_size_(0) {}

  Status Init() {
    RETURN_NOT_OK(dict_offsets_.Resize(kInitialHashTableSize));

    // We append the end offset after each append to the dictionary, so this
    // sets the initial condition for the length-0 case
    //
    // initial offsets (dict size == 0): 0
    // after 1st dict entry of length 3: 0 3
    // after 2nd dict entry of length 4: 0 3 7
    RETURN_NOT_OK(dict_offsets_.Append(0));
    return HashTable::Init(kInitialHashTableSize);
  }

  Status Append(const ArrayData& arr) override {
    constexpr uint8_t empty_value = 0;
    if (!initialized_) {
      RETURN_NOT_OK(Init());
    }

    const int32_t* offsets = GetValues<int32_t>(arr, 1);
    const uint8_t* data;
    if (arr.buffers[2].get() == nullptr) {
      data = &empty_value;
    } else {
      data = GetValues<uint8_t>(arr, 2);
    }

    auto action = checked_cast<Action*>(this);
    RETURN_NOT_OK(action->Reserve(arr.length));

#define HASH_INNER_LOOP()                                                           \
  const int32_t position = offsets[i];                                              \
  const int32_t length = offsets[i + 1] - position;                                 \
  const uint8_t* value = data + position;                                           \
                                                                                    \
  int64_t j = HashValue(value, length) & mod_bitmask_;                              \
  hash_slot_t slot = hash_slots_[j];                                                \
                                                                                    \
  const int32_t* dict_offsets = dict_offsets_.data();                               \
  const uint8_t* dict_data = dict_data_.data();                                     \
  while (kHashSlotEmpty != slot &&                                                  \
         !((dict_offsets[slot + 1] - dict_offsets[slot]) == length &&               \
           0 == memcmp(value, dict_data + dict_offsets[slot], length))) {           \
    ++j;                                                                            \
    if (ARROW_PREDICT_FALSE(j == hash_table_size_)) {                               \
      j = 0;                                                                        \
    }                                                                               \
    slot = hash_slots_[j];                                                          \
  }                                                                                 \
                                                                                    \
  if (slot == kHashSlotEmpty) {                                                     \
    if (!Action::allow_expand) {                                                    \
      throw HashException("Encountered new dictionary value");                      \
    }                                                                               \
                                                                                    \
    slot = dict_size_++;                                                            \
    hash_slots_[j] = slot;                                                          \
                                                                                    \
    RETURN_NOT_OK(dict_data_.Append(value, length));                                \
    RETURN_NOT_OK(dict_offsets_.Append(static_cast<int32_t>(dict_data_.length()))); \
                                                                                    \
    action->ObserveNotFound(slot);                                                  \
                                                                                    \
    if (ARROW_PREDICT_FALSE(dict_size_ > hash_table_load_threshold_)) {             \
      RETURN_NOT_OK(action->DoubleSize());                                          \
    }                                                                               \
  } else {                                                                          \
    action->ObserveFound(slot);                                                     \
  }

    GENERIC_HASH_PASS(HASH_INNER_LOOP);

#undef HASH_INNER_LOOP

    return Status::OK();
  }

  Status GetDictionary(std::shared_ptr<ArrayData>* out) override {
    // TODO(wesm): handle null being in the dictionary
    BufferVector buffers = {nullptr, nullptr, nullptr};

    RETURN_NOT_OK(dict_offsets_.Finish(&buffers[1]));
    RETURN_NOT_OK(dict_data_.Finish(&buffers[2]));

    *out = ArrayData::Make(type_, dict_size_, std::move(buffers), 0);
    return Status::OK();
  }

 protected:
  int64_t HashValue(const uint8_t* data, int32_t length) const {
    return HashUtil::Hash<HASH_USE_SSE>(data, length, 0);
  }

  Status DoubleTableSize() {
#define VARBYTES_SETUP                                \
  const int32_t* dict_offsets = dict_offsets_.data(); \
  const uint8_t* dict_data = dict_data_.data()

#define VARBYTES_COMPUTE_HASH                                           \
  const int32_t length = dict_offsets[index + 1] - dict_offsets[index]; \
  const uint8_t* value = dict_data + dict_offsets[index];               \
  int64_t j = HashValue(value, length) & new_mod_bitmask

    DOUBLE_TABLE_SIZE(VARBYTES_SETUP, VARBYTES_COMPUTE_HASH);

#undef VARBYTES_SETUP
#undef VARBYTES_COMPUTE_HASH

    return Status::OK();
  }

  TypedBufferBuilder<int32_t> dict_offsets_;
  TypedBufferBuilder<uint8_t> dict_data_;
  int32_t dict_size_;
};

// ----------------------------------------------------------------------
// Hash table pass for fixed size binary types

template <typename Type, typename Action>
class HashTableKernel<Type, Action, enable_if_fixed_size_binary<Type>>
    : public HashTable {
 public:
  HashTableKernel(const std::shared_ptr<DataType>& type, MemoryPool* pool)
      : HashTable(type, pool), dict_data_(pool), dict_size_(0) {
    const auto& fw_type = checked_cast<const FixedSizeBinaryType&>(*type);
    byte_width_ = fw_type.bit_width() / 8;
  }

  Status Init() {
    RETURN_NOT_OK(dict_data_.Resize(kInitialHashTableSize * byte_width_));
    return HashTable::Init(kInitialHashTableSize);
  }

  Status Append(const ArrayData& arr) override {
    if (!initialized_) {
      RETURN_NOT_OK(Init());
    }

    const uint8_t* data = GetValues<uint8_t>(arr, 1);

    auto action = checked_cast<Action*>(this);
    RETURN_NOT_OK(action->Reserve(arr.length));

#define HASH_INNER_LOOP()                                                      \
  const uint8_t* value = data + i * byte_width_;                               \
  int64_t j = HashValue(value) & mod_bitmask_;                                 \
  hash_slot_t slot = hash_slots_[j];                                           \
                                                                               \
  const uint8_t* dict_data = dict_data_.data();                                \
  while (kHashSlotEmpty != slot &&                                             \
         !(0 == memcmp(value, dict_data + slot * byte_width_, byte_width_))) { \
    ++j;                                                                       \
    if (ARROW_PREDICT_FALSE(j == hash_table_size_)) {                          \
      j = 0;                                                                   \
    }                                                                          \
    slot = hash_slots_[j];                                                     \
  }                                                                            \
                                                                               \
  if (slot == kHashSlotEmpty) {                                                \
    if (!Action::allow_expand) {                                               \
      throw HashException("Encountered new dictionary value");                 \
    }                                                                          \
                                                                               \
    slot = dict_size_++;                                                       \
    hash_slots_[j] = slot;                                                     \
                                                                               \
    RETURN_NOT_OK(dict_data_.Append(value, byte_width_));                      \
                                                                               \
    action->ObserveNotFound(slot);                                             \
                                                                               \
    if (ARROW_PREDICT_FALSE(dict_size_ > hash_table_load_threshold_)) {        \
      RETURN_NOT_OK(action->DoubleSize());                                     \
    }                                                                          \
  } else {                                                                     \
    action->ObserveFound(slot);                                                \
  }

    GENERIC_HASH_PASS(HASH_INNER_LOOP);

#undef HASH_INNER_LOOP

    return Status::OK();
  }

  Status GetDictionary(std::shared_ptr<ArrayData>* out) override {
    // TODO(wesm): handle null being in the dictionary
    BufferVector buffers = {nullptr, nullptr};
    RETURN_NOT_OK(dict_data_.Finish(&buffers[1]));

    *out = ArrayData::Make(type_, dict_size_, std::move(buffers), 0);
    return Status::OK();
  }

 protected:
  int64_t HashValue(const uint8_t* data) const {
    return HashUtil::Hash<HASH_USE_SSE>(data, byte_width_, 0);
  }

  Status DoubleTableSize() {
#define FIXED_BYTES_SETUP const uint8_t* dict_data = dict_data_.data()

#define FIXED_BYTES_COMPUTE_HASH \
  int64_t j = HashValue(dict_data + index * byte_width_) & new_mod_bitmask

    DOUBLE_TABLE_SIZE(FIXED_BYTES_SETUP, FIXED_BYTES_COMPUTE_HASH);

#undef FIXED_BYTES_SETUP
#undef FIXED_BYTES_COMPUTE_HASH

    return Status::OK();
  }

  int32_t byte_width_;
  TypedBufferBuilder<uint8_t> dict_data_;
  int32_t dict_size_;
};

// ----------------------------------------------------------------------
// Hash table pass for uint8 and int8

template <typename T>
inline int Hash8Bit(const T val) {
  return 0;
}

template <>
inline int Hash8Bit(const uint8_t val) {
  return val;
}

template <>
inline int Hash8Bit(const int8_t val) {
  return val + 128;
}

template <typename Type, typename Action>
class HashTableKernel<Type, Action, enable_if_8bit_int<Type>> : public HashTable {
 public:
  using T = typename Type::c_type;

  HashTableKernel(const std::shared_ptr<DataType>& type, MemoryPool* pool)
      : HashTable(type, pool) {
    std::fill(table_, table_ + 256, kHashSlotEmpty);
  }

  Status Append(const ArrayData& arr) override {
    const T* values = GetValues<T>(arr, 1);
    auto action = checked_cast<Action*>(this);
    RETURN_NOT_OK(action->Reserve(arr.length));

#define HASH_INNER_LOOP()                                      \
  const T value = values[i];                                   \
  const int hash = Hash8Bit<T>(value);                         \
  hash_slot_t slot = table_[hash];                             \
                                                               \
  if (slot == kHashSlotEmpty) {                                \
    if (!Action::allow_expand) {                               \
      throw HashException("Encountered new dictionary value"); \
    }                                                          \
                                                               \
    slot = static_cast<hash_slot_t>(dict_.size());             \
    table_[hash] = slot;                                       \
    dict_.push_back(value);                                    \
    action->ObserveNotFound(slot);                             \
  } else {                                                     \
    action->ObserveFound(slot);                                \
  }

    GENERIC_HASH_PASS(HASH_INNER_LOOP);

#undef HASH_INNER_LOOP

    return Status::OK();
  }

  Status GetDictionary(std::shared_ptr<ArrayData>* out) override {
    using BuilderType = typename TypeTraits<Type>::BuilderType;
    BuilderType builder(pool_);

    for (const T value : dict_) {
      RETURN_NOT_OK(builder.Append(value));
    }

    return builder.FinishInternal(out);
  }

 private:
  hash_slot_t table_[256];
  std::vector<T> dict_;
};

// ----------------------------------------------------------------------
// Unique implementation

template <typename Type>
class UniqueImpl : public HashTableKernel<Type, UniqueImpl<Type>> {
 public:
  static constexpr bool allow_expand = true;
  using Base = HashTableKernel<Type, UniqueImpl<Type>>;
  using Base::Base;

  Status Reserve(const int64_t length) { return Status::OK(); }

  void ObserveFound(const hash_slot_t slot) {}
  void ObserveNull() {}
  void ObserveNotFound(const hash_slot_t slot) {}

  Status DoubleSize() { return Base::DoubleTableSize(); }

  Status Append(const ArrayData& input) override { return Base::Append(input); }

  Status Flush(Datum* out) override {
    // No-op
    return Status::OK();
  }
};

// ----------------------------------------------------------------------
// Dictionary encode implementation

template <typename Type>
class DictEncodeImpl : public HashTableKernel<Type, DictEncodeImpl<Type>> {
 public:
  static constexpr bool allow_expand = true;
  using Base = HashTableKernel<Type, DictEncodeImpl>;

  DictEncodeImpl(const std::shared_ptr<DataType>& type, MemoryPool* pool)
      : Base(type, pool), indices_builder_(pool) {}

  Status Reserve(const int64_t length) { return indices_builder_.Reserve(length); }

  void ObserveNull() { indices_builder_.UnsafeAppendNull(); }

  void ObserveFound(const hash_slot_t slot) { indices_builder_.UnsafeAppend(slot); }

  void ObserveNotFound(const hash_slot_t slot) { return ObserveFound(slot); }

  Status DoubleSize() { return Base::DoubleTableSize(); }

  Status Flush(Datum* out) override {
    std::shared_ptr<ArrayData> result;
    RETURN_NOT_OK(indices_builder_.FinishInternal(&result));
    out->value = std::move(result);
    return Status::OK();
  }

  using Base::Append;

 private:
  Int32Builder indices_builder_;
};

// ----------------------------------------------------------------------
// Kernel wrapper for generic hash table kernels

class HashKernelImpl : public HashKernel {
 public:
  explicit HashKernelImpl(std::unique_ptr<HashTable> hasher)
      : hasher_(std::move(hasher)) {}

  Status Call(FunctionContext* ctx, const Datum& input, Datum* out) override {
    DCHECK_EQ(Datum::ARRAY, input.kind());
    RETURN_NOT_OK(Append(ctx, *input.array()));
    return Flush(out);
  }

  Status Append(FunctionContext* ctx, const ArrayData& input) override {
    std::lock_guard<std::mutex> guard(lock_);
    try {
      RETURN_NOT_OK(hasher_->Append(input));
    } catch (const HashException& e) {
      return Status(e.code(), e.what());
    }
    return Status::OK();
  }

  Status Flush(Datum* out) override { return hasher_->Flush(out); }

  Status GetDictionary(std::shared_ptr<ArrayData>* out) override {
    return hasher_->GetDictionary(out);
  }

 private:
  std::mutex lock_;
  std::unique_ptr<HashTable> hasher_;
};

}  // namespace

Status GetUniqueKernel(FunctionContext* ctx, const std::shared_ptr<DataType>& type,
                       std::unique_ptr<HashKernel>* out) {
  std::unique_ptr<HashTable> hasher;

#define UNIQUE_CASE(InType)                                         \
  case InType::type_id:                                             \
    hasher.reset(new UniqueImpl<InType>(type, ctx->memory_pool())); \
    break

  switch (type->id()) {
    UNIQUE_CASE(NullType);
    UNIQUE_CASE(BooleanType);
    UNIQUE_CASE(UInt8Type);
    UNIQUE_CASE(Int8Type);
    UNIQUE_CASE(UInt16Type);
    UNIQUE_CASE(Int16Type);
    UNIQUE_CASE(UInt32Type);
    UNIQUE_CASE(Int32Type);
    UNIQUE_CASE(UInt64Type);
    UNIQUE_CASE(Int64Type);
    UNIQUE_CASE(FloatType);
    UNIQUE_CASE(DoubleType);
    UNIQUE_CASE(Date32Type);
    UNIQUE_CASE(Date64Type);
    UNIQUE_CASE(Time32Type);
    UNIQUE_CASE(Time64Type);
    UNIQUE_CASE(TimestampType);
    UNIQUE_CASE(BinaryType);
    UNIQUE_CASE(StringType);
    UNIQUE_CASE(FixedSizeBinaryType);
    UNIQUE_CASE(Decimal128Type);
    default:
      break;
  }

#undef UNIQUE_CASE

  CHECK_IMPLEMENTED(hasher, "unique", type);
  out->reset(new HashKernelImpl(std::move(hasher)));
  return Status::OK();
}

Status GetDictionaryEncodeKernel(FunctionContext* ctx,
                                 const std::shared_ptr<DataType>& type,
                                 std::unique_ptr<HashKernel>* out) {
  std::unique_ptr<HashTable> hasher;

#define DICTIONARY_ENCODE_CASE(InType)                                  \
  case InType::type_id:                                                 \
    hasher.reset(new DictEncodeImpl<InType>(type, ctx->memory_pool())); \
    break

  switch (type->id()) {
    DICTIONARY_ENCODE_CASE(NullType);
    DICTIONARY_ENCODE_CASE(BooleanType);
    DICTIONARY_ENCODE_CASE(UInt8Type);
    DICTIONARY_ENCODE_CASE(Int8Type);
    DICTIONARY_ENCODE_CASE(UInt16Type);
    DICTIONARY_ENCODE_CASE(Int16Type);
    DICTIONARY_ENCODE_CASE(UInt32Type);
    DICTIONARY_ENCODE_CASE(Int32Type);
    DICTIONARY_ENCODE_CASE(UInt64Type);
    DICTIONARY_ENCODE_CASE(Int64Type);
    DICTIONARY_ENCODE_CASE(FloatType);
    DICTIONARY_ENCODE_CASE(DoubleType);
    DICTIONARY_ENCODE_CASE(Date32Type);
    DICTIONARY_ENCODE_CASE(Date64Type);
    DICTIONARY_ENCODE_CASE(Time32Type);
    DICTIONARY_ENCODE_CASE(Time64Type);
    DICTIONARY_ENCODE_CASE(TimestampType);
    DICTIONARY_ENCODE_CASE(BinaryType);
    DICTIONARY_ENCODE_CASE(StringType);
    DICTIONARY_ENCODE_CASE(FixedSizeBinaryType);
    DICTIONARY_ENCODE_CASE(Decimal128Type);
    default:
      break;
  }

#undef DICTIONARY_ENCODE_CASE

  CHECK_IMPLEMENTED(hasher, "dictionary-encode", type);
  out->reset(new HashKernelImpl(std::move(hasher)));
  return Status::OK();
}

namespace {

Status InvokeHash(FunctionContext* ctx, HashKernel* func, const Datum& value,
                  std::vector<Datum>* kernel_outputs,
                  std::shared_ptr<Array>* dictionary) {
  RETURN_NOT_OK(detail::InvokeUnaryArrayKernel(ctx, func, value, kernel_outputs));

  std::shared_ptr<ArrayData> dict_data;
  RETURN_NOT_OK(func->GetDictionary(&dict_data));
  *dictionary = MakeArray(dict_data);
  return Status::OK();
}

}  // namespace

Status Unique(FunctionContext* ctx, const Datum& value, std::shared_ptr<Array>* out) {
  std::unique_ptr<HashKernel> func;
  RETURN_NOT_OK(GetUniqueKernel(ctx, value.type(), &func));

  std::vector<Datum> dummy_outputs;
  return InvokeHash(ctx, func.get(), value, &dummy_outputs, out);
}

Status DictionaryEncode(FunctionContext* ctx, const Datum& value, Datum* out) {
  std::unique_ptr<HashKernel> func;
  RETURN_NOT_OK(GetDictionaryEncodeKernel(ctx, value.type(), &func));

  std::shared_ptr<Array> dictionary;
  std::vector<Datum> indices_outputs;
  RETURN_NOT_OK(InvokeHash(ctx, func.get(), value, &indices_outputs, &dictionary));

  // Create the dictionary type
  DCHECK_EQ(indices_outputs[0].kind(), Datum::ARRAY);
  std::shared_ptr<DataType> dict_type =
      ::arrow::dictionary(indices_outputs[0].array()->type, dictionary);

  // Create DictionaryArray for each piece yielded by the kernel invocations
  std::vector<std::shared_ptr<Array>> dict_chunks;
  for (const Datum& datum : indices_outputs) {
    dict_chunks.emplace_back(
        std::make_shared<DictionaryArray>(dict_type, MakeArray(datum.array())));
  }

  *out = detail::WrapArraysLike(value, dict_chunks);
  return Status::OK();
}

}  // namespace compute
}  // namespace arrow

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

#include "arrow/array/diff.h"

#include <algorithm>
#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/array_decimal.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/array_run_end.h"
#include "arrow/buffer.h"
#include "arrow/buffer_builder.h"
#include "arrow/extension_type.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/range.h"
#include "arrow/util/ree_util.h"
#include "arrow/util/string.h"
#include "arrow/util/unreachable.h"
#include "arrow/vendored/datetime.h"
#include "arrow/visit_type_inline.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;
using internal::MakeLazyRange;

template <typename ArrayType>
auto GetView(const ArrayType& array, int64_t index) -> decltype(array.GetView(index)) {
  return array.GetView(index);
}

struct Slice {
  const Array* array_;
  int64_t offset_, length_;

  bool operator==(const Slice& other) const {
    return length_ == other.length_ &&
           array_->RangeEquals(offset_, offset_ + length_, other.offset_, *other.array_);
  }
  bool operator!=(const Slice& other) const { return !(*this == other); }
};

template <typename ArrayType, typename T = typename ArrayType::TypeClass,
          typename = enable_if_list_like<T>>
static Slice GetView(const ArrayType& array, int64_t index) {
  return Slice{array.values().get(), array.value_offset(index),
               array.value_length(index)};
}

struct UnitSlice {
  const Array* array_;
  int64_t offset_;

  bool operator==(const UnitSlice& other) const {
    return array_->RangeEquals(offset_, offset_ + 1, other.offset_, *other.array_);
  }
  bool operator!=(const UnitSlice& other) const { return !(*this == other); }
};

// FIXME(bkietz) this is inefficient;
// StructArray's fields can be diffed independently then merged
static UnitSlice GetView(const StructArray& array, int64_t index) {
  return UnitSlice{&array, index};
}

static UnitSlice GetView(const UnionArray& array, int64_t index) {
  return UnitSlice{&array, index};
}

/// \brief A simple virtual comparator interface for two arrays.
///
/// The base and target array ara bound at construction time. Then
/// Equals(base_index, target_index) should return true if the values
/// at the given indices are equal.
struct ValueComparator {
  virtual ~ValueComparator() = default;

  /// \brief Compare the validity and values at the given indices in the base and target
  /// arrays.
  ///
  /// \param base_index The index in the base array.
  /// \param target_index The index in the target array.
  /// \return true if the values at the given indices are equal, false otherwise.
  /// \pre base_index and target_index are valid indices in their respective arrays.
  virtual bool Equals(int64_t base_index, int64_t target_index) = 0;

  /// \brief Return the run length of equal values starting at the given indices in the
  /// base and target arrays.
  ///
  /// \param base_index The starting index in the base array.
  /// \param base_length The length of the base array.
  /// \param target_index The starting index in the target array.
  /// \param target_length The length of the target array.
  /// \return The run length of equal values starting at the given indices in the base
  /// and target arrays.
  virtual int64_t RunLengthOfEqualsFrom(int64_t base_index, int64_t base_length,
                                        int64_t target_index, int64_t target_length) {
    int64_t run_length_of_equals = 0;
    while (base_index < base_length && target_index < target_length) {
      if (!Equals(base_index, target_index)) {
        break;
      }
      base_index += 1;
      target_index += 1;
      run_length_of_equals += 1;
    }
    return run_length_of_equals;
  }
};

template <typename ArrayType>
struct DefaultValueComparator : public ValueComparator {
  const ArrayType& base;
  const ArrayType& target;

  DefaultValueComparator(const ArrayType& base, const ArrayType& target)
      : base(base), target(target) {}

  ~DefaultValueComparator() override = default;

  bool Equals(int64_t base_index, int64_t target_index) override {
    const bool base_valid = base.IsValid(base_index);
    const bool target_valid = target.IsValid(target_index);
    if (base_valid && target_valid) {
      return GetView(base, base_index) == GetView(target, target_index);
    }
    return base_valid == target_valid;
  }
};

template <typename RunEndCType>
class REEValueComparator : public ValueComparator {
 private:
  const RunEndEncodedArray& base_;
  const RunEndEncodedArray& target_;
  std::unique_ptr<ValueComparator> inner_value_comparator_;
  ree_util::PhysicalIndexFinder<RunEndCType> base_physical_index_finder_;
  ree_util::PhysicalIndexFinder<RunEndCType> target_physical_index_finder_;

 public:
  REEValueComparator(const RunEndEncodedArray& base, const RunEndEncodedArray& target,
                     std::unique_ptr<ValueComparator>&& inner_value_comparator)
      : base_(base),
        target_(target),
        inner_value_comparator_(std::move(inner_value_comparator)),
        base_physical_index_finder_(*base_.data()),
        target_physical_index_finder_(*target_.data()) {
    DCHECK_EQ(*base_.type(), *target_.type());
  }

  ~REEValueComparator() override = default;

 private:
  /// \pre 0 <= i < base_.length()
  inline int64_t FindPhysicalIndexOnBase(int64_t i) {
    return base_physical_index_finder_.FindPhysicalIndex(i);
  }

  /// \pre 0 <= i < target_.length()
  inline int64_t FindPhysicalIndexOnTarget(int64_t i) {
    return target_physical_index_finder_.FindPhysicalIndex(i);
  }

  const RunEndCType* base_run_ends() { return base_physical_index_finder_.run_ends; }

  const RunEndCType* target_run_ends() { return target_physical_index_finder_.run_ends; }

 public:
  int64_t RunLengthOfEqualsFrom(int64_t base_index, int64_t base_length,
                                int64_t target_index, int64_t target_length) override {
    // Ensure the first search for physical index on the values arrays is safe.
    if (base_index >= base_length || target_index >= target_length) {
      // Without values on either side, there is no run of equal values.
      return 0;
    }

    // Translate the two logical indices into physical indices.
    int64_t physical_base_index = FindPhysicalIndexOnBase(base_index);
    int64_t physical_target_index = FindPhysicalIndexOnTarget(target_index);

    int64_t run_length_of_equals = 0;
    // The loop invariant (base_index < base_length && target_index < target_length)
    // is valid when the loop starts because of the check above.
    for (;;) {
      const auto base_run_end =
          static_cast<int64_t>(base_run_ends()[physical_base_index]) - base_.offset();
      const auto target_run_end =
          static_cast<int64_t>(target_run_ends()[physical_target_index]) -
          target_.offset();
      // The end of the runs containing the logical indices, by definition, ends
      // after the logical indices.
      DCHECK_LT(base_index, base_run_end);
      DCHECK_LT(target_index, target_run_end);

      // Compare the physical values that make up the runs containing base_index
      // and target_index.
      if (!inner_value_comparator_->Equals(physical_base_index, physical_target_index)) {
        // First difference found, stop because the run of equal values cannot
        // be extended further.
        break;
      }

      const int64_t base_run = std::min(base_run_end, base_length) - base_index;
      const int64_t target_run = std::min(target_run_end, target_length) - target_index;
      // Due to the loop-invariant (base_index < base_length && target_index <
      // target_length) and properties of the run-ends asserted above, both base_run and
      // target_run are strictly greater than zero.
      DCHECK_GT(base_run, 0);
      DCHECK_GT(target_run, 0);

      // Skip the smallest run (or both runs if they are equal)
      const int64_t increment = std::min(base_run, target_run);
      physical_base_index += increment == base_run;
      physical_target_index += increment == target_run;

      // Since both base_run and target_run are greater than zero,
      // increment is also greater than zero...
      DCHECK_GT(increment, 0);
      // ...which implies that the loop will make progress and eventually terminate
      // because base_index or target_index will equal base_length or target_length,
      // respectively.
      base_index += increment;
      target_index += increment;
      // The value representing the two runs are equal, so we can assume that at
      // least `increment` (size of smallest run) values are equal.
      run_length_of_equals += increment;

      if (base_index >= base_length || target_index >= target_length) {
        break;
      }
    }

    return run_length_of_equals;
  }

  bool Equals(int64_t base_index, int64_t target_index) override {
    const int64_t physical_base_index = FindPhysicalIndexOnBase(base_index);
    const int64_t physical_target_index = FindPhysicalIndexOnTarget(target_index);
    return inner_value_comparator_->Equals(physical_base_index, physical_target_index);
  }
};

class ValueComparatorFactory {
 private:
  std::unique_ptr<ValueComparator> comparator_;

 public:
  template <typename T>
  Status Visit(const T&, const Array& base, const Array& target) {
    using ArrayType = typename TypeTraits<T>::ArrayType;
    comparator_ = std::make_unique<DefaultValueComparator<ArrayType>>(
        checked_cast<const ArrayType&>(base), checked_cast<const ArrayType&>(target));
    return Status::OK();
  }

  Status Visit(const NullType&, const Array&, const Array&) {
    return Status::NotImplemented("null type");
  }
  Status Visit(const ListViewType&, const Array&, const Array&) {
    return Status::NotImplemented("list-view type");
  }

  Status Visit(const LargeListViewType&, const Array&, const Array&) {
    return Status::NotImplemented("list-view type");
  }

  Status Visit(const ExtensionType&, const Array&, const Array&) {
    return Status::NotImplemented("extension type");
  }

  Status Visit(const DictionaryType&, const Array& base, const Array& target) {
    return Status::NotImplemented("dictionary type");
  }

  Status Visit(const RunEndEncodedType& ree_type, const Array& base,
               const Array& target) {
    const auto& base_ree = checked_cast<const RunEndEncodedArray&>(base);
    const auto& target_ree = checked_cast<const RunEndEncodedArray&>(target);

    ARROW_ASSIGN_OR_RAISE(
        auto inner_values_comparator,
        Create(*ree_type.value_type(), *base_ree.values(), *target_ree.values()));

    // Instantiate the specialized comparator types with operator new instead of
    // make_unique<T>() to avoid binary bloat. unique_ptr<T>'s constructor is templated
    // on the type of the deleter and we're fine with destructor calls being virtually
    // dispatched via ValueComparator.
    ValueComparator* ree_value_comparator = nullptr;
    switch (ree_type.run_end_type()->id()) {
      case Type::INT16:
        ree_value_comparator = new REEValueComparator<int16_t>(
            base_ree, target_ree, std::move(inner_values_comparator));
        break;
      case Type::INT32:
        ree_value_comparator = new REEValueComparator<int32_t>(
            base_ree, target_ree, std::move(inner_values_comparator));
        break;
      case Type::INT64:
        ree_value_comparator = new REEValueComparator<int64_t>(
            base_ree, target_ree, std::move(inner_values_comparator));
        break;
      default:
        Unreachable();
    }
    comparator_.reset(ree_value_comparator);
    return Status::OK();
  }

  static Result<std::unique_ptr<ValueComparator>> Create(const DataType& type,
                                                         const Array& base,
                                                         const Array& target) {
    ValueComparatorFactory self;
    RETURN_NOT_OK(VisitTypeInline(type, &self, base, target));
    return std::move(self.comparator_);
  }
};

// represents an intermediate state in the comparison of two arrays
struct EditPoint {
  int64_t base, target;
  bool operator==(EditPoint other) const {
    return base == other.base && target == other.target;
  }
};

/// A generic sequence difference algorithm, based on
///
/// E. W. Myers, "An O(ND) difference algorithm and its variations,"
/// Algorithmica, vol. 1, no. 1-4, pp. 251–266, 1986.
///
/// To summarize, an edit script is computed by maintaining the furthest set of EditPoints
/// which are reachable in a given number of edits D. This is used to compute the furthest
/// set reachable with D+1 edits, and the process continues inductively until a complete
/// edit script is discovered.
///
/// From each edit point a single deletion and insertion is made then as many shared
/// elements as possible are skipped, recording only the endpoint of the run. This
/// representation is minimal in the common case where the sequences differ only slightly,
/// since most of the elements are shared between base and target and are represented
/// implicitly.
class QuadraticSpaceMyersDiff {
 public:
  QuadraticSpaceMyersDiff(const Array& base, const Array& target, MemoryPool* pool)
      : base_(base), target_(target), pool_(pool) {}

 private:
  // increment the position within base (the element pointed to was deleted)
  // then extend maximally
  EditPoint DeleteOne(EditPoint p) const {
    if (p.base != base_end_) {
      ++p.base;
    }
    return ExtendFrom(p);
  }

  // increment the position within target (the element pointed to was inserted)
  // then extend maximally
  EditPoint InsertOne(EditPoint p) const {
    if (p.target != target_end_) {
      ++p.target;
    }
    return ExtendFrom(p);
  }

  // increment the position within base and target (the elements skipped in this way were
  // present in both sequences)
  EditPoint ExtendFrom(EditPoint p) const {
    const int64_t run_length_of_equals =
        _comparator->RunLengthOfEqualsFrom(p.base, base_end_, p.target, target_end_);
    p.base += run_length_of_equals;
    p.target += run_length_of_equals;
    return p;
  }

  // beginning of a range for storing per-edit state in endpoint_base_ and insert_
  int64_t StorageOffset(int64_t edit_count) const {
    return edit_count * (edit_count + 1) / 2;
  }

  // given edit_count and index, augment endpoint_base_[index] with the corresponding
  // position in target (which is only implicitly represented in edit_count, index)
  EditPoint GetEditPoint(int64_t edit_count, int64_t index) const {
    DCHECK_GE(index, StorageOffset(edit_count));
    DCHECK_LT(index, StorageOffset(edit_count + 1));
    auto insertions_minus_deletions =
        2 * (index - StorageOffset(edit_count)) - edit_count;
    auto maximal_base = endpoint_base_[index];
    auto maximal_target = std::min(
        target_begin_ + ((maximal_base - base_begin_) + insertions_minus_deletions),
        target_end_);
    return {maximal_base, maximal_target};
  }

  void Next() {
    ++edit_count_;
    // base_begin_ is used as a dummy value here since Iterator may not be default
    // constructible. The newly allocated range is completely overwritten below.
    endpoint_base_.resize(StorageOffset(edit_count_ + 1), base_begin_);
    insert_.resize(StorageOffset(edit_count_ + 1), false);

    auto previous_offset = StorageOffset(edit_count_ - 1);
    auto current_offset = StorageOffset(edit_count_);

    // try deleting from base first
    for (int64_t i = 0, i_out = 0; i < edit_count_; ++i, ++i_out) {
      auto previous_endpoint = GetEditPoint(edit_count_ - 1, i + previous_offset);
      endpoint_base_[i_out + current_offset] = DeleteOne(previous_endpoint).base;
    }

    // check if inserting from target could do better
    for (int64_t i = 0, i_out = 1; i < edit_count_; ++i, ++i_out) {
      // retrieve the previously computed best endpoint for (edit_count_, i_out)
      // for comparison with the best endpoint achievable with an insertion
      auto endpoint_after_deletion = GetEditPoint(edit_count_, i_out + current_offset);

      auto previous_endpoint = GetEditPoint(edit_count_ - 1, i + previous_offset);
      auto endpoint_after_insertion = InsertOne(previous_endpoint);

      if (endpoint_after_insertion.base - endpoint_after_deletion.base >= 0) {
        // insertion was more efficient; keep it and mark the insertion in insert_
        insert_[i_out + current_offset] = true;
        endpoint_base_[i_out + current_offset] = endpoint_after_insertion.base;
      }
    }

    // check for completion
    EditPoint finish = {base_end_, target_end_};
    for (int64_t i_out = 0; i_out < edit_count_ + 1; ++i_out) {
      if (GetEditPoint(edit_count_, i_out + current_offset) == finish) {
        finish_index_ = i_out + current_offset;
        return;
      }
    }
  }

  bool Done() { return finish_index_ != -1; }

  Result<std::shared_ptr<StructArray>> GetEdits(MemoryPool* pool) {
    DCHECK(Done());

    int64_t length = edit_count_ + 1;
    ARROW_ASSIGN_OR_RAISE(auto insert_buf, AllocateEmptyBitmap(length, pool));
    ARROW_ASSIGN_OR_RAISE(auto run_length_buf,
                          AllocateBuffer(length * sizeof(int64_t), pool));
    auto run_length = reinterpret_cast<int64_t*>(run_length_buf->mutable_data());

    auto index = finish_index_;
    auto endpoint = GetEditPoint(edit_count_, finish_index_);

    for (int64_t i = edit_count_; i > 0; --i) {
      bool insert = insert_[index];
      bit_util::SetBitTo(insert_buf->mutable_data(), i, insert);

      auto insertions_minus_deletions =
          (endpoint.base - base_begin_) - (endpoint.target - target_begin_);
      if (insert) {
        ++insertions_minus_deletions;
      } else {
        --insertions_minus_deletions;
      }
      index = (i - 1 - insertions_minus_deletions) / 2 + StorageOffset(i - 1);

      // endpoint of previous edit
      auto previous = GetEditPoint(i - 1, index);
      run_length[i] = endpoint.base - previous.base - !insert;
      DCHECK_GE(run_length[i], 0);

      endpoint = previous;
    }
    bit_util::SetBitTo(insert_buf->mutable_data(), 0, false);
    run_length[0] = endpoint.base - base_begin_;

    return StructArray::Make(
        {std::make_shared<BooleanArray>(length, std::move(insert_buf)),
         std::make_shared<Int64Array>(length, std::move(run_length_buf))},
        {field("insert", boolean()), field("run_length", int64())});
  }

 public:
  Result<std::shared_ptr<StructArray>> Diff() {
    base_begin_ = 0;
    base_end_ = base_.length();
    target_begin_ = 0;
    target_end_ = target_.length();
    ARROW_ASSIGN_OR_RAISE(_comparator,
                          ValueComparatorFactory::Create(*base_.type(), base_, target_));

    finish_index_ = -1;
    edit_count_ = 0;
    endpoint_base_ = {ExtendFrom({base_begin_, target_begin_}).base};
    insert_ = {true};
    if ((base_end_ - base_begin_ == target_end_ - target_begin_) &&
        endpoint_base_[0] == base_end_) {
      // trivial case: base == target
      finish_index_ = 0;
    }
    while (!Done()) {
      Next();
    }
    return GetEdits(pool_);
  }

 private:
  // Constructor-injected references
  const Array& base_;
  const Array& target_;
  MemoryPool* pool_;

  // Initialized on Diff() and immutable thereafter
  int64_t base_begin_ = 0, base_end_ = -1;
  int64_t target_begin_ = 0, target_end_ = -1;
  std::unique_ptr<ValueComparator> _comparator;

  // Initialized on Next() and mutated throughout the diffing process
  int64_t finish_index_ = -1;
  int64_t edit_count_ = 0;
  // each element of endpoint_base_ is the furthest position in base reachable given an
  // edit_count and (# insertions) - (# deletions). Each bit of insert_ records whether
  // the corresponding furthest position was reached via an insertion or a deletion
  // (followed by a run of shared elements). See StorageOffset for the
  // layout of these vectors
  std::vector<int64_t> endpoint_base_;
  std::vector<bool> insert_;
};

Result<std::shared_ptr<StructArray>> NullDiff(const Array& base, const Array& target,
                                              MemoryPool* pool) {
  bool insert = base.length() < target.length();
  auto run_length = std::min(base.length(), target.length());
  auto edit_count = std::max(base.length(), target.length()) - run_length;

  TypedBufferBuilder<bool> insert_builder(pool);
  RETURN_NOT_OK(insert_builder.Resize(edit_count + 1));
  insert_builder.UnsafeAppend(false);
  TypedBufferBuilder<int64_t> run_length_builder(pool);
  RETURN_NOT_OK(run_length_builder.Resize(edit_count + 1));
  run_length_builder.UnsafeAppend(run_length);
  if (edit_count > 0) {
    insert_builder.UnsafeAppend(edit_count, insert);
    run_length_builder.UnsafeAppend(edit_count, 0);
  }

  std::shared_ptr<Buffer> insert_buf, run_length_buf;
  RETURN_NOT_OK(insert_builder.Finish(&insert_buf));
  RETURN_NOT_OK(run_length_builder.Finish(&run_length_buf));

  return StructArray::Make({std::make_shared<BooleanArray>(edit_count + 1, insert_buf),
                            std::make_shared<Int64Array>(edit_count + 1, run_length_buf)},
                           {field("insert", boolean()), field("run_length", int64())});
}

Result<std::shared_ptr<StructArray>> Diff(const Array& base, const Array& target,
                                          MemoryPool* pool) {
  if (!base.type()->Equals(target.type())) {
    return Status::TypeError("only taking the diff of like-typed arrays is supported.");
  }

  if (base.type()->id() == Type::NA) {
    return NullDiff(base, target, pool);
  } else if (base.type()->id() == Type::EXTENSION) {
    auto base_storage = checked_cast<const ExtensionArray&>(base).storage();
    auto target_storage = checked_cast<const ExtensionArray&>(target).storage();
    return Diff(*base_storage, *target_storage, pool);
  } else if (base.type()->id() == Type::DICTIONARY) {
    return Status::NotImplemented("diffing arrays of type ", *base.type());
  } else if (base.type()->id() == Type::LIST_VIEW ||
             base.type()->id() == Type::LARGE_LIST_VIEW) {
    return Status::NotImplemented("diffing arrays of type ", *base.type());
  } else {
    return QuadraticSpaceMyersDiff(base, target, pool).Diff();
  }
}

using Formatter = std::function<void(const Array&, int64_t index, std::ostream*)>;

static Result<Formatter> MakeFormatter(const DataType& type);

class MakeFormatterImpl {
 public:
  Result<Formatter> Make(const DataType& type) && {
    RETURN_NOT_OK(VisitTypeInline(type, this));
    return std::move(impl_);
  }

 private:
  template <typename VISITOR, typename... ARGS>
  friend Status VisitTypeInline(const DataType&, VISITOR*, ARGS&&... args);

  // factory implementation
  Status Visit(const BooleanType&) {
    impl_ = [](const Array& array, int64_t index, std::ostream* os) {
      *os << (checked_cast<const BooleanArray&>(array).Value(index) ? "true" : "false");
    };
    return Status::OK();
  }

  // format Numerics with std::ostream defaults
  template <typename T>
  enable_if_number<T, Status> Visit(const T&) {
    impl_ = [](const Array& array, int64_t index, std::ostream* os) {
      const auto& numeric = checked_cast<const NumericArray<T>&>(array);
      if (sizeof(decltype(numeric.Value(index))) == sizeof(char)) {
        // override std::ostream defaults for /(u|)int8_t/ since they are
        // formatted as potentially unprintable/tty borking characters
        *os << static_cast<int16_t>(numeric.Value(index));
      } else {
        *os << numeric.Value(index);
      }
    };
    return Status::OK();
  }

  template <typename T>
  enable_if_date<T, Status> Visit(const T&) {
    using unit = typename std::conditional<std::is_same<T, Date32Type>::value,
                                           arrow_vendored::date::days,
                                           std::chrono::milliseconds>::type;

    static arrow_vendored::date::sys_days epoch{arrow_vendored::date::jan / 1 / 1970};

    impl_ = [](const Array& array, int64_t index, std::ostream* os) {
      unit value(checked_cast<const NumericArray<T>&>(array).Value(index));
      *os << arrow_vendored::date::format("%F", value + epoch);
    };
    return Status::OK();
  }

  template <typename T>
  enable_if_time<T, Status> Visit(const T&) {
    impl_ = MakeTimeFormatter<T, false>("%T");
    return Status::OK();
  }

  Status Visit(const TimestampType&) {
    impl_ = MakeTimeFormatter<TimestampType, true>("%F %T");
    return Status::OK();
  }

  Status Visit(const DayTimeIntervalType&) {
    impl_ = [](const Array& array, int64_t index, std::ostream* os) {
      auto day_millis = checked_cast<const DayTimeIntervalArray&>(array).Value(index);
      *os << day_millis.days << "d" << day_millis.milliseconds << "ms";
    };
    return Status::OK();
  }

  Status Visit(const MonthDayNanoIntervalType&) {
    impl_ = [](const Array& array, int64_t index, std::ostream* os) {
      auto month_day_nanos =
          checked_cast<const MonthDayNanoIntervalArray&>(array).Value(index);
      *os << month_day_nanos.months << "M" << month_day_nanos.days << "d"
          << month_day_nanos.nanoseconds << "ns";
    };
    return Status::OK();
  }

  template <typename T>
  enable_if_has_string_view<T, Status> Visit(const T&) {
    using ArrayType = typename TypeTraits<T>::ArrayType;
    impl_ = [](const Array& array, int64_t index, std::ostream* os) {
      std::string_view view = checked_cast<const ArrayType&>(array).GetView(index);
      if constexpr (T::is_utf8) {
        // format String and StringView with \"\n\r\t\\ escaped
        *os << '"' << Escape(view) << '"';
      } else {
        // format Binary, LargeBinary, BinaryView, and FixedSizeBinary in hexadecimal
        *os << HexEncode(view);
      }
    };
    return Status::OK();
  }

  // format Decimals with Decimal___Array::FormatValue
  template <typename T>
  enable_if_decimal<T, Status> Visit(const T&) {
    impl_ = [](const Array& array, int64_t index, std::ostream* os) {
      if constexpr (T::type_id == Type::DECIMAL128) {
        *os << checked_cast<const Decimal128Array&>(array).FormatValue(index);
      } else {
        *os << checked_cast<const Decimal256Array&>(array).FormatValue(index);
      }
    };
    return Status::OK();
  }

  template <typename T>
  enable_if_list_like<T, Status> Visit(const T& t) {
    struct ListImpl {
      explicit ListImpl(Formatter f) : values_formatter_(std::move(f)) {}

      void operator()(const Array& array, int64_t index, std::ostream* os) {
        const auto& list_array =
            checked_cast<const typename TypeTraits<T>::ArrayType&>(array);
        *os << "[";
        for (int32_t i = 0; i < list_array.value_length(index); ++i) {
          if (i != 0) {
            *os << ", ";
          }
          values_formatter_(*list_array.values(), i + list_array.value_offset(index), os);
        }
        *os << "]";
      }

      Formatter values_formatter_;
    };

    ARROW_ASSIGN_OR_RAISE(auto values_formatter, MakeFormatter(*t.value_type()));
    impl_ = ListImpl(std::move(values_formatter));
    return Status::OK();
  }

  Status Visit(const ListViewType& t) {
    return Status::NotImplemented("formatting diffs between arrays of type ", t);
  }

  Status Visit(const LargeListViewType& t) {
    return Status::NotImplemented("formatting diffs between arrays of type ", t);
  }

  // TODO(bkietz) format maps better

  Status Visit(const StructType& t) {
    struct StructImpl {
      explicit StructImpl(std::vector<Formatter> f) : field_formatters_(std::move(f)) {}

      void operator()(const Array& array, int64_t index, std::ostream* os) {
        const auto& struct_array = checked_cast<const StructArray&>(array);
        *os << "{";
        for (int i = 0, printed = 0; i < struct_array.num_fields(); ++i) {
          if (printed != 0) {
            *os << ", ";
          }
          if (struct_array.field(i)->IsNull(index)) {
            continue;
          }
          ++printed;
          *os << struct_array.struct_type()->field(i)->name() << ": ";
          field_formatters_[i](*struct_array.field(i), index, os);
        }
        *os << "}";
      }

      std::vector<Formatter> field_formatters_;
    };

    std::vector<Formatter> field_formatters(t.num_fields());
    for (int i = 0; i < t.num_fields(); ++i) {
      ARROW_ASSIGN_OR_RAISE(field_formatters[i], MakeFormatter(*t.field(i)->type()));
    }

    impl_ = StructImpl(std::move(field_formatters));
    return Status::OK();
  }

  Status Visit(const UnionType& t) {
    struct UnionImpl {
      explicit UnionImpl(std::vector<Formatter> f) : field_formatters_(std::move(f)) {}

      void DoFormat(const UnionArray& array, int64_t index, int64_t child_index,
                    std::ostream* os) {
        auto type_code = array.raw_type_codes()[index];
        auto child = array.field(array.child_id(index));

        *os << "{" << static_cast<int16_t>(type_code) << ": ";
        if (child->IsNull(child_index)) {
          *os << "null";
        } else {
          field_formatters_[type_code](*child, child_index, os);
        }
        *os << "}";
      }

      std::vector<Formatter> field_formatters_;
    };

    struct SparseImpl : UnionImpl {
      using UnionImpl::UnionImpl;

      void operator()(const Array& array, int64_t index, std::ostream* os) {
        const auto& union_array = checked_cast<const SparseUnionArray&>(array);
        DoFormat(union_array, index, index, os);
      }
    };

    struct DenseImpl : UnionImpl {
      using UnionImpl::UnionImpl;

      void operator()(const Array& array, int64_t index, std::ostream* os) {
        const auto& union_array = checked_cast<const DenseUnionArray&>(array);
        DoFormat(union_array, index, union_array.raw_value_offsets()[index], os);
      }
    };

    std::vector<Formatter> field_formatters(t.max_type_code() + 1);
    for (int i = 0; i < t.num_fields(); ++i) {
      auto type_id = t.type_codes()[i];
      ARROW_ASSIGN_OR_RAISE(field_formatters[type_id],
                            MakeFormatter(*t.field(i)->type()));
    }

    if (t.mode() == UnionMode::SPARSE) {
      impl_ = SparseImpl(std::move(field_formatters));
    } else {
      impl_ = DenseImpl(std::move(field_formatters));
    }
    return Status::OK();
  }

  Status Visit(const NullType& t) {
    return Status::NotImplemented("formatting diffs between arrays of type ", t);
  }

  Status Visit(const DictionaryType& t) {
    return Status::NotImplemented("formatting diffs between arrays of type ", t);
  }

  Status Visit(const ExtensionType& t) {
    return Status::NotImplemented("formatting diffs between arrays of type ", t);
  }

  Status Visit(const DurationType& t) {
    return Status::NotImplemented("formatting diffs between arrays of type ", t);
  }

  Status Visit(const MonthIntervalType& t) {
    return Status::NotImplemented("formatting diffs between arrays of type ", t);
  }

  Status Visit(const RunEndEncodedType& t) {
    return Status::NotImplemented("formatting diffs between arrays of type ", t);
  }

  template <typename T, bool AddEpoch>
  Formatter MakeTimeFormatter(const std::string& fmt_str) {
    return [fmt_str](const Array& array, int64_t index, std::ostream* os) {
      auto fmt = fmt_str.c_str();
      auto unit = checked_cast<const T&>(*array.type()).unit();
      auto value = checked_cast<const NumericArray<T>&>(array).Value(index);
      // Using unqualified `format` directly would produce ambiguous
      // lookup because of `std::format` (ARROW-15520).
      namespace avd = arrow_vendored::date;
      using std::chrono::nanoseconds;
      using std::chrono::microseconds;
      using std::chrono::milliseconds;
      using std::chrono::seconds;
      if (AddEpoch) {
        static avd::sys_days epoch{avd::jan / 1 / 1970};

        switch (unit) {
          case TimeUnit::NANO:
            *os << avd::format(fmt, static_cast<nanoseconds>(value) + epoch);
            break;
          case TimeUnit::MICRO:
            *os << avd::format(fmt, static_cast<microseconds>(value) + epoch);
            break;
          case TimeUnit::MILLI:
            *os << avd::format(fmt, static_cast<milliseconds>(value) + epoch);
            break;
          case TimeUnit::SECOND:
            *os << avd::format(fmt, static_cast<seconds>(value) + epoch);
            break;
        }
        return;
      }
      switch (unit) {
        case TimeUnit::NANO:
          *os << avd::format(fmt, static_cast<nanoseconds>(value));
          break;
        case TimeUnit::MICRO:
          *os << avd::format(fmt, static_cast<microseconds>(value));
          break;
        case TimeUnit::MILLI:
          *os << avd::format(fmt, static_cast<milliseconds>(value));
          break;
        case TimeUnit::SECOND:
          *os << avd::format(fmt, static_cast<seconds>(value));
          break;
      }
    };
  }

  Formatter impl_;
};

static Result<Formatter> MakeFormatter(const DataType& type) {
  return MakeFormatterImpl{}.Make(type);
}

Status VisitEditScript(
    const Array& edits,
    const std::function<Status(int64_t delete_begin, int64_t delete_end,
                               int64_t insert_begin, int64_t insert_end)>& visitor) {
  static const auto edits_type =
      struct_({field("insert", boolean()), field("run_length", int64())});
  DCHECK(edits.type()->Equals(*edits_type));
  DCHECK_GE(edits.length(), 1);

  auto insert = checked_pointer_cast<BooleanArray>(
      checked_cast<const StructArray&>(edits).field(0));
  auto run_lengths =
      checked_pointer_cast<Int64Array>(checked_cast<const StructArray&>(edits).field(1));

  DCHECK(!insert->Value(0));

  auto length = run_lengths->Value(0);
  int64_t base_begin, base_end, target_begin, target_end;
  base_begin = base_end = target_begin = target_end = length;
  for (int64_t i = 1; i < edits.length(); ++i) {
    if (insert->Value(i)) {
      ++target_end;
    } else {
      ++base_end;
    }
    length = run_lengths->Value(i);
    if (length != 0) {
      RETURN_NOT_OK(visitor(base_begin, base_end, target_begin, target_end));
      base_begin = base_end = base_end + length;
      target_begin = target_end = target_end + length;
    }
  }
  if (length == 0) {
    return visitor(base_begin, base_end, target_begin, target_end);
  }
  return Status::OK();
}

class UnifiedDiffFormatter {
 public:
  UnifiedDiffFormatter(std::ostream* os, Formatter formatter)
      : os_(os), formatter_(std::move(formatter)) {}

  Status operator()(int64_t delete_begin, int64_t delete_end, int64_t insert_begin,
                    int64_t insert_end) {
    *os_ << "@@ -" << delete_begin << ", +" << insert_begin << " @@" << std::endl;

    for (int64_t i = delete_begin; i < delete_end; ++i) {
      *os_ << "-";
      if (base_->IsValid(i)) {
        formatter_(*base_, i, &*os_);
      } else {
        *os_ << "null";
      }
      *os_ << std::endl;
    }

    for (int64_t i = insert_begin; i < insert_end; ++i) {
      *os_ << "+";
      if (target_->IsValid(i)) {
        formatter_(*target_, i, &*os_);
      } else {
        *os_ << "null";
      }
      *os_ << std::endl;
    }

    return Status::OK();
  }

  Status operator()(const Array& edits, const Array& base, const Array& target) {
    if (edits.length() == 1) {
      return Status::OK();
    }
    base_ = &base;
    target_ = &target;
    *os_ << std::endl;
    return VisitEditScript(edits, *this);
  }

 private:
  std::ostream* os_ = nullptr;
  const Array* base_ = nullptr;
  const Array* target_ = nullptr;
  Formatter formatter_;
};

Result<std::function<Status(const Array& edits, const Array& base, const Array& target)>>
MakeUnifiedDiffFormatter(const DataType& type, std::ostream* os) {
  if (type.id() == Type::NA) {
    return [os](const Array& edits, const Array& base, const Array& target) {
      if (base.length() != target.length()) {
        *os << "# Null arrays differed" << std::endl
            << "-" << base.length() << " nulls" << std::endl
            << "+" << target.length() << " nulls" << std::endl;
      }
      return Status::OK();
    };
  }

  ARROW_ASSIGN_OR_RAISE(auto formatter, MakeFormatter(type));
  return UnifiedDiffFormatter(os, std::move(formatter));
}

}  // namespace arrow

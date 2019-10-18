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
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/buffer_builder.h"
#include "arrow/memory_pool.h"
#include "arrow/pretty_print.h"
#include "arrow/status.h"
#include "arrow/type_traits.h"
#include "arrow/util/logging.h"
#include "arrow/util/range.h"
#include "arrow/util/stl.h"
#include "arrow/util/string.h"
#include "arrow/util/variant.h"
#include "arrow/util/visibility.h"
#include "arrow/vendored/datetime.h"
#include "arrow/visitor_inline.h"

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
          typename =
              typename std::enable_if<std::is_base_of<BaseListType, T>::value ||
                                      std::is_base_of<FixedSizeListType, T>::value>::type>
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

struct NullTag {
  constexpr bool operator==(const NullTag& other) const { return true; }
  constexpr bool operator!=(const NullTag& other) const { return false; }
};

template <typename T>
class NullOr {
 public:
  using VariantType = util::variant<NullTag, T>;

  NullOr() : variant_(NullTag{}) {}
  explicit NullOr(T t) : variant_(std::move(t)) {}

  template <typename ArrayType>
  NullOr(const ArrayType& array, int64_t index) {
    if (array.IsNull(index)) {
      variant_.emplace(NullTag{});
    } else {
      variant_.emplace(GetView(array, index));
    }
  }

  bool operator==(const NullOr& other) const { return variant_ == other.variant_; }
  bool operator!=(const NullOr& other) const { return variant_ != other.variant_; }

 private:
  VariantType variant_;
};

template <typename ArrayType>
using ViewType = decltype(GetView(std::declval<ArrayType>(), 0));

template <typename ArrayType>
class ViewGenerator {
 public:
  using View = ViewType<ArrayType>;

  explicit ViewGenerator(const Array& array)
      : array_(checked_cast<const ArrayType&>(array)) {
    DCHECK_EQ(array.null_count(), 0);
  }

  View operator()(int64_t index) const { return GetView(array_, index); }

 private:
  const ArrayType& array_;
};

template <typename ArrayType>
internal::LazyRange<ViewGenerator<ArrayType>> MakeViewRange(const Array& array) {
  using Generator = ViewGenerator<ArrayType>;
  return internal::LazyRange<Generator>(Generator(array), array.length());
}

template <typename ArrayType>
class NullOrViewGenerator {
 public:
  using View = ViewType<ArrayType>;

  explicit NullOrViewGenerator(const Array& array)
      : array_(checked_cast<const ArrayType&>(array)) {}

  NullOr<View> operator()(int64_t index) const {
    return array_.IsNull(index) ? NullOr<View>() : NullOr<View>(GetView(array_, index));
  }

 private:
  const ArrayType& array_;
};

template <typename ArrayType>
internal::LazyRange<NullOrViewGenerator<ArrayType>> MakeNullOrViewRange(
    const Array& array) {
  using Generator = NullOrViewGenerator<ArrayType>;
  return internal::LazyRange<Generator>(Generator(array), array.length());
}

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
template <typename Iterator>
class QuadraticSpaceMyersDiff {
 public:
  // represents an intermediate state in the comparison of two arrays
  struct EditPoint {
    Iterator base, target;

    bool operator==(EditPoint other) const {
      return base == other.base && target == other.target;
    }
  };

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
    for (; p.base != base_end_ && p.target != target_end_; ++p.base, ++p.target) {
      if (*p.base != *p.target) {
        break;
      }
    }
    return p;
  }

  QuadraticSpaceMyersDiff(Iterator base_begin, Iterator base_end, Iterator target_begin,
                          Iterator target_end)
      : base_begin_(base_begin),
        base_end_(base_end),
        target_begin_(target_begin),
        target_end_(target_end),
        endpoint_base_({ExtendFrom({base_begin_, target_begin_}).base}),
        insert_({true}) {
    if (std::distance(base_begin_, base_end_) ==
            std::distance(target_begin_, target_end_) &&
        endpoint_base_[0] == base_end_) {
      // trivial case: base == target
      finish_index_ = 0;
    }
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
    std::shared_ptr<Buffer> insert_buf, run_length_buf;
    RETURN_NOT_OK(AllocateBitmap(pool, length, &insert_buf));
    RETURN_NOT_OK(AllocateBuffer(pool, length * sizeof(int64_t), &run_length_buf));
    auto run_length = reinterpret_cast<int64_t*>(run_length_buf->mutable_data());

    auto index = finish_index_;
    auto endpoint = GetEditPoint(edit_count_, finish_index_);

    for (int64_t i = edit_count_; i > 0; --i) {
      bool insert = insert_[index];
      BitUtil::SetBitTo(insert_buf->mutable_data(), i, insert);

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
    BitUtil::SetBitTo(insert_buf->mutable_data(), 0, false);
    run_length[0] = endpoint.base - base_begin_;

    return StructArray::Make({std::make_shared<BooleanArray>(length, insert_buf),
                              std::make_shared<Int64Array>(length, run_length_buf)},
                             {field("insert", boolean()), field("run_length", int64())});
  }

 private:
  int64_t finish_index_ = -1;
  int64_t edit_count_ = 0;
  Iterator base_begin_, base_end_;
  Iterator target_begin_, target_end_;
  // each element of endpoint_base_ is the furthest position in base reachable given an
  // edit_count and (# insertions) - (# deletions). Each bit of insert_ records whether
  // the corresponding furthest position was reached via an insertion or a deletion
  // (followed by a run of shared elements). See StorageOffset for the
  // layout of these vectors
  std::vector<Iterator> endpoint_base_;
  std::vector<bool> insert_;
};

struct DiffImpl {
  Status Visit(const NullType&) {
    bool insert = base_.length() < target_.length();
    auto run_length = std::min(base_.length(), target_.length());
    auto edit_count = std::max(base_.length(), target_.length()) - run_length;

    TypedBufferBuilder<bool> insert_builder(pool_);
    RETURN_NOT_OK(insert_builder.Resize(edit_count + 1));
    insert_builder.UnsafeAppend(false);
    TypedBufferBuilder<int64_t> run_length_builder(pool_);
    RETURN_NOT_OK(run_length_builder.Resize(edit_count + 1));
    run_length_builder.UnsafeAppend(run_length);
    if (edit_count > 0) {
      insert_builder.UnsafeAppend(edit_count, insert);
      run_length_builder.UnsafeAppend(edit_count, 0);
    }

    std::shared_ptr<Buffer> insert_buf, run_length_buf;
    RETURN_NOT_OK(insert_builder.Finish(&insert_buf));
    RETURN_NOT_OK(run_length_builder.Finish(&run_length_buf));

    ARROW_ASSIGN_OR_RAISE(
        out_,
        StructArray::Make({std::make_shared<BooleanArray>(edit_count + 1, insert_buf),
                           std::make_shared<Int64Array>(edit_count + 1, run_length_buf)},
                          {field("insert", boolean()), field("run_length", int64())}));
    return Status::OK();
  }

  template <typename T>
  Status Visit(const T&) {
    using ArrayType = typename TypeTraits<T>::ArrayType;
    if (base_.null_count() == 0 && target_.null_count() == 0) {
      auto base = MakeViewRange<ArrayType>(base_);
      auto target = MakeViewRange<ArrayType>(target_);
      ARROW_ASSIGN_OR_RAISE(out_,
                            Diff(base.begin(), base.end(), target.begin(), target.end()));
    } else {
      auto base = MakeNullOrViewRange<ArrayType>(base_);
      auto target = MakeNullOrViewRange<ArrayType>(target_);
      ARROW_ASSIGN_OR_RAISE(out_,
                            Diff(base.begin(), base.end(), target.begin(), target.end()));
    }
    return Status::OK();
  }

  Status Visit(const ExtensionType&) {
    auto base = checked_cast<const ExtensionArray&>(base_).storage();
    auto target = checked_cast<const ExtensionArray&>(target_).storage();
    ARROW_ASSIGN_OR_RAISE(out_, arrow::Diff(*base, *target, pool_));
    return Status::OK();
  }

  Status Visit(const DictionaryType& t) {
    return Status::NotImplemented("diffing arrays of type ", t);
  }

  Result<std::shared_ptr<StructArray>> Diff() {
    RETURN_NOT_OK(VisitTypeInline(*base_.type(), this));
    return out_;
  }

  template <typename Iterator>
  Result<std::shared_ptr<StructArray>> Diff(Iterator base_begin, Iterator base_end,
                                            Iterator target_begin, Iterator target_end) {
    QuadraticSpaceMyersDiff<Iterator> impl(base_begin, base_end, target_begin,
                                           target_end);
    while (!impl.Done()) {
      impl.Next();
    }
    return impl.GetEdits(pool_);
  }

  const Array& base_;
  const Array& target_;
  MemoryPool* pool_;
  std::shared_ptr<StructArray> out_;
};

Result<std::shared_ptr<StructArray>> Diff(const Array& base, const Array& target,
                                          MemoryPool* pool) {
  if (!base.type()->Equals(target.type())) {
    return Status::TypeError("only taking the diff of like-typed arrays is supported.");
  }

  return DiffImpl{base, target, pool, nullptr}.Diff();
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
  template <typename VISITOR>
  friend Status VisitTypeInline(const DataType&, VISITOR*);

  // factory implementation
  Status Visit(const BooleanType&) {
    impl_ = [](const Array& array, int64_t index, std::ostream* os) {
      *os << (checked_cast<const BooleanArray&>(array).Value(index) ? "true" : "false");
    };
    return Status::OK();
  }

  // format Numerics with std::ostream defaults
  template <typename T>
  typename std::enable_if<
      is_number_type<T>::value && !is_date<T>::value && !is_time<T>::value, Status>::type
  Visit(const T&) {
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

  // format Binary and FixedSizeBinary in hexadecimal
  template <typename T>
  enable_if_binary_like<T, Status> Visit(const T&) {
    impl_ = [](const Array& array, int64_t index, std::ostream* os) {
      *os << HexEncode(
          checked_cast<const typename TypeTraits<T>::ArrayType&>(array).GetView(index));
    };
    return Status::OK();
  }

  // format Strings with \"\n\r\t\\ escaped
  Status Visit(const StringType&) {
    impl_ = [](const Array& array, int64_t index, std::ostream* os) {
      *os << "\"" << Escape(checked_cast<const StringArray&>(array).GetView(index))
          << "\"";
    };
    return Status::OK();
  }

  // format Decimals with Decimal128Array::FormatValue
  Status Visit(const Decimal128Type&) {
    impl_ = [](const Array& array, int64_t index, std::ostream* os) {
      *os << checked_cast<const Decimal128Array&>(array).FormatValue(index);
    };
    return Status::OK();
  }

  Status Visit(const ListType& t) {
    struct ListImpl {
      explicit ListImpl(Formatter f) : values_formatter_(std::move(f)) {}

      void operator()(const Array& array, int64_t index, std::ostream* os) {
        const auto& list_array = checked_cast<const ListArray&>(array);
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
          *os << struct_array.struct_type()->child(i)->name() << ": ";
          field_formatters_[i](*struct_array.field(i), index, os);
        }
        *os << "}";
      }

      std::vector<Formatter> field_formatters_;
    };

    std::vector<Formatter> field_formatters(t.num_children());
    for (int i = 0; i < t.num_children(); ++i) {
      ARROW_ASSIGN_OR_RAISE(field_formatters[i], MakeFormatter(*t.child(i)->type()));
    }

    impl_ = StructImpl(std::move(field_formatters));
    return Status::OK();
  }

  Status Visit(const UnionType& t) {
    struct UnionImpl {
      UnionImpl(std::vector<Formatter> f, std::vector<int> c)
          : field_formatters_(std::move(f)), type_id_to_child_index_(std::move(c)) {}

      void DoFormat(const UnionArray& array, int64_t index, int64_t child_index,
                    std::ostream* os) {
        auto type_id = array.raw_type_ids()[index];
        const auto& child = *array.child(type_id_to_child_index_[type_id]);

        *os << "{" << static_cast<int16_t>(type_id) << ": ";
        if (child.IsNull(child_index)) {
          *os << "null";
        } else {
          field_formatters_[type_id](child, child_index, os);
        }
        *os << "}";
      }

      std::vector<Formatter> field_formatters_;
      std::vector<int> type_id_to_child_index_;
    };

    struct SparseImpl : UnionImpl {
      using UnionImpl::UnionImpl;

      void operator()(const Array& array, int64_t index, std::ostream* os) {
        const auto& union_array = checked_cast<const UnionArray&>(array);
        DoFormat(union_array, index, index, os);
      }
    };

    struct DenseImpl : UnionImpl {
      using UnionImpl::UnionImpl;

      void operator()(const Array& array, int64_t index, std::ostream* os) {
        const auto& union_array = checked_cast<const UnionArray&>(array);
        DoFormat(union_array, index, union_array.raw_value_offsets()[index], os);
      }
    };

    std::vector<Formatter> field_formatters(t.max_type_code() + 1);
    std::vector<int> type_id_to_child_index(field_formatters.size());
    for (int i = 0; i < t.num_children(); ++i) {
      auto type_id = t.type_codes()[i];
      type_id_to_child_index[type_id] = i;
      ARROW_ASSIGN_OR_RAISE(field_formatters[type_id],
                            MakeFormatter(*t.child(i)->type()));
    }

    if (t.mode() == UnionMode::SPARSE) {
      impl_ = SparseImpl(std::move(field_formatters), std::move(type_id_to_child_index));
    } else {
      impl_ = DenseImpl(std::move(field_formatters), std::move(type_id_to_child_index));
    }
    return Status::OK();
  }

  Status Visit(const DataType& t) {
    return Status::NotImplemented("formatting diffs between arrays of type ", t);
  }

  template <typename T, bool AddEpoch>
  Formatter MakeTimeFormatter(const std::string& fmt_str) {
    return [fmt_str](const Array& array, int64_t index, std::ostream* os) {
      auto fmt = fmt_str.c_str();
      auto unit = checked_cast<const T&>(*array.type()).unit();
      auto value = checked_cast<const NumericArray<T>&>(array).Value(index);
      using arrow_vendored::date::format;
      using std::chrono::nanoseconds;
      using std::chrono::microseconds;
      using std::chrono::milliseconds;
      using std::chrono::seconds;
      if (AddEpoch) {
        static arrow_vendored::date::sys_days epoch{arrow_vendored::date::jan / 1 / 1970};

        switch (unit) {
          case TimeUnit::NANO:
            *os << format(fmt, static_cast<nanoseconds>(value) + epoch);
            break;
          case TimeUnit::MICRO:
            *os << format(fmt, static_cast<microseconds>(value) + epoch);
            break;
          case TimeUnit::MILLI:
            *os << format(fmt, static_cast<milliseconds>(value) + epoch);
            break;
          case TimeUnit::SECOND:
            *os << format(fmt, static_cast<seconds>(value) + epoch);
            break;
        }
        return;
      }
      switch (unit) {
        case TimeUnit::NANO:
          *os << format(fmt, static_cast<nanoseconds>(value));
          break;
        case TimeUnit::MICRO:
          *os << format(fmt, static_cast<microseconds>(value));
          break;
        case TimeUnit::MILLI:
          *os << format(fmt, static_cast<milliseconds>(value));
          break;
        case TimeUnit::SECOND:
          *os << format(fmt, static_cast<seconds>(value));
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
  ARROW_ASSIGN_OR_RAISE(auto formatter, MakeFormatter(type));
  return UnifiedDiffFormatter(os, std::move(formatter));
}

}  // namespace arrow

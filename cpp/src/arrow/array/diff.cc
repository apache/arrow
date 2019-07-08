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
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/type_traits.h"
#include "arrow/util/lazy.h"
#include "arrow/util/logging.h"
#include "arrow/util/string.h"
#include "arrow/util/visibility.h"
#include "arrow/visitor_inline.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;
using internal::MakeLazyRange;

template <typename Iterator>
class DiffImpl {
 public:
  // represents an intermediate state in the comparison of two arrays
  struct EditPoint {
    Iterator base, target;

    bool operator==(EditPoint other) const {
      return base == other.base && target == other.target;
    }
  };

  DiffImpl(Iterator base_begin, Iterator base_end, Iterator target_begin,
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
      finish_index_ = 0;
    }
  }

  // beginning of a range for storing per-edit state in endpoint_base_ and insert_
  uint64_t begin(uint64_t edit_count) const { return edit_count * (edit_count + 1) / 2; }

  // end of a range for storing per-edit state in endpoint_base_ and insert_
  uint64_t end(uint64_t edit_count) const { return begin(edit_count + 1); }

  EditPoint GetEditPoint(uint64_t edit_count, uint64_t index) const {
    DCHECK_GE(index, begin(edit_count));
    DCHECK_LT(index, end(edit_count));
    int64_t base_distance = endpoint_base_[index] - base_begin_;
    auto k = static_cast<int64_t>(edit_count) - 2 * (index - begin(edit_count));
    return {endpoint_base_[index], target_begin_ + (base_distance - k)};
  }

  EditPoint DeleteOne(EditPoint p) const {
    if (p.base != base_end_) {
      ++p.base;
    }
    return p;
  }

  EditPoint InsertOne(EditPoint p) const {
    if (p.target != target_end_) {
      ++p.target;
    }
    return p;
  }

  EditPoint ExtendFrom(EditPoint p) const {
    for (; p.base != base_end_ && p.target != target_end_; ++p.base, ++p.target) {
      if (*p.base != *p.target) {
        break;
      }
    }
    return p;
  }

  void Next() {
    endpoint_base_.resize(end(edit_count_ + 1), base_begin_);
    insert_.resize(end(edit_count_ + 1), false);

    // try deleting from base first
    for (uint64_t i = begin(edit_count_), i_out = end(edit_count_); i != end(edit_count_);
         ++i, ++i_out) {
      endpoint_base_[i_out] = ExtendFrom(DeleteOne(GetEditPoint(edit_count_, i))).base;
    }

    // check if inserting from target could do better
    for (uint64_t i = begin(edit_count_), i_out = end(edit_count_) + 1;
         i != end(edit_count_); ++i, ++i_out) {
      auto x_endpoint = GetEditPoint(edit_count_ + 1, i_out);

      endpoint_base_[i_out] = ExtendFrom(InsertOne(GetEditPoint(edit_count_, i))).base;
      auto y_endpoint = GetEditPoint(edit_count_ + 1, i_out);

      if (y_endpoint.base - x_endpoint.base >= 0) {
        insert_[i_out] = true;
      } else {
        endpoint_base_[i_out] = x_endpoint.base;
      }
    }

    ++edit_count_;

    // check for completion
    EditPoint finish = {base_end_, target_end_};
    for (uint64_t i = begin(edit_count_); i != end(edit_count_); ++i) {
      if (GetEditPoint(edit_count_, i) == finish) {
        finish_index_ = i;
        return;
      }
    }
  }

  bool Done() { return finish_index_ != static_cast<uint64_t>(-1); }

  Status GetEdits(MemoryPool* pool, std::shared_ptr<Array>* out) {
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

      auto x_minus_y = (endpoint.base - base_begin_) - (endpoint.target - target_begin_);
      if (insert) {
        ++x_minus_y;
      } else {
        --x_minus_y;
      }
      index = (i - 1 - x_minus_y) / 2 + begin(i - 1);

      // endpoint of previous edit
      auto previous = GetEditPoint(i - 1, index);
      run_length[i] = endpoint.base - previous.base - !insert;

      endpoint = previous;
    }
    BitUtil::SetBitTo(insert_buf->mutable_data(), 0, false);
    run_length[0] = endpoint.base - base_begin_;

    ARROW_ASSIGN_OR_RAISE(
        *out, StructArray::Make({std::make_shared<BooleanArray>(length, insert_buf),
                                 std::make_shared<UInt64Array>(length, run_length_buf)},
                                {"insert", "run_length"}));
    return Status::OK();
  }

 private:
  uint64_t finish_index_ = -1;
  uint64_t edit_count_ = 0;
  Iterator base_begin_, base_end_;
  Iterator target_begin_, target_end_;
  std::vector<Iterator> endpoint_base_;
  std::vector<bool> insert_;
};

// check whether a type's array class is an instantiation of NumericArray
template <typename T>
struct array_is_numeric {
  static std::false_type test(...);

  template <typename U>
  static std::true_type test(NumericArray<U>*);

  static constexpr bool value = decltype(array_is_numeric::test(
      static_cast<typename TypeTraits<T>::ArrayType*>(nullptr)))::value;
};

static_assert(array_is_numeric<Int32Type>::value, "int32");
static_assert(!array_is_numeric<BinaryType>::value, "binary");
static_assert(array_is_numeric<Date32Type>::value, "date32");

struct DiffImplVisitor {
  template <typename T>
  typename std::enable_if<array_is_numeric<T>::value, Status>::type Visit(const T&) {
    auto base_data = checked_cast<const NumericArray<T>&>(base_).raw_values();
    auto target_data = checked_cast<const NumericArray<T>&>(target_).raw_values();
    return Diff(base_data, base_data + base_.length(), target_data,
                target_data + target_.length());
  }

  template <typename T>
  enable_if_binary_like<T, Status> Visit(const T&) {
    using ArrayType = typename TypeTraits<T>::ArrayType;
    struct ViewGenerator {
      explicit ViewGenerator(const Array& arr)
          : arr_(checked_cast<const ArrayType&>(arr)) {}

      util::string_view operator()(int64_t i) const { return arr_.GetView(i); }

      const ArrayType& arr_;
    };

    auto base = MakeLazyRange(ViewGenerator(base_), base_.length());
    auto target = MakeLazyRange(ViewGenerator(target_), target_.length());
    return Diff(base.begin(), base.end(), target.begin(), target.end());
  }

  Status Visit(const DataType& t) {
    return Status::NotImplemented("diffing arrays of type ", t);
  }

  Status Diff() { return VisitTypeInline(*base_.type(), this); }

  template <typename Iterator>
  Status Diff(Iterator base_begin, Iterator base_end, Iterator target_begin,
              Iterator target_end) {
    DiffImpl<Iterator> impl(base_begin, base_end, target_begin, target_end);
    while (!impl.Done()) {
      impl.Next();
    }
    return impl.GetEdits(pool_, out_);
  }

  const Array& base_;
  const Array& target_;
  MemoryPool* pool_;
  std::shared_ptr<Array>* out_;
};

Status Diff(const Array& base, const Array& target, MemoryPool* pool,
            std::shared_ptr<Array>* out) {
  if (!base.type()->Equals(target.type())) {
    return Status::TypeError("only taking the diff of like-typed arrays is supported.");
  }

  if (base.null_count() != 0 || target.null_count() != 0) {
    return Status::NotImplemented(
        "taking the diff of arrays with nulls is not supported");
  }

  return DiffImplVisitor{base, target, pool, out}.Diff();
}

Status DiffVisitor::Visit(const Array& edits) {
  static const auto edits_type =
      struct_({field("insert", boolean()), field("run_length", uint64())});
  DCHECK(edits.type()->Equals(*edits_type));
  DCHECK_GE(edits.length(), 1);

  auto insert = checked_pointer_cast<BooleanArray>(
      checked_cast<const StructArray&>(edits).field(0));
  auto run_lengths =
      checked_pointer_cast<UInt64Array>(checked_cast<const StructArray&>(edits).field(1));

  DCHECK(!insert->Value(0));

  auto length = run_lengths->Value(0);
  RETURN_NOT_OK(Run(length));

  int64_t base_index = length, target_index = length;
  for (int64_t i = 1; i < edits.length(); ++i) {
    if (insert->Value(i)) {
      RETURN_NOT_OK(Insert(target_index));
      ++target_index;
    } else {
      RETURN_NOT_OK(Delete(base_index));
      ++base_index;
    }
    length = run_lengths->Value(i);
    RETURN_NOT_OK(Run(length));
    base_index += length;
    target_index += length;
  }
  return Status::OK();
}

// format Numerics with std::ostream defaults
template <typename T>
void Format(std::ostream& os, const NumericArray<T>& arr, int64_t index) {
  os << arr.Value(index) << std::endl;
}

// format Binary and FixedSizeBinary in hexadecimal
template <typename A>
enable_if_binary_like<A> Format(std::ostream& os, const A& arr, int64_t index) {
  os << HexEncode(arr.GetView(index)) << std::endl;
}

// format Strings with \"\n\r\t\\ escaped
void Format(std::ostream& os, const StringArray& arr, int64_t index) {
  os << "\"" << Escape(arr.GetView(index)) << "\"" << std::endl;
}

// format Decimals with Decimal128Array::FormatValue
void Format(std::ostream& os, const Decimal128Array& arr, int64_t index) {
  os << arr.FormatValue(index) << std::endl;
}

template <typename ArrayType>
class UnifiedDiffFormatter : public DiffVisitor {
 public:
  UnifiedDiffFormatter(std::ostream& os, const Array& base, const Array& target)
      : os_(os),
        base_(checked_cast<const ArrayType&>(base)),
        target_(checked_cast<const ArrayType&>(target)) {
    os_ << std::endl;
  }

  Status Insert(int64_t target_index) override {
    ++target_end_;
    return Status::OK();
  }

  Status Delete(int64_t base_index) override {
    ++base_end_;
    return Status::OK();
  }

  Status Run(int64_t length) override {
    if (target_begin_ == target_end_ && base_begin_ == base_end_) {
      // this is the first run, so don't write the preceding (empty) hunk
      base_begin_ = base_end_ = target_begin_ = target_end_ = length;
      return Status::OK();
    }
    if (length == 0 &&
        !(base_end_ == base_.length() && target_end_ == target_.length())) {
      return Status::OK();
    }
    // non trivial run- finalize the current hunk
    os_ << "@@ -" << base_begin_ << ", +" << target_begin_ << " @@" << std::endl;
    for (int64_t i = base_begin_; i < base_end_; ++i) {
      Format(os_ << "-", base_, i);
    }
    base_begin_ = base_end_ += length;
    for (int64_t i = target_begin_; i < target_end_; ++i) {
      Format(os_ << "+", target_, i);
    }
    target_begin_ = target_end_ += length;
    return Status::OK();
  }

 private:
  std::ostream& os_;
  int64_t base_begin_ = 0, base_end_ = 0, target_begin_ = 0, target_end_ = 0;
  const ArrayType& base_;
  const ArrayType& target_;
};

struct MakeUnifiedDiffFormatterImpl {
  template <typename A>
  Status Visit(const A&, decltype(Format(std::declval<std::ostream&>(),
                                         std::declval<const A&>(), 0))* = nullptr) {
    out_.reset(new UnifiedDiffFormatter<A>(os_, base_, target_));
    return Status::OK();
  }

  Status Visit(const Array&) {
    return Status::NotImplemented("formatting diffs between arrays of type ",
                                  *base_.type());
  }

  Result<std::unique_ptr<DiffVisitor>> Make() {
    RETURN_NOT_OK(VisitArrayInline(base_, this));
    return std::move(out_);
  }

  std::ostream& os_;
  const Array& base_;
  const Array& target_;
  std::unique_ptr<DiffVisitor> out_;
};

Result<std::unique_ptr<DiffVisitor>> MakeUnifiedDiffFormatter(std::ostream& os,
                                                              const Array& base,
                                                              const Array& target) {
  if (!base.type()->Equals(target.type())) {
    return Status::TypeError(
        "diffs may only be generated between arrays of like type, got ", *base.type(),
        " and ", *target.type());
  }

  return MakeUnifiedDiffFormatterImpl{os, base, target, nullptr}.Make();
}

}  // namespace arrow

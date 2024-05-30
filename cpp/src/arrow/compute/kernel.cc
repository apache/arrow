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

#include "arrow/compute/kernel.h"

#include <cstddef>
#include <memory>
#include <sstream>
#include <string>

#include "arrow/buffer.h"
#include "arrow/compute/exec.h"
#include "arrow/result.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/hash_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

namespace arrow {

using internal::checked_cast;
using internal::hash_combine;

static constexpr size_t kHashSeed = 0;

namespace compute {

// ----------------------------------------------------------------------
// KernelContext

Result<std::shared_ptr<ResizableBuffer>> KernelContext::Allocate(int64_t nbytes) {
  return AllocateResizableBuffer(nbytes, exec_ctx_->memory_pool());
}

Result<std::shared_ptr<ResizableBuffer>> KernelContext::AllocateBitmap(int64_t num_bits) {
  const int64_t nbytes = bit_util::BytesForBits(num_bits);
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ResizableBuffer> result,
                        AllocateResizableBuffer(nbytes, exec_ctx_->memory_pool()));
  // Since bitmaps are typically written bit by bit, we could leak uninitialized bits.
  // Make sure all memory is initialized (this also appeases Valgrind).
  std::memset(result->mutable_data(), 0, result->size());
  return result;
}

Status Kernel::InitAll(KernelContext* ctx, const KernelInitArgs& args,
                       std::vector<std::unique_ptr<KernelState>>* states) {
  for (auto& state : *states) {
    ARROW_ASSIGN_OR_RAISE(state, args.kernel->init(ctx, args));
  }
  return Status::OK();
}

Result<std::unique_ptr<KernelState>> ScalarAggregateKernel::MergeAll(
    const ScalarAggregateKernel* kernel, KernelContext* ctx,
    std::vector<std::unique_ptr<KernelState>> states) {
  auto out = std::move(states.back());
  states.pop_back();
  ctx->SetState(out.get());
  for (auto& state : states) {
    RETURN_NOT_OK(kernel->merge(ctx, std::move(*state), out.get()));
  }
  return out;
}

// ----------------------------------------------------------------------
// Some basic TypeMatcher implementations

namespace match {

class SameTypeIdMatcher : public TypeMatcher {
 public:
  explicit SameTypeIdMatcher(Type::type accepted_id) : accepted_id_(accepted_id) {}

  bool Matches(const DataType& type) const override { return type.id() == accepted_id_; }

  std::string ToString() const override {
    std::stringstream ss;
    ss << "Type::" << ::arrow::internal::ToString(accepted_id_);
    return ss.str();
  }

  bool Equals(const TypeMatcher& other) const override {
    if (this == &other) {
      return true;
    }
    auto casted = dynamic_cast<const SameTypeIdMatcher*>(&other);
    if (casted == nullptr) {
      return false;
    }
    return this->accepted_id_ == casted->accepted_id_;
  }

 private:
  Type::type accepted_id_;
};

std::shared_ptr<TypeMatcher> SameTypeId(Type::type type_id) {
  return std::make_shared<SameTypeIdMatcher>(type_id);
}

template <typename ArrowType>
class TimeUnitMatcher : public TypeMatcher {
  using ThisType = TimeUnitMatcher<ArrowType>;

 public:
  explicit TimeUnitMatcher(TimeUnit::type accepted_unit)
      : accepted_unit_(accepted_unit) {}

  bool Matches(const DataType& type) const override {
    if (type.id() != ArrowType::type_id) {
      return false;
    }
    const auto& time_type = checked_cast<const ArrowType&>(type);
    return time_type.unit() == accepted_unit_;
  }

  bool Equals(const TypeMatcher& other) const override {
    if (this == &other) {
      return true;
    }
    auto casted = dynamic_cast<const ThisType*>(&other);
    if (casted == nullptr) {
      return false;
    }
    return this->accepted_unit_ == casted->accepted_unit_;
  }

  std::string ToString() const override {
    std::stringstream ss;
    ss << ArrowType::type_name() << "(" << ::arrow::internal::ToString(accepted_unit_)
       << ")";
    return ss.str();
  }

 private:
  TimeUnit::type accepted_unit_;
};

using DurationTypeUnitMatcher = TimeUnitMatcher<DurationType>;
using Time32TypeUnitMatcher = TimeUnitMatcher<Time32Type>;
using Time64TypeUnitMatcher = TimeUnitMatcher<Time64Type>;
using TimestampTypeUnitMatcher = TimeUnitMatcher<TimestampType>;

std::shared_ptr<TypeMatcher> TimestampTypeUnit(TimeUnit::type unit) {
  return std::make_shared<TimestampTypeUnitMatcher>(unit);
}

std::shared_ptr<TypeMatcher> Time32TypeUnit(TimeUnit::type unit) {
  return std::make_shared<Time32TypeUnitMatcher>(unit);
}

std::shared_ptr<TypeMatcher> Time64TypeUnit(TimeUnit::type unit) {
  return std::make_shared<Time64TypeUnitMatcher>(unit);
}

std::shared_ptr<TypeMatcher> DurationTypeUnit(TimeUnit::type unit) {
  return std::make_shared<DurationTypeUnitMatcher>(unit);
}

class IntegerMatcher : public TypeMatcher {
 public:
  IntegerMatcher() {}

  bool Matches(const DataType& type) const override { return is_integer(type.id()); }

  bool Equals(const TypeMatcher& other) const override {
    if (this == &other) {
      return true;
    }
    auto casted = dynamic_cast<const IntegerMatcher*>(&other);
    return casted != nullptr;
  }

  std::string ToString() const override { return "integer"; }
};

std::shared_ptr<TypeMatcher> Integer() { return std::make_shared<IntegerMatcher>(); }

class PrimitiveMatcher : public TypeMatcher {
 public:
  PrimitiveMatcher() {}

  bool Matches(const DataType& type) const override { return is_primitive(type.id()); }

  bool Equals(const TypeMatcher& other) const override {
    if (this == &other) {
      return true;
    }
    auto casted = dynamic_cast<const PrimitiveMatcher*>(&other);
    return casted != nullptr;
  }

  std::string ToString() const override { return "primitive"; }
};

std::shared_ptr<TypeMatcher> Primitive() { return std::make_shared<PrimitiveMatcher>(); }

class BinaryLikeMatcher : public TypeMatcher {
 public:
  BinaryLikeMatcher() {}

  bool Matches(const DataType& type) const override { return is_binary_like(type.id()); }

  bool Equals(const TypeMatcher& other) const override {
    if (this == &other) {
      return true;
    }
    auto casted = dynamic_cast<const BinaryLikeMatcher*>(&other);
    return casted != nullptr;
  }
  std::string ToString() const override { return "binary-like"; }
};

std::shared_ptr<TypeMatcher> BinaryLike() {
  return std::make_shared<BinaryLikeMatcher>();
}

class LargeBinaryLikeMatcher : public TypeMatcher {
 public:
  LargeBinaryLikeMatcher() {}

  bool Matches(const DataType& type) const override {
    return is_large_binary_like(type.id());
  }

  bool Equals(const TypeMatcher& other) const override {
    if (this == &other) {
      return true;
    }
    auto casted = dynamic_cast<const LargeBinaryLikeMatcher*>(&other);
    return casted != nullptr;
  }
  std::string ToString() const override { return "large-binary-like"; }
};

class FixedSizeBinaryLikeMatcher : public TypeMatcher {
 public:
  FixedSizeBinaryLikeMatcher() {}

  bool Matches(const DataType& type) const override {
    return is_fixed_size_binary(type.id());
  }

  bool Equals(const TypeMatcher& other) const override {
    if (this == &other) {
      return true;
    }
    auto casted = dynamic_cast<const FixedSizeBinaryLikeMatcher*>(&other);
    return casted != nullptr;
  }
  std::string ToString() const override { return "fixed-size-binary-like"; }
};

std::shared_ptr<TypeMatcher> LargeBinaryLike() {
  return std::make_shared<LargeBinaryLikeMatcher>();
}

std::shared_ptr<TypeMatcher> FixedSizeBinaryLike() {
  return std::make_shared<FixedSizeBinaryLikeMatcher>();
}

class RunEndIntegerMatcher : public TypeMatcher {
 public:
  ~RunEndIntegerMatcher() override = default;

  bool Matches(const DataType& type) const override { return is_run_end_type(type.id()); }

  bool Equals(const TypeMatcher& other) const override {
    auto casted = dynamic_cast<const RunEndIntegerMatcher*>(&other);
    return casted != nullptr;
  }

  std::string ToString() const override { return "run-end-integer"; }
};

std::shared_ptr<TypeMatcher> RunEndInteger() {
  return std::make_shared<RunEndIntegerMatcher>();
}

class RunEndEncodedMatcher : public TypeMatcher {
 public:
  RunEndEncodedMatcher(std::shared_ptr<TypeMatcher> run_end_type_matcher,
                       std::shared_ptr<TypeMatcher> value_type_matcher)
      : run_end_type_matcher{std::move(run_end_type_matcher)},
        value_type_matcher{std::move(value_type_matcher)} {}

  ~RunEndEncodedMatcher() override = default;

  bool Matches(const DataType& type) const override {
    if (type.id() == Type::RUN_END_ENCODED) {
      const auto& ree_type = dynamic_cast<const RunEndEncodedType&>(type);
      // This invariant is enforced in RunEndEncodedType's constructor
      DCHECK(is_run_end_type(ree_type.run_end_type()->id()));
      return run_end_type_matcher->Matches(*ree_type.run_end_type()) &&
             value_type_matcher->Matches(*ree_type.value_type());
    }
    return false;
  }

  bool Equals(const TypeMatcher& other) const override {
    if (this == &other) {
      return true;
    }
    const auto* casted = dynamic_cast<const RunEndEncodedMatcher*>(&other);
    return casted != nullptr && value_type_matcher->Equals(*casted->value_type_matcher) &&
           run_end_type_matcher->Equals(*casted->run_end_type_matcher);
  }

  std::string ToString() const override {
    return "run_end_encoded(" + run_end_type_matcher->ToString() + ", " +
           value_type_matcher->ToString() + ")";
  };

 private:
  std::shared_ptr<TypeMatcher> run_end_type_matcher;
  std::shared_ptr<TypeMatcher> value_type_matcher;
};

std::shared_ptr<TypeMatcher> RunEndEncoded(
    std::shared_ptr<TypeMatcher> value_type_matcher) {
  return std::make_shared<RunEndEncodedMatcher>(RunEndInteger(),
                                                std::move(value_type_matcher));
}

std::shared_ptr<TypeMatcher> RunEndEncoded(Type::type value_type_id) {
  return RunEndEncoded(SameTypeId(value_type_id));
}

std::shared_ptr<TypeMatcher> RunEndEncoded(
    std::shared_ptr<TypeMatcher> run_end_type_matcher,
    std::shared_ptr<TypeMatcher> value_type_matcher) {
  return std::make_shared<RunEndEncodedMatcher>(std::move(run_end_type_matcher),
                                                std::move(value_type_matcher));
}

}  // namespace match

// ----------------------------------------------------------------------
// InputType

size_t InputType::Hash() const {
  size_t result = kHashSeed;
  hash_combine(result, static_cast<int>(kind_));
  switch (kind_) {
    case InputType::EXACT_TYPE:
      hash_combine(result, type_->Hash());
      break;
    default:
      break;
  }
  return result;
}

std::string InputType::ToString() const {
  std::stringstream ss;
  switch (kind_) {
    case InputType::ANY_TYPE:
      ss << "any";
      break;
    case InputType::EXACT_TYPE:
      ss << type_->ToString();
      break;
    case InputType::USE_TYPE_MATCHER: {
      ss << type_matcher_->ToString();
    } break;
    default:
      DCHECK(false);
      break;
  }
  return ss.str();
}

bool InputType::Equals(const InputType& other) const {
  if (this == &other) {
    return true;
  }
  if (kind_ != other.kind_) {
    return false;
  }
  switch (kind_) {
    case InputType::ANY_TYPE:
      return true;
    case InputType::EXACT_TYPE:
      return type_->Equals(*other.type_);
    case InputType::USE_TYPE_MATCHER:
      return type_matcher_->Equals(*other.type_matcher_);
    default:
      return false;
  }
}

bool InputType::Matches(const DataType& type) const {
  switch (kind_) {
    case InputType::EXACT_TYPE:
      return type_->Equals(type);
    case InputType::USE_TYPE_MATCHER:
      return type_matcher_->Matches(type);
    default:
      // ANY_TYPE
      return true;
  }
}

bool InputType::Matches(const Datum& value) const {
  switch (value.kind()) {
    case Datum::ARRAY:
    case Datum::CHUNKED_ARRAY:
    case Datum::SCALAR:
      break;
    default:
      DCHECK(false);
      return false;
  }
  return Matches(*value.type());
}

const std::shared_ptr<DataType>& InputType::type() const {
  DCHECK_EQ(InputType::EXACT_TYPE, kind_);
  return type_;
}

const TypeMatcher& InputType::type_matcher() const {
  DCHECK_EQ(InputType::USE_TYPE_MATCHER, kind_);
  return *type_matcher_;
}

// ----------------------------------------------------------------------
// OutputType

Result<TypeHolder> OutputType::Resolve(KernelContext* ctx,
                                       const std::vector<TypeHolder>& types) const {
  if (kind_ == OutputType::FIXED) {
    return type_.get();
  } else {
    return resolver_(ctx, types);
  }
}

const std::shared_ptr<DataType>& OutputType::type() const {
  DCHECK_EQ(FIXED, kind_);
  return type_;
}

const OutputType::Resolver& OutputType::resolver() const {
  DCHECK_EQ(COMPUTED, kind_);
  return resolver_;
}

std::string OutputType::ToString() const {
  if (kind_ == OutputType::FIXED) {
    return type_->ToString();
  } else {
    return "computed";
  }
}

// ----------------------------------------------------------------------
// KernelSignature

KernelSignature::KernelSignature(std::vector<InputType> in_types, OutputType out_type,
                                 bool is_varargs)
    : in_types_(std::move(in_types)),
      out_type_(std::move(out_type)),
      is_varargs_(is_varargs),
      hash_code_(0) {
  DCHECK(!is_varargs || (is_varargs && (in_types_.size() >= 1)));
}

std::shared_ptr<KernelSignature> KernelSignature::Make(std::vector<InputType> in_types,
                                                       OutputType out_type,
                                                       bool is_varargs) {
  return std::make_shared<KernelSignature>(std::move(in_types), std::move(out_type),
                                           is_varargs);
}

bool KernelSignature::Equals(const KernelSignature& other) const {
  if (is_varargs_ != other.is_varargs_) {
    return false;
  }
  if (in_types_.size() != other.in_types_.size()) {
    return false;
  }
  for (size_t i = 0; i < in_types_.size(); ++i) {
    if (!in_types_[i].Equals(other.in_types_[i])) {
      return false;
    }
  }
  return true;
}

bool KernelSignature::MatchesInputs(const std::vector<TypeHolder>& types) const {
  if (is_varargs_) {
    for (size_t i = 0; i < types.size(); ++i) {
      if (!in_types_[std::min(i, in_types_.size() - 1)].Matches(*types[i])) {
        return false;
      }
    }
  } else {
    if (types.size() != in_types_.size()) {
      return false;
    }
    for (size_t i = 0; i < in_types_.size(); ++i) {
      if (!in_types_[i].Matches(*types[i])) {
        return false;
      }
    }
  }
  return true;
}

size_t KernelSignature::Hash() const {
  if (hash_code_ != 0) {
    return hash_code_;
  }
  size_t result = kHashSeed;
  for (const auto& in_type : in_types_) {
    hash_combine(result, in_type.Hash());
  }
  hash_code_ = result;
  return result;
}

std::string KernelSignature::ToString() const {
  std::stringstream ss;

  if (is_varargs_) {
    ss << "varargs[";
  } else {
    ss << "(";
  }
  for (size_t i = 0; i < in_types_.size(); ++i) {
    if (i > 0) {
      ss << ", ";
    }
    ss << in_types_[i].ToString();
  }
  if (is_varargs_) {
    ss << "*]";
  } else {
    ss << ")";
  }
  ss << " -> " << out_type_.ToString();
  return ss.str();
}

}  // namespace compute
}  // namespace arrow

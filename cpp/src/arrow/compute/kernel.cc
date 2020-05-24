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

inline void ZeroLastByte(Buffer* buffer) {
  *(buffer->mutable_data() + (buffer->size() - 1)) = 0;
}

Result<std::shared_ptr<Buffer>> KernelContext::Allocate(int64_t nbytes) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> result,
                        AllocateBuffer(nbytes, exec_ctx_->memory_pool()));
  result->ZeroPadding();
  return result;
}

Result<std::shared_ptr<Buffer>> KernelContext::AllocateBitmap(int64_t num_bits) {
  const int64_t nbytes = BitUtil::BytesForBits(num_bits);
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> result,
                        AllocateBuffer(nbytes, exec_ctx_->memory_pool()));
  // Some utility methods access the last byte before it might be
  // initialized this makes valgrind/asan unhappy, so we proactively
  // zero it.
  if (nbytes > 0) {
    ZeroLastByte(result.get());
    result->ZeroPadding();
  }
  return result;
}

void KernelContext::SetStatus(const Status& status) {
  if (ARROW_PREDICT_FALSE(!status_.ok())) {
    return;
  }
  status_ = status;
}

/// \brief Clear any error status
void KernelContext::ResetStatus() { status_ = Status::OK(); }

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

class TimestampUnitMatcher : public TypeMatcher {
 public:
  explicit TimestampUnitMatcher(TimeUnit::type accepted_unit)
      : accepted_unit_(accepted_unit) {}

  bool Matches(const DataType& type) const override {
    if (type.id() != Type::TIMESTAMP) {
      return false;
    }
    const auto& ts_type = checked_cast<const TimestampType&>(type);
    return ts_type.unit() == accepted_unit_;
  }

  bool Equals(const TypeMatcher& other) const override {
    if (this == &other) {
      return true;
    }
    auto casted = dynamic_cast<const TimestampUnitMatcher*>(&other);
    if (casted == nullptr) {
      return false;
    }
    return this->accepted_unit_ == casted->accepted_unit_;
  }

  std::string ToString() const override {
    std::stringstream ss;
    ss << "timestamp(" << ::arrow::internal::ToString(accepted_unit_) << ")";
    return ss.str();
  }

 private:
  TimeUnit::type accepted_unit_;
};

std::shared_ptr<TypeMatcher> TimestampUnit(TimeUnit::type unit) {
  return std::make_shared<TimestampUnitMatcher>(unit);
}

}  // namespace match

// ----------------------------------------------------------------------
// InputType

size_t InputType::Hash() const {
  size_t result = kHashSeed;
  hash_combine(result, static_cast<int>(shape_));
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
  switch (shape_) {
    case ValueDescr::ANY:
      ss << "any";
      break;
    case ValueDescr::ARRAY:
      ss << "array";
      break;
    case ValueDescr::SCALAR:
      ss << "scalar";
      break;
    default:
      DCHECK(false);
      break;
  }
  ss << "[";
  switch (kind_) {
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
  ss << "]";
  return ss.str();
}

bool InputType::Equals(const InputType& other) const {
  if (this == &other) {
    return true;
  }
  if (kind_ != other.kind_ || shape_ != other.shape_) {
    return false;
  }
  switch (kind_) {
    case InputType::EXACT_TYPE:
      return type_->Equals(*other.type_);
    case InputType::USE_TYPE_MATCHER:
      return type_matcher_->Equals(*other.type_matcher_);
    default:
      return false;
  }
}

bool InputType::Matches(const ValueDescr& descr) const {
  if (shape_ != ValueDescr::ANY && descr.shape != shape_) {
    return false;
  }
  switch (kind_) {
    case InputType::EXACT_TYPE:
      return type_->Equals(*descr.type);
    case InputType::USE_TYPE_MATCHER:
      return type_matcher_->Matches(*descr.type);
    default:
      // ANY_TYPE
      return true;
  }
}

bool InputType::Matches(const Datum& value) const { return Matches(value.descr()); }

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

OutputType::OutputType(ValueDescr descr) : OutputType(descr.type) {
  shape_ = descr.shape;
}

Result<ValueDescr> OutputType::Resolve(KernelContext* ctx,
                                       const std::vector<ValueDescr>& args) const {
  if (kind_ == OutputType::FIXED) {
    ValueDescr::Shape out_shape = shape_;
    if (out_shape == ValueDescr::ANY) {
      out_shape = GetBroadcastShape(args);
    }
    return ValueDescr(type_, out_shape);
  } else {
    return resolver_(ctx, args);
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
  // VarArgs sigs must have only a single input type to use for argument validation
  DCHECK(!is_varargs || (is_varargs && (in_types_.size() == 1)));
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

bool KernelSignature::MatchesInputs(const std::vector<ValueDescr>& args) const {
  if (is_varargs_) {
    for (const auto& arg : args) {
      if (!in_types_[0].Matches(arg)) {
        return false;
      }
    }
  } else {
    if (args.size() != in_types_.size()) {
      return false;
    }
    for (size_t i = 0; i < in_types_.size(); ++i) {
      if (!in_types_[i].Matches(args[i])) {
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
    ss << "varargs[" << in_types_[0].ToString() << "]";
  } else {
    ss << "(";
    for (size_t i = 0; i < in_types_.size(); ++i) {
      if (i > 0) {
        ss << ", ";
      }
      ss << in_types_[i].ToString();
    }
    ss << ")";
  }
  ss << " -> " << out_type_.ToString();
  return ss.str();
}

}  // namespace compute
}  // namespace arrow

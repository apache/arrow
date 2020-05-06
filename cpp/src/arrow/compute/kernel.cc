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
#include "arrow/util/hashing.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

namespace arrow {

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
  ZeroLastByte(result.get());
  result->ZeroPadding();
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
// InputType

size_t InputType::Hash() const {
  size_t result = kHashSeed;
  hash_combine(result, static_cast<int>(shape_));
  switch (kind_) {
    case InputType::EXACT_TYPE:
      hash_combine(result, type_->Hash());
      break;
    case InputType::SAME_TYPE_ID:
      hash_combine(result, static_cast<int>(type_id_));
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
    case InputType::SAME_TYPE_ID: {
      // Indicate that the parameters for the type are unspecified. TODO: don't
      // show this for types without parameters, like Type::INT32
      ss << internal::ToString(type_id_) << "*";
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
    case InputType::SAME_TYPE_ID:
      return type_id_ == other.type_id_;
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
    case InputType::SAME_TYPE_ID:
      return type_id_ == descr.type->id();
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

Type::type InputType::type_id() const {
  DCHECK_EQ(InputType::SAME_TYPE_ID, kind_);
  return type_id_;
}

// ----------------------------------------------------------------------
// OutputType

OutputType::Resolver ResolveAs(ValueDescr descr) {
  return [descr](const std::vector<ValueDescr>&) { return descr; };
}

OutputType::OutputType(ValueDescr descr) : resolver_(ResolveAs(descr)) {}

Result<ValueDescr> OutputType::Resolve(const std::vector<ValueDescr>& args) const {
  if (kind_ == OutputType::FIXED) {
    return ValueDescr(type_, GetBroadcastShape(args));
  } else {
    return resolver_(args);
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
  // Varargs sigs must have only a single input type to use for argument validation
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

int64_t KernelSignature::Hash() const {
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

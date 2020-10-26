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

#include "arrow/dataset/expression.h"

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::checked_cast;

namespace dataset {

bool Identical(const Expression2& l, const Expression2& r) { return l.impl_ == r.impl_; }

const Expression2::Call* CallNotNull(const Expression2& expr) {
  auto call = expr.call();
  DCHECK_NE(call, nullptr);
  return call;
}

inline void GetAllFieldRefs(const Expression2& expr,
                            std::unordered_set<FieldRef, FieldRef::Hash>* refs) {
  if (auto lit = expr.literal()) return;

  if (auto ref = expr.field_ref()) {
    refs->emplace(*ref);
    return;
  }

  for (const Expression2& arg : CallNotNull(expr)->arguments) {
    GetAllFieldRefs(arg, refs);
  }
}

inline std::vector<ValueDescr> GetDescriptors(const std::vector<Expression2>& exprs) {
  std::vector<ValueDescr> descrs(exprs.size());
  for (size_t i = 0; i < exprs.size(); ++i) {
    DCHECK(exprs[i].IsBound());
    descrs[i] = exprs[i].descr();
  }
  return descrs;
}

struct CallState : ExpressionState {
  std::vector<std::shared_ptr<ExpressionState>> argument_states;
  std::shared_ptr<compute::KernelState> kernel_state;
};

struct FieldPathGetDatumImpl {
  template <typename T, typename = decltype(FieldPath{}.Get(std::declval<const T&>()))>
  Result<Datum> operator()(const std::shared_ptr<T>& ptr) {
    return path_.Get(*ptr).template As<Datum>();
  }

  template <typename T>
  Result<Datum> operator()(const T&) {
    return Status::NotImplemented("FieldPath::Get() into Datum ", datum_.ToString());
  }

  const Datum& datum_;
  const FieldPath& path_;
};

inline Result<Datum> GetDatumField(const FieldRef& ref, const Datum& input) {
  Datum field;

  FieldPath path;
  if (auto type = input.type()) {
    ARROW_ASSIGN_OR_RAISE(path, ref.FindOneOrNone(*input.type()));
  } else if (input.kind() == Datum::RECORD_BATCH) {
    ARROW_ASSIGN_OR_RAISE(path, ref.FindOneOrNone(*input.record_batch()->schema()));
  } else if (input.kind() == Datum::TABLE) {
    ARROW_ASSIGN_OR_RAISE(path, ref.FindOneOrNone(*input.table()->schema()));
  }

  if (path) {
    ARROW_ASSIGN_OR_RAISE(field,
                          util::visit(FieldPathGetDatumImpl{input, path}, input.value));
  }

  if (field == Datum{}) {
    field = Datum(std::make_shared<NullScalar>());
  }

  return field;
}

}  // namespace dataset
}  // namespace arrow

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

#include "arrow/dataset/projector.h"

#include "arrow/status.h"
#include "arrow/type.h"

namespace arrow {
namespace dataset {

Status CheckProjectable(const Schema& from, const Schema& to) {
  for (const auto& to_field : to.fields()) {
    ARROW_ASSIGN_OR_RAISE(auto from_field, FieldRef(to_field->name()).GetOneOrNone(from));

    if (from_field == nullptr) {
      if (to_field->nullable()) continue;

      return Status::TypeError("field ", to_field->ToString(),
                               " is not nullable and does not exist in origin schema ",
                               from);
    }

    if (from_field->type()->id() == Type::NA) {
      // promotion from null to any type is supported
      if (to_field->nullable()) continue;

      return Status::TypeError("field ", to_field->ToString(),
                               " is not nullable but has type ", NullType(),
                               " in origin schema ", from);
    }

    if (!from_field->type()->Equals(to_field->type())) {
      return Status::TypeError("fields had matching names but differing types. From: ",
                               from_field->ToString(), " To: ", to_field->ToString());
    }

    if (from_field->nullable() && !to_field->nullable()) {
      return Status::TypeError("field ", to_field->ToString(),
                               " is not nullable but is not required in origin schema ",
                               from);
    }
  }

  return Status::OK();
}

}  // namespace dataset
}  // namespace arrow

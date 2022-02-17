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

#pragma once

#include <flatbuffers/flatbuffers.h>

#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/expression.h"
#include "arrow/compute/exec/options.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/util/visibility.h"

#include "generated/Plan_generated.h"

namespace arrow {

namespace flatbuf = org::apache::arrow::flatbuf;

namespace compute {

namespace ir = org::apache::arrow::computeir::flatbuf;

class ARROW_EXPORT CatalogSourceNodeOptions : public ExecNodeOptions {
 public:
  CatalogSourceNodeOptions(std::string name, std::shared_ptr<Schema> schema,
                           Expression filter = literal(true),
                           std::vector<FieldRef> projection = {})
      : name(std::move(name)),
        schema(std::move(schema)),
        filter(std::move(filter)),
        projection(std::move(projection)) {}

  std::string name;
  std::shared_ptr<Schema> schema;
  Expression filter;
  std::vector<FieldRef> projection;
};

ARROW_EXPORT
Result<Datum> Convert(const ir::Literal& lit);

ARROW_EXPORT
Result<Expression> Convert(const ir::Expression& lit);

ARROW_EXPORT
Result<Declaration> Convert(const ir::Relation& rel);

template <typename Ir>
auto ConvertRoot(const Buffer& buf) -> decltype(Convert(std::declval<Ir>())) {
  return Convert(*flatbuffers::GetRoot<Ir>(buf.data()));
}

}  // namespace compute
}  // namespace arrow

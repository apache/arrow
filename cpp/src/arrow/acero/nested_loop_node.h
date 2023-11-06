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

#include <vector>

#include "arrow/acero/options.h"
#include "arrow/acero/schema_util.h"
#include "arrow/result.h"
#include "arrow/status.h"

namespace arrow {

using compute::ExecContext;

namespace acero {

class ARROW_ACERO_EXPORT NestedLoopJoinSchema {
 public:
  Status Init(JoinType join_type, const Schema& left_schema, const Schema& right_schema,
              const Expression& filter, const std::string& left_field_name_suffix,
              const std::string& right_field_name_suffix);

  Status Init(JoinType join_type, const Schema& left_schema,
              const std::vector<FieldRef>& left_output, const Schema& right_schema,
              const std::vector<FieldRef>& right_output, const Expression& filter,
              const std::string& left_field_name_suffix,
              const std::string& right_field_name_suffix);

  static Status ValidateSchemas(JoinType join_type, const Schema& left_schema,
                                const std::vector<FieldRef>& left_output,
                                const Schema& right_schema,
                                const std::vector<FieldRef>& right_output);

  std::shared_ptr<Schema> MakeOutputSchema(const std::string& left_field_name_suffix,
                                           const std::string& right_field_name_suffix);

  SchemaProjectionMaps<NestedLoopJoinProjection> proj_maps[2];

  Result<Expression> BindFilter(Expression filter, const Schema& left_schema,
                                const Schema& right_schema, ExecContext* exec_context);

 private:
  Status CollectFilterColumns(std::vector<FieldRef>& left_filter,
                              std::vector<FieldRef>& right_filter,
                              const Expression& filter, const Schema& left_schema,
                              const Schema& right_schema);
};

}  // namespace acero
}  // namespace arrow
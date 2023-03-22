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

#include "./arrow_types.h"

#include <arrow/compute/api_scalar.h>
#include <arrow/compute/expression.h>

namespace compute = ::arrow::compute;

std::shared_ptr<compute::FunctionOptions> make_compute_options(std::string func_name,
                                                               cpp11::list options);

// [[arrow::export]]
bool compute___expr__equals(const std::shared_ptr<compute::Expression>& lhs,
                            const std::shared_ptr<compute::Expression>& rhs) {
  return lhs->Equals(*rhs);
}
// [[arrow::export]]
std::shared_ptr<compute::Expression> compute___expr__call(std::string func_name,
                                                          cpp11::list argument_list,
                                                          cpp11::list options) {
  std::vector<compute::Expression> arguments;
  for (SEXP argument : argument_list) {
    auto argument_ptr = cpp11::as_cpp<std::shared_ptr<compute::Expression>>(argument);
    arguments.push_back(*argument_ptr);
  }

  auto options_ptr = make_compute_options(func_name, options);

  return std::make_shared<compute::Expression>(
      compute::call(std::move(func_name), std::move(arguments), std::move(options_ptr)));
}

// [[arrow::export]]
bool compute___expr__is_field_ref(const std::shared_ptr<compute::Expression>& x) {
  return x->field_ref() != nullptr;
}

// [[arrow::export]]
std::string compute___expr__get_field_ref_name(
    const std::shared_ptr<compute::Expression>& x) {
  if (auto field_ref = x->field_ref()) {
    // Exclude nested field refs because we only use this to determine if we have simple
    // field refs
    if (!field_ref->IsNested()) {
      return *field_ref->name();
    }
  }
  return "";
}

// [[arrow::export]]
std::shared_ptr<compute::Expression> compute___expr__field_ref(std::string name) {
  return std::make_shared<compute::Expression>(compute::field_ref(std::move(name)));
}

// [[arrow::export]]
std::shared_ptr<compute::Expression> compute___expr__nested_field_ref(
    const std::shared_ptr<compute::Expression>& x, std::string name) {
  if (auto field_ref = x->field_ref()) {
    std::vector<arrow::FieldRef> ref_vec;
    if (field_ref->IsNested()) {
      ref_vec = *field_ref->nested_refs();
    } else {
      // There's just one
      ref_vec.push_back(*field_ref);
    }
    // Add the new ref
    ref_vec.push_back(arrow::FieldRef(std::move(name)));
    return std::make_shared<compute::Expression>(
        compute::field_ref(arrow::FieldRef{std::move(ref_vec)}));
  } else {
    cpp11::stop("'x' must be a FieldRef Expression");
  }
}

// [[arrow::export]]
std::shared_ptr<compute::Expression> compute___expr__scalar(
    const std::shared_ptr<arrow::Scalar>& x) {
  return std::make_shared<compute::Expression>(compute::literal(std::move(x)));
}

// [[arrow::export]]
std::string compute___expr__ToString(const std::shared_ptr<compute::Expression>& x) {
  return x->ToString();
}

// [[arrow::export]]
std::shared_ptr<arrow::DataType> compute___expr__type(
    const std::shared_ptr<compute::Expression>& x,
    const std::shared_ptr<arrow::Schema>& schema) {
  auto bound = ValueOrStop(x->Bind(*schema));
  return bound.type()->GetSharedPtr();
}

// [[arrow::export]]
arrow::Type::type compute___expr__type_id(const std::shared_ptr<compute::Expression>& x,
                                          const std::shared_ptr<arrow::Schema>& schema) {
  auto bound = ValueOrStop(x->Bind(*schema));
  return bound.type()->id();
}

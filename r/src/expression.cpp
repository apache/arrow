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

#if defined(ARROW_R_WITH_ARROW)

#include <arrow/compute/api_scalar.h>
#include <arrow/dataset/api.h>
namespace ds = ::arrow::dataset;

std::shared_ptr<arrow::compute::FunctionOptions> make_compute_options(
    std::string func_name, cpp11::list options);

// [[arrow::export]]
std::shared_ptr<ds::Expression> dataset___expr__call(std::string func_name,
                                                     cpp11::list argument_list,
                                                     cpp11::list options) {
  std::vector<ds::Expression> arguments;
  for (SEXP argument : argument_list) {
    auto argument_ptr = cpp11::as_cpp<std::shared_ptr<ds::Expression>>(argument);
    arguments.push_back(*argument_ptr);
  }

  auto options_ptr = make_compute_options(func_name, options);

  return std::make_shared<ds::Expression>(
      ds::call(std::move(func_name), std::move(arguments), std::move(options_ptr)));
}

// [[arrow::export]]
std::shared_ptr<ds::Expression> dataset___expr__field_ref(std::string name) {
  return std::make_shared<ds::Expression>(ds::field_ref(std::move(name)));
}

// [[arrow::export]]
std::shared_ptr<ds::Expression> dataset___expr__scalar(
    const std::shared_ptr<arrow::Scalar>& x) {
  return std::make_shared<ds::Expression>(ds::literal(std::move(x)));
}

// [[arrow::export]]
std::string dataset___expr__ToString(const std::shared_ptr<ds::Expression>& x) {
  return x->ToString();
}

#endif

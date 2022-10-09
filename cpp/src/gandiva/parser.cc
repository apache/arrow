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

#include "gandiva/parser.h"

#include "arrow/status.h"

#include "gandiva/gandiva_aliases.h"

#include "gandiva/grammar.hh"
#include "gandiva/type_inference.h"

namespace gandiva {

Status Parser::Parse(const std::string& content, NodePtr* ptr) {
  location_.initialize(&content);
  scan_begin(content);
  node_ptr_ = ptr;
  gandiva::grammar grammar(*this);
  int res = grammar();
  scan_end();
  if (res != 0) {
    return Status::ParseError(error_message_);
  }

  auto status = InferTypes(*node_ptr_, schema_, node_ptr_);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}
}  // namespace gandiva

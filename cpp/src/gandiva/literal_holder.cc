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

#include <sstream>

#include "gandiva/literal_holder.h"

namespace gandiva {

namespace {

template <typename OStream>
struct LiteralToStream {
  OStream& ostream_;

  template <typename Value>
  void operator()(const Value& v) {
    ostream_ << v;
  }
};

}  // namespace

std::string ToString(const LiteralHolder& holder) {
  std::stringstream ss;
  LiteralToStream<std::stringstream> visitor{ss};
  ::arrow::util::visit(visitor, holder);
  return ss.str();
}

}  // namespace gandiva

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

#include "arrow/timestamp_converter.h"

#include "arrow/util/make_unique.h"
#include "arrow/util/parsing.h"

namespace arrow {
bool Iso8601Converter::operator()(const std::shared_ptr<DataType>& type, const char* s,
                                  size_t length, value_type* out) {
  // TODO: this should be refactored if proposed API would be accepted
  return internal::StringConverter<TimestampType>(type)(s, length, out);
}

std::unique_ptr<TimestampConverter> Iso8601Converter::Make() {
  return internal::make_unique<Iso8601Converter>();
}
}  // namespace arrow

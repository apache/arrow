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

#include "arrow/util/test_common.h"

namespace arrow {

TestInt::TestInt() : value(-999) {}
TestInt::TestInt(int i) : value(i) {}  // NOLINT runtime/explicit
bool TestInt::operator==(const TestInt& other) const { return value == other.value; }

std::ostream& operator<<(std::ostream& os, const TestInt& v) {
  os << "{" << v.value << "}";
  return os;
}

TestStr::TestStr() : value("") {}
TestStr::TestStr(const std::string& s) : value(s) {}  // NOLINT runtime/explicit
TestStr::TestStr(const char* s) : value(s) {}         // NOLINT runtime/explicit
TestStr::TestStr(const TestInt& test_int) {
  if (IsIterationEnd(test_int)) {
    value = "";
  } else {
    value = std::to_string(test_int.value);
  }
}

bool TestStr::operator==(const TestStr& other) const { return value == other.value; }

std::ostream& operator<<(std::ostream& os, const TestStr& v) {
  os << "{\"" << v.value << "\"}";
  return os;
}

std::vector<TestInt> RangeVector(unsigned int max, unsigned int step) {
  auto count = max / step;
  std::vector<TestInt> range(count);
  for (unsigned int i = 0; i < count; i++) {
    range[i] = i * step;
  }
  return range;
}

Transformer<TestInt, TestStr> MakeFilter(std::function<bool(TestInt&)> filter) {
  return [filter](TestInt next) -> Result<TransformFlow<TestStr>> {
    if (filter(next)) {
      return TransformYield(TestStr(next));
    } else {
      return TransformSkip();
    }
  };
}

}  // namespace arrow

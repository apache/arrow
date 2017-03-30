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

#include "gtest/gtest.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/table.h"
#include "arrow/test-util.h"
#include "arrow/type.h"

#include "arrow/python/common.h"
#include "arrow/python/pandas_convert.h"

namespace arrow {
namespace py {

TEST(PyBuffer, InvalidInputObject) {
  PyBuffer buffer(Py_None);
}

TEST(PandasConversionTest, TestObjectBlockWriteFails) {
  StringBuilder builder(default_memory_pool());
  const char value[] = {'\xf1', '\0'};

  for (int i = 0; i < 1000; ++i) {
    builder.Append(value, static_cast<int32_t>(strlen(value)));
  }

  std::shared_ptr<Array> arr;
  ASSERT_OK(builder.Finish(&arr));

  auto f1 = field("f1", utf8());
  auto f2 = field("f2", utf8());
  auto f3 = field("f3", utf8());
  std::vector<std::shared_ptr<Field>> fields = {f1, f2, f3};
  std::vector<std::shared_ptr<Column>> cols = {std::make_shared<Column>(f1, arr),
      std::make_shared<Column>(f2, arr), std::make_shared<Column>(f3, arr)};

  auto schema = std::make_shared<Schema>(fields);
  auto table = std::make_shared<Table>(schema, cols);

  PyObject* out;
  Py_BEGIN_ALLOW_THREADS;
  ASSERT_RAISES(UnknownError, ConvertTableToPandas(table, 2, &out));
  Py_END_ALLOW_THREADS;
}

}  // namespace py
}  // namespace arrow

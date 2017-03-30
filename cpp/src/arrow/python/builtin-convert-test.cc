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

#include <memory>

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/python/builtin_convert.h"
#include "arrow/table.h"
#include "arrow/test-util.h"

namespace arrow {
namespace py {

TEST(BuiltinConversionTest, TestMixedTypeFails) {
  PyAcquireGIL lock;
  MemoryPool* pool = default_memory_pool();
  std::shared_ptr<Array> arr;

  PyObject* list = PyList_New(3);
  ASSERT_NE(list, nullptr);

  PyObject* str = PyUnicode_FromString("abc");
  ASSERT_NE(str, nullptr);

  PyObject* integer = PyLong_FromLong(1234L);
  ASSERT_NE(integer, nullptr);

  PyObject* doub = PyFloat_FromDouble(123.0234);
  ASSERT_NE(doub, nullptr);

  // This steals a reference to each object, so we don't need to decref them later
  // just the list
  ASSERT_EQ(PyList_SetItem(list, 0, str), 0);
  ASSERT_EQ(PyList_SetItem(list, 1, integer), 0);
  ASSERT_EQ(PyList_SetItem(list, 2, doub), 0);

  ASSERT_RAISES(UnknownError, ConvertPySequence(list, pool, &arr));

  Py_DECREF(list);
}

}  // namespace py
}  // namespace arrow

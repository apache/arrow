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

// Functions for converting between pandas's NumPy-based data representation
// and Arrow data structures

#pragma once

#include "arrow/chunked_array.h"
#include "arrow/python/common.h"
#include "arrow/python/platform.h"
#include "arrow/util/hashing.h"

namespace arrow {

class Array;
class Scalar;

namespace py {

struct ArrowToPythonObjectOptions {
  MemoryPool* pool = default_memory_pool();
  bool deduplicate_objects = false;
};

class ARROW_PYTHON_EXPORT ArrowToPython {
 public:
  // \brief Converts the given Array to a PyList object. Returns NULL if there
  // is an error converting the Array. The list elements are the same ones
  // generated via ToLogical()
  //
  // N.B. This has limited type support.  ARROW-12976 tracks extending the implementation.
  Result<PyObject*> ToPyList(const Array& array);

  // Populates out_objects with the result of converting the array values
  // to python objects. The same logic as ToLogical().
  //
  // N.B. Not all types are supported. ARROW-12976 tracks extending the implementation.
  Status ToNumpyObjectArray(const ArrowToPythonObjectOptions& options,
                            const ChunkedArray& array, PyObject** out_objects);

  // \brief Converts the given Scalar to a python object that best corresponds
  // with the Scalar's type.
  //
  // For example timestamp[ms] is translated into datetime.datetime.
  //
  // N.B. This has limited type support.  ARROW-12976 tracks extending the implementation.
  Result<PyObject*> ToLogical(const Scalar& scalar);

  // \brief Converts the given Scalar the type that is closed to its arrow
  // representation.
  //
  // For instance timestamp would be translated to a integer representing an
  // offset from the unix epoch.
  //
  // Returns nullptr on error.
  //
  // GIL must be health when calling this method.
  // N.B. This has limited type support.  ARROW-12976 tracks full implementation.
  Result<PyObject*> ToPrimitive(const Scalar& scalar);

 private:
  Status Init();
};

namespace internal {
// TODO(ARROW-12976):  See if we can refactor Pandas ObjectWriter logic
// to the .cc file and move this there as well if we can.

// Generic Array -> PyObject** converter that handles object deduplication, if
// requested
template <typename ArrayType, typename WriteValue, typename Assigner>
inline Status WriteArrayObjects(const ArrayType& arr, WriteValue&& write_func,
                                Assigner out_values) {
  // TODO(ARROW-12976): Use visitor here?
  const bool has_nulls = arr.null_count() > 0;
  for (int64_t i = 0; i < arr.length(); ++i) {
    if (has_nulls && arr.IsNull(i)) {
      Py_INCREF(Py_None);
      *out_values = Py_None;
    } else {
      RETURN_NOT_OK(write_func(arr.GetView(i), out_values));
    }
    ++out_values;
  }
  return Status::OK();
}

template <typename T, typename Enable = void>
struct MemoizationTraits {
  using Scalar = typename T::c_type;
};

template <typename T>
struct MemoizationTraits<T, enable_if_has_string_view<T>> {
  // For binary, we memoize string_view as a scalar value to avoid having to
  // unnecessarily copy the memory into the memo table data structure
  using Scalar = util::string_view;
};

template <typename Type, typename WrapFunction>
inline Status ConvertAsPyObjects(const ArrowToPythonObjectOptions& options,
                                 const ChunkedArray& data, WrapFunction&& wrap_func,
                                 PyObject** out_values) {
  using ArrayType = typename TypeTraits<Type>::ArrayType;
  using Scalar = typename MemoizationTraits<Type>::Scalar;

  ::arrow::internal::ScalarMemoTable<Scalar> memo_table(options.pool);
  std::vector<PyObject*> unique_values;
  int32_t memo_size = 0;

  auto WrapMemoized = [&](const Scalar& value, PyObject** out_values) {
    int32_t memo_index;
    RETURN_NOT_OK(memo_table.GetOrInsert(value, &memo_index));
    if (memo_index == memo_size) {
      // New entry
      RETURN_NOT_OK(wrap_func(value, out_values));
      unique_values.push_back(*out_values);
      ++memo_size;
    } else {
      // Duplicate entry
      Py_INCREF(unique_values[memo_index]);
      *out_values = unique_values[memo_index];
    }
    return Status::OK();
  };

  auto WrapUnmemoized = [&](const Scalar& value, PyObject** out_values) {
    return wrap_func(value, out_values);
  };

  for (int c = 0; c < data.num_chunks(); c++) {
    const auto& arr = arrow::internal::checked_cast<const ArrayType&>(*data.chunk(c));
    if (options.deduplicate_objects) {
      RETURN_NOT_OK(WriteArrayObjects(arr, WrapMemoized, out_values));
    } else {
      RETURN_NOT_OK(WriteArrayObjects(arr, WrapUnmemoized, out_values));
    }
    out_values += arr.length();
  }
  return Status::OK();
}

}  // namespace internal

}  // namespace py
}  // namespace arrow

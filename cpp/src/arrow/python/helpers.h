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

#ifndef PYARROW_HELPERS_H
#define PYARROW_HELPERS_H

#include "arrow/python/platform.h"

#include <memory>
#include <string>
#include <utility>

#include <numpy/halffloat.h>

#include "arrow/type.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Decimal128;

namespace py {

class OwnedRef;

// \brief Get an arrow DataType instance from Arrow's Type::type enum
// \param[in] type One of the values of Arrow's Type::type enum
// \return A shared pointer to DataType
ARROW_EXPORT std::shared_ptr<DataType> GetPrimitiveType(Type::type type);

// \brief Construct a np.float16 object from a npy_half value.
ARROW_EXPORT PyObject* PyHalf_FromHalf(npy_half value);

// \brief Convert a Python object to a npy_half value.
ARROW_EXPORT Status PyFloat_AsHalf(PyObject* obj, npy_half* out);

namespace internal {

// \brief Import a Python module
// \param[in] module_name The name of the module
// \param[out] ref The OwnedRef containing the module PyObject*
Status ImportModule(const std::string& module_name, OwnedRef* ref);

// \brief Import an object from a Python module
// \param[in] module A Python module
// \param[in] name The name of the object to import
// \param[out] ref The OwnedRef containing the \c name attribute of the Python module \c
// module
Status ImportFromModule(const OwnedRef& module, const std::string& name, OwnedRef* ref);

// \brief Import
Status ImportDecimalType(OwnedRef* decimal_type);

// \brief Convert a Python Decimal object to a C++ string
// \param[in] python_decimal A Python decimal.Decimal instance
// \param[out] The string representation of the Python Decimal instance
// \return The status of the operation
Status PythonDecimalToString(PyObject* python_decimal, std::string* out);

// \brief Convert a C++ std::string to a Python Decimal instance
// \param[in] decimal_constructor The decimal type object
// \param[in] decimal_string A decimal string
// \return An instance of decimal.Decimal
PyObject* DecimalFromString(PyObject* decimal_constructor,
                            const std::string& decimal_string);

// \brief Convert a Python decimal to an Arrow Decimal128 object
// \param[in] python_decimal A Python decimal.Decimal instance
// \param[in] arrow_type An instance of arrow::DecimalType
// \param[out] out A pointer to a Decimal128
// \return The status of the operation
Status DecimalFromPythonDecimal(PyObject* python_decimal, const DecimalType& arrow_type,
                                Decimal128* out);

// \brief Check whether obj is an integer, independent of Python versions.
bool IsPyInteger(PyObject* obj);

// \brief Use pandas missing value semantics to check if a value is null
bool PandasObjectIsNull(PyObject* obj);

// \brief Check whether obj is nan
bool PyFloat_IsNaN(PyObject* obj);

// \brief Check whether obj is an instance of Decimal
bool PyDecimal_Check(PyObject* obj);

// \brief Check whether obj is nan. This function will abort the program if the argument
// is not a Decimal instance
bool PyDecimal_ISNAN(PyObject* obj);

// \brief Convert a Python integer into an unsigned 64-bit integer
// \param[in] obj A Python integer
// \param[out] out A pointer to a C uint64_t to hold the result of the conversion
// \return The status of the operation
Status UInt64FromPythonInt(PyObject* obj, uint64_t* out);

// \brief Helper class to track and update the precision and scale of a decimal
class DecimalMetadata {
 public:
  DecimalMetadata();
  DecimalMetadata(int32_t precision, int32_t scale);

  // \brief Adjust the precision and scale of a decimal type given a new precision and a
  // new scale \param[in] suggested_precision A candidate precision \param[in]
  // suggested_scale A candidate scale \return The status of the operation
  Status Update(int32_t suggested_precision, int32_t suggested_scale);

  // \brief A convenient interface for updating the precision and scale based on a Python
  // Decimal object \param object A Python Decimal object \return The status of the
  // operation
  Status Update(PyObject* object);

  int32_t precision() const { return precision_; }
  int32_t scale() const { return scale_; }

 private:
  int32_t precision_;
  int32_t scale_;
};

}  // namespace internal
}  // namespace py
}  // namespace arrow

#endif  // PYARROW_HELPERS_H

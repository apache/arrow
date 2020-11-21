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

//! This library demonstrates a minimal usage of Rust's C data interface to pass
//! arrays from and to Python.

use std::error;
use std::fmt;
use std::sync::Arc;

use pyo3::exceptions::PyOSError;
use pyo3::wrap_pyfunction;
use pyo3::{libc::uintptr_t, prelude::*};

use arrow::array::{make_array_from_raw, ArrayRef, Int64Array};
use arrow::compute::kernels;
use arrow::error::ArrowError;
use arrow::ffi;

/// an error that bridges ArrowError with a Python error
#[derive(Debug)]
enum PyO3ArrowError {
    ArrowError(ArrowError),
}

impl fmt::Display for PyO3ArrowError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            PyO3ArrowError::ArrowError(ref e) => e.fmt(f),
        }
    }
}

impl error::Error for PyO3ArrowError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            // The cause is the underlying implementation error type. Is implicitly
            // cast to the trait object `&error::Error`. This works because the
            // underlying type already implements the `Error` trait.
            PyO3ArrowError::ArrowError(ref e) => Some(e),
        }
    }
}

impl From<ArrowError> for PyO3ArrowError {
    fn from(err: ArrowError) -> PyO3ArrowError {
        PyO3ArrowError::ArrowError(err)
    }
}

impl From<PyO3ArrowError> for PyErr {
    fn from(err: PyO3ArrowError) -> PyErr {
        PyOSError::new_err(err.to_string())
    }
}

fn to_rust(ob: PyObject, py: Python) -> PyResult<ArrayRef> {
    // prepare a pointer to receive the Array struct
    let (array_pointer, schema_pointer) =
        ffi::ArrowArray::into_raw(unsafe { ffi::ArrowArray::empty() });

    // make the conversion through PyArrow's private API
    // this changes the pointer's memory and is thus unsafe. In particular, `_export_to_c` can go out of bounds
    ob.call_method1(
        py,
        "_export_to_c",
        (array_pointer as uintptr_t, schema_pointer as uintptr_t),
    )?;

    let array = unsafe { make_array_from_raw(array_pointer, schema_pointer) }
        .map_err(|e| PyO3ArrowError::from(e))?;
    Ok(array)
}

fn to_py(array: ArrayRef, py: Python) -> PyResult<PyObject> {
    let (array_pointer, schema_pointer) =
        array.to_raw().map_err(|e| PyO3ArrowError::from(e))?;

    let pa = py.import("pyarrow")?;

    let array = pa.getattr("Array")?.call_method1(
        "_import_from_c",
        (array_pointer as uintptr_t, schema_pointer as uintptr_t),
    )?;
    Ok(array.to_object(py))
}

/// Returns `array + array` of an int64 array.
#[pyfunction]
fn double(array: PyObject, py: Python) -> PyResult<PyObject> {
    // import
    let array = to_rust(array, py)?;

    // perform some operation
    let array =
        array
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or(PyO3ArrowError::ArrowError(ArrowError::ParseError(
                "Expects an int64".to_string(),
            )))?;
    let array =
        kernels::arithmetic::add(&array, &array).map_err(|e| PyO3ArrowError::from(e))?;
    let array = Arc::new(array);

    // export
    to_py(array, py)
}

/// calls a lambda function that receives and returns an array
/// whose result must be the array multiplied by two
#[pyfunction]
fn double_py(lambda: PyObject, py: Python) -> PyResult<bool> {
    // create
    let array = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]));
    let expected = Arc::new(Int64Array::from(vec![Some(2), None, Some(6)])) as ArrayRef;

    // to py
    let array = to_py(array, py)?;

    let array = lambda.call1(py, (array,))?;

    let array = to_rust(array, py)?;

    Ok(array == expected)
}

/// Returns the substring
#[pyfunction]
fn substring(array: PyObject, start: i64, py: Python) -> PyResult<PyObject> {
    // import
    let array = to_rust(array, py)?;

    // substring
    let array = kernels::substring::substring(array.as_ref(), start, &None)
        .map_err(|e| PyO3ArrowError::from(e))?;

    // export
    to_py(array, py)
}

#[pymodule]
fn arrow_pyarrow_integration_testing(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(double))?;
    m.add_wrapped(wrap_pyfunction!(double_py))?;
    m.add_wrapped(wrap_pyfunction!(substring))?;
    Ok(())
}

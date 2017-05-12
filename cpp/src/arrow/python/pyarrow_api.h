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

// DO NOT EDIT THIS FILE. Update from pyarrow/lib_api.h after pyarrow build

/* Generated by Cython 0.25.2 */

#ifndef __PYX_HAVE_API__pyarrow__lib
#define __PYX_HAVE_API__pyarrow__lib
#include "Python.h"

static PyObject *(*__pyx_api_f_7pyarrow_3lib_pyarrow_wrap_buffer)(std::shared_ptr< arrow::Buffer>  const &) = 0;
#define pyarrow_wrap_buffer __pyx_api_f_7pyarrow_3lib_pyarrow_wrap_buffer
static PyObject *(*__pyx_api_f_7pyarrow_3lib_pyarrow_wrap_data_type)(std::shared_ptr< arrow::DataType>  const &) = 0;
#define pyarrow_wrap_data_type __pyx_api_f_7pyarrow_3lib_pyarrow_wrap_data_type
static PyObject *(*__pyx_api_f_7pyarrow_3lib_pyarrow_wrap_field)(std::shared_ptr< arrow::Field>  const &) = 0;
#define pyarrow_wrap_field __pyx_api_f_7pyarrow_3lib_pyarrow_wrap_field
static PyObject *(*__pyx_api_f_7pyarrow_3lib_pyarrow_wrap_schema)(std::shared_ptr< arrow::Schema>  const &) = 0;
#define pyarrow_wrap_schema __pyx_api_f_7pyarrow_3lib_pyarrow_wrap_schema
static PyObject *(*__pyx_api_f_7pyarrow_3lib_pyarrow_wrap_array)(std::shared_ptr< arrow::Array>  const &) = 0;
#define pyarrow_wrap_array __pyx_api_f_7pyarrow_3lib_pyarrow_wrap_array
static PyObject *(*__pyx_api_f_7pyarrow_3lib_pyarrow_wrap_tensor)(std::shared_ptr< arrow::Tensor>  const &) = 0;
#define pyarrow_wrap_tensor __pyx_api_f_7pyarrow_3lib_pyarrow_wrap_tensor
static PyObject *(*__pyx_api_f_7pyarrow_3lib_pyarrow_wrap_column)(std::shared_ptr< arrow::Column>  const &) = 0;
#define pyarrow_wrap_column __pyx_api_f_7pyarrow_3lib_pyarrow_wrap_column
static PyObject *(*__pyx_api_f_7pyarrow_3lib_pyarrow_wrap_table)(std::shared_ptr< arrow::Table>  const &) = 0;
#define pyarrow_wrap_table __pyx_api_f_7pyarrow_3lib_pyarrow_wrap_table
static PyObject *(*__pyx_api_f_7pyarrow_3lib_pyarrow_wrap_batch)(std::shared_ptr< arrow::RecordBatch>  const &) = 0;
#define pyarrow_wrap_batch __pyx_api_f_7pyarrow_3lib_pyarrow_wrap_batch
#if !defined(__Pyx_PyIdentifier_FromString)
#if PY_MAJOR_VERSION < 3
  #define __Pyx_PyIdentifier_FromString(s) PyString_FromString(s)
#else
  #define __Pyx_PyIdentifier_FromString(s) PyUnicode_FromString(s)
#endif
#endif

#ifndef __PYX_HAVE_RT_ImportModule
#define __PYX_HAVE_RT_ImportModule
static PyObject *__Pyx_ImportModule(const char *name) {
    PyObject *py_name = 0;
    PyObject *py_module = 0;
    py_name = __Pyx_PyIdentifier_FromString(name);
    if (!py_name)
        goto bad;
    py_module = PyImport_Import(py_name);
    Py_DECREF(py_name);
    return py_module;
bad:
    Py_XDECREF(py_name);
    return 0;
}
#endif

#ifndef __PYX_HAVE_RT_ImportFunction
#define __PYX_HAVE_RT_ImportFunction
static int __Pyx_ImportFunction(PyObject *module, const char *funcname, void (**f)(void), const char *sig) {
    PyObject *d = 0;
    PyObject *cobj = 0;
    union {
        void (*fp)(void);
        void *p;
    } tmp;
    d = PyObject_GetAttrString(module, (char *)"__pyx_capi__");
    if (!d)
        goto bad;
    cobj = PyDict_GetItemString(d, funcname);
    if (!cobj) {
        PyErr_Format(PyExc_ImportError,
            "%.200s does not export expected C function %.200s",
                PyModule_GetName(module), funcname);
        goto bad;
    }
#if PY_VERSION_HEX >= 0x02070000
    if (!PyCapsule_IsValid(cobj, sig)) {
        PyErr_Format(PyExc_TypeError,
            "C function %.200s.%.200s has wrong signature (expected %.500s, got %.500s)",
             PyModule_GetName(module), funcname, sig, PyCapsule_GetName(cobj));
        goto bad;
    }
    tmp.p = PyCapsule_GetPointer(cobj, sig);
#else
    {const char *desc, *s1, *s2;
    desc = (const char *)PyCObject_GetDesc(cobj);
    if (!desc)
        goto bad;
    s1 = desc; s2 = sig;
    while (*s1 != '\0' && *s1 == *s2) { s1++; s2++; }
    if (*s1 != *s2) {
        PyErr_Format(PyExc_TypeError,
            "C function %.200s.%.200s has wrong signature (expected %.500s, got %.500s)",
             PyModule_GetName(module), funcname, sig, desc);
        goto bad;
    }
    tmp.p = PyCObject_AsVoidPtr(cobj);}
#endif
    *f = tmp.fp;
    if (!(*f))
        goto bad;
    Py_DECREF(d);
    return 0;
bad:
    Py_XDECREF(d);
    return -1;
}
#endif


static int import_pyarrow__lib(void) {
  PyObject *module = 0;
  module = __Pyx_ImportModule("pyarrow.lib");
  if (!module) goto bad;
  if (__Pyx_ImportFunction(module, "pyarrow_wrap_buffer", (void (**)(void))&__pyx_api_f_7pyarrow_3lib_pyarrow_wrap_buffer, "PyObject *(std::shared_ptr< arrow::Buffer>  const &)") < 0) goto bad;
  if (__Pyx_ImportFunction(module, "pyarrow_wrap_data_type", (void (**)(void))&__pyx_api_f_7pyarrow_3lib_pyarrow_wrap_data_type, "PyObject *(std::shared_ptr< arrow::DataType>  const &)") < 0) goto bad;
  if (__Pyx_ImportFunction(module, "pyarrow_wrap_field", (void (**)(void))&__pyx_api_f_7pyarrow_3lib_pyarrow_wrap_field, "PyObject *(std::shared_ptr< arrow::Field>  const &)") < 0) goto bad;
  if (__Pyx_ImportFunction(module, "pyarrow_wrap_schema", (void (**)(void))&__pyx_api_f_7pyarrow_3lib_pyarrow_wrap_schema, "PyObject *(std::shared_ptr< arrow::Schema>  const &)") < 0) goto bad;
  if (__Pyx_ImportFunction(module, "pyarrow_wrap_array", (void (**)(void))&__pyx_api_f_7pyarrow_3lib_pyarrow_wrap_array, "PyObject *(std::shared_ptr< arrow::Array>  const &)") < 0) goto bad;
  if (__Pyx_ImportFunction(module, "pyarrow_wrap_tensor", (void (**)(void))&__pyx_api_f_7pyarrow_3lib_pyarrow_wrap_tensor, "PyObject *(std::shared_ptr< arrow::Tensor>  const &)") < 0) goto bad;
  if (__Pyx_ImportFunction(module, "pyarrow_wrap_column", (void (**)(void))&__pyx_api_f_7pyarrow_3lib_pyarrow_wrap_column, "PyObject *(std::shared_ptr< arrow::Column>  const &)") < 0) goto bad;
  if (__Pyx_ImportFunction(module, "pyarrow_wrap_table", (void (**)(void))&__pyx_api_f_7pyarrow_3lib_pyarrow_wrap_table, "PyObject *(std::shared_ptr< arrow::Table>  const &)") < 0) goto bad;
  if (__Pyx_ImportFunction(module, "pyarrow_wrap_batch", (void (**)(void))&__pyx_api_f_7pyarrow_3lib_pyarrow_wrap_batch, "PyObject *(std::shared_ptr< arrow::RecordBatch>  const &)") < 0) goto bad;
  Py_DECREF(module); module = 0;
  return 0;
  bad:
  Py_XDECREF(module);
  return -1;
}

#endif /* !__PYX_HAVE_API__pyarrow__lib */

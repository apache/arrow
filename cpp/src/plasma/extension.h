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

#ifndef PLASMA_EXTENSION_H
#define PLASMA_EXTENSION_H

#undef _XOPEN_SOURCE
#undef _POSIX_C_SOURCE
#include <Python.h>

#include "bytesobject.h"  // NOLINT

#include "plasma/client.h"
#include "plasma/common.h"

static int PyObjectToPlasmaClient(PyObject* object, PlasmaClient** client) {
  if (PyCapsule_IsValid(object, "plasma")) {
    *client = reinterpret_cast<PlasmaClient*>(PyCapsule_GetPointer(object, "plasma"));
    return 1;
  } else {
    PyErr_SetString(PyExc_TypeError, "must be a 'plasma' capsule");
    return 0;
  }
}

int PyStringToUniqueID(PyObject* object, ObjectID* object_id) {
  if (PyBytes_Check(object)) {
    memcpy(object_id, PyBytes_AsString(object), sizeof(ObjectID));
    return 1;
  } else {
    PyErr_SetString(PyExc_TypeError, "must be a 20 character string");
    return 0;
  }
}

#endif  // PLASMA_EXTENSION_H

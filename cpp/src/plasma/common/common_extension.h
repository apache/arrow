#ifndef COMMON_EXTENSION_H
#define COMMON_EXTENSION_H

#include <Python.h>
#include "marshal.h"
#include "structmember.h"

#include "common.h"

extern PyObject *CommonError;

// clang-format off
typedef struct {
  PyObject_HEAD
  ObjectID object_id;
} PyObjectID;
// clang-format on

extern PyTypeObject PyObjectIDType;

/* Python module for pickling. */
extern PyObject *pickle_module;
extern PyObject *pickle_dumps;
extern PyObject *pickle_loads;

void init_pickle_module(void);

int PyStringToUniqueID(PyObject *object, ObjectID *object_id);

int PyObjectToUniqueID(PyObject *object, ObjectID *object_id);

PyObject *PyObjectID_make(ObjectID object_id);

PyObject *check_simple_value(PyObject *self, PyObject *args);

PyObject *compute_put_id(PyObject *self, PyObject *args);

#endif /* COMMON_EXTENSION_H */

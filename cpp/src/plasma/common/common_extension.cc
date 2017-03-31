#include <Python.h>
#include "bytesobject.h"
#include "node.h"

#include "common.h"
#include "common_extension.h"
#include "utarray.h"
#include "utstring.h"

PyObject *CommonError;

/* Initialize pickle module. */

PyObject *pickle_module = NULL;
PyObject *pickle_loads = NULL;
PyObject *pickle_dumps = NULL;
PyObject *pickle_protocol = NULL;

void init_pickle_module(void) {
#if PY_MAJOR_VERSION >= 3
  pickle_module = PyImport_ImportModule("pickle");
#else
  pickle_module = PyImport_ImportModuleNoBlock("cPickle");
#endif
  CHECK(pickle_module != NULL);
  CHECK(PyObject_HasAttrString(pickle_module, "loads"));
  CHECK(PyObject_HasAttrString(pickle_module, "dumps"));
  CHECK(PyObject_HasAttrString(pickle_module, "HIGHEST_PROTOCOL"));
  pickle_loads = PyUnicode_FromString("loads");
  pickle_dumps = PyUnicode_FromString("dumps");
  pickle_protocol = PyObject_GetAttrString(pickle_module, "HIGHEST_PROTOCOL");
  CHECK(pickle_protocol != NULL);
}

/* Define the PyObjectID class. */

int PyStringToUniqueID(PyObject *object, ObjectID *object_id) {
  if (PyBytes_Check(object)) {
    memcpy(&object_id->id[0], PyBytes_AsString(object), UNIQUE_ID_SIZE);
    return 1;
  } else {
    PyErr_SetString(PyExc_TypeError, "must be a 20 character string");
    return 0;
  }
}

int PyObjectToUniqueID(PyObject *object, ObjectID *objectid) {
  if (PyObject_IsInstance(object, (PyObject *) &PyObjectIDType)) {
    *objectid = ((PyObjectID *) object)->object_id;
    return 1;
  } else {
    PyErr_SetString(PyExc_TypeError, "must be an ObjectID");
    return 0;
  }
}

static int PyObjectID_init(PyObjectID *self, PyObject *args, PyObject *kwds) {
  const char *data;
  int size;
  if (!PyArg_ParseTuple(args, "s#", &data, &size)) {
    return -1;
  }
  if (size != sizeof(ObjectID)) {
    PyErr_SetString(CommonError,
                    "ObjectID: object id string needs to have length 20");
    return -1;
  }
  memcpy(&self->object_id.id[0], data, sizeof(self->object_id.id));
  return 0;
}

/* Create a PyObjectID from C. */
PyObject *PyObjectID_make(ObjectID object_id) {
  PyObjectID *result = PyObject_New(PyObjectID, &PyObjectIDType);
  result = (PyObjectID *) PyObject_Init((PyObject *) result, &PyObjectIDType);
  result->object_id = object_id;
  return (PyObject *) result;
}

static PyObject *PyObjectID_id(PyObject *self) {
  PyObjectID *s = (PyObjectID *) self;
  return PyBytes_FromStringAndSize((char *) &s->object_id.id[0],
                                   sizeof(s->object_id.id));
}

static PyObject *PyObjectID_hex(PyObject *self) {
  PyObjectID *s = (PyObjectID *) self;
  char hex_id[ID_STRING_SIZE];
  ObjectID_to_string(s->object_id, hex_id, ID_STRING_SIZE);
  PyObject *result = PyUnicode_FromString(hex_id);
  return result;
}

static PyObject *PyObjectID_richcompare(PyObjectID *self,
                                        PyObject *other,
                                        int op) {
  PyObject *result = NULL;
  if (Py_TYPE(self)->tp_richcompare != Py_TYPE(other)->tp_richcompare) {
    result = Py_NotImplemented;
  } else {
    PyObjectID *other_id = (PyObjectID *) other;
    switch (op) {
    case Py_LT:
      result = Py_NotImplemented;
      break;
    case Py_LE:
      result = Py_NotImplemented;
      break;
    case Py_EQ:
      result = ObjectID_equal(self->object_id, other_id->object_id) ? Py_True
                                                                    : Py_False;
      break;
    case Py_NE:
      result = !ObjectID_equal(self->object_id, other_id->object_id) ? Py_True
                                                                     : Py_False;
      break;
    case Py_GT:
      result = Py_NotImplemented;
      break;
    case Py_GE:
      result = Py_NotImplemented;
      break;
    }
  }
  Py_XINCREF(result);
  return result;
}

static long PyObjectID_hash(PyObjectID *self) {
  PyObject *tuple = PyTuple_New(UNIQUE_ID_SIZE);
  for (int i = 0; i < UNIQUE_ID_SIZE; ++i) {
    PyTuple_SetItem(tuple, i, PyLong_FromLong(self->object_id.id[i]));
  }
  long hash = PyObject_Hash(tuple);
  Py_XDECREF(tuple);
  return hash;
}

static PyObject *PyObjectID_repr(PyObjectID *self) {
  char hex_id[ID_STRING_SIZE];
  ObjectID_to_string(self->object_id, hex_id, ID_STRING_SIZE);
  UT_string *repr;
  utstring_new(repr);
  utstring_printf(repr, "ObjectID(%s)", hex_id);
  PyObject *result = PyUnicode_FromString(utstring_body(repr));
  utstring_free(repr);
  return result;
}

static PyObject *PyObjectID___reduce__(PyObjectID *self) {
  PyErr_SetString(CommonError, "ObjectID objects cannot be serialized.");
  return NULL;
}

static PyMethodDef PyObjectID_methods[] = {
    {"id", (PyCFunction) PyObjectID_id, METH_NOARGS,
     "Return the hash associated with this ObjectID"},
    {"hex", (PyCFunction) PyObjectID_hex, METH_NOARGS,
     "Return the object ID as a string in hex."},
    {"__reduce__", (PyCFunction) PyObjectID___reduce__, METH_NOARGS,
     "Say how to pickle this ObjectID. This raises an exception to prevent"
     "object IDs from being serialized."},
    {NULL} /* Sentinel */
};

static PyMemberDef PyObjectID_members[] = {
    {NULL} /* Sentinel */
};

PyTypeObject PyObjectIDType = {
    PyVarObject_HEAD_INIT(NULL, 0)        /* ob_size */
    "common.ObjectID",                    /* tp_name */
    sizeof(PyObjectID),                   /* tp_basicsize */
    0,                                    /* tp_itemsize */
    0,                                    /* tp_dealloc */
    0,                                    /* tp_print */
    0,                                    /* tp_getattr */
    0,                                    /* tp_setattr */
    0,                                    /* tp_compare */
    (reprfunc) PyObjectID_repr,           /* tp_repr */
    0,                                    /* tp_as_number */
    0,                                    /* tp_as_sequence */
    0,                                    /* tp_as_mapping */
    (hashfunc) PyObjectID_hash,           /* tp_hash */
    0,                                    /* tp_call */
    0,                                    /* tp_str */
    0,                                    /* tp_getattro */
    0,                                    /* tp_setattro */
    0,                                    /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,                   /* tp_flags */
    "ObjectID object",                    /* tp_doc */
    0,                                    /* tp_traverse */
    0,                                    /* tp_clear */
    (richcmpfunc) PyObjectID_richcompare, /* tp_richcompare */
    0,                                    /* tp_weaklistoffset */
    0,                                    /* tp_iter */
    0,                                    /* tp_iternext */
    PyObjectID_methods,                   /* tp_methods */
    PyObjectID_members,                   /* tp_members */
    0,                                    /* tp_getset */
    0,                                    /* tp_base */
    0,                                    /* tp_dict */
    0,                                    /* tp_descr_get */
    0,                                    /* tp_descr_set */
    0,                                    /* tp_dictoffset */
    (initproc) PyObjectID_init,           /* tp_init */
    0,                                    /* tp_alloc */
    PyType_GenericNew,                    /* tp_new */
};

/* Define the methods for the module. */

#define SIZE_LIMIT 100
#define NUM_ELEMENTS_LIMIT 1000

#if PY_MAJOR_VERSION >= 3
#define PyInt_Check PyLong_Check
#endif

/**
 * This method checks if a Python object is sufficiently simple that it can be
 * serialized and passed by value as an argument to a task (without being put in
 * the object store). The details of which objects are sufficiently simple are
 * defined by this method and are not particularly important. But for
 * performance reasons, it is better to place "small" objects in the task itself
 * and "large" objects in the object store.
 *
 * @param value The Python object in question.
 * @param num_elements_contained If this method returns 1, then the number of
 *        objects recursively contained within this object will be added to the
 *        value at this address. This is used to make sure that we do not
 *        serialize objects that are too large.
 * @return 0 if the object cannot be serialized in the task and 1 if it can.
 */
int is_simple_value(PyObject *value, int *num_elements_contained) {
  *num_elements_contained += 1;
  if (*num_elements_contained >= NUM_ELEMENTS_LIMIT) {
    return 0;
  }
  if (PyInt_Check(value) || PyLong_Check(value) || value == Py_False ||
      value == Py_True || PyFloat_Check(value) || value == Py_None) {
    return 1;
  }
  if (PyBytes_CheckExact(value)) {
    *num_elements_contained += PyBytes_Size(value);
    return (*num_elements_contained < NUM_ELEMENTS_LIMIT);
  }
  if (PyUnicode_CheckExact(value)) {
    *num_elements_contained += PyUnicode_GET_SIZE(value);
    return (*num_elements_contained < NUM_ELEMENTS_LIMIT);
  }
  if (PyList_CheckExact(value) && PyList_Size(value) < SIZE_LIMIT) {
    for (Py_ssize_t i = 0; i < PyList_Size(value); ++i) {
      if (!is_simple_value(PyList_GetItem(value, i), num_elements_contained)) {
        return 0;
      }
    }
    return (*num_elements_contained < NUM_ELEMENTS_LIMIT);
  }
  if (PyDict_CheckExact(value) && PyDict_Size(value) < SIZE_LIMIT) {
    PyObject *key, *val;
    Py_ssize_t pos = 0;
    while (PyDict_Next(value, &pos, &key, &val)) {
      if (!is_simple_value(key, num_elements_contained) ||
          !is_simple_value(val, num_elements_contained)) {
        return 0;
      }
    }
    return (*num_elements_contained < NUM_ELEMENTS_LIMIT);
  }
  if (PyTuple_CheckExact(value) && PyTuple_Size(value) < SIZE_LIMIT) {
    for (Py_ssize_t i = 0; i < PyTuple_Size(value); ++i) {
      if (!is_simple_value(PyTuple_GetItem(value, i), num_elements_contained)) {
        return 0;
      }
    }
    return (*num_elements_contained < NUM_ELEMENTS_LIMIT);
  }
  return 0;
}

PyObject *check_simple_value(PyObject *self, PyObject *args) {
  PyObject *value;
  if (!PyArg_ParseTuple(args, "O", &value)) {
    return NULL;
  }
  int num_elements_contained = 0;
  if (is_simple_value(value, &num_elements_contained)) {
    Py_RETURN_TRUE;
  }
  Py_RETURN_FALSE;
}

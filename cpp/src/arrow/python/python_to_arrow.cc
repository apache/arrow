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

#include "python_to_arrow.h"

#include <sstream>

#include "scalars.h"

constexpr int32_t kMaxRecursionDepth = 100;

extern "C" {
  PyObject* pyarrow_serialize_callback = NULL;
  PyObject* pyarrow_deserialize_callback = NULL;
}

namespace arrow {

Status CallCustomSerializationCallback(PyObject* elem, PyObject** serialized_object) {
  *serialized_object = NULL;
  if (!pyarrow_serialize_callback) {
    std::stringstream ss;
    ss << "data type of " << PyUnicode_AsUTF8(PyObject_Repr(elem))
       << " not recognized and custom serialization handler not registered";
    return Status::NotImplemented(ss.str());
  } else {
    PyObject* arglist = Py_BuildValue("(O)", elem);
    // The reference count of the result of the call to PyObject_CallObject
    // must be decremented. This is done in SerializeDict in this file.
    PyObject* result = PyObject_CallObject(pyarrow_serialize_callback, arglist);
    Py_XDECREF(arglist);
    if (!result) { return Status::NotImplemented("python error"); }
    *serialized_object = result;
  }
  return Status::OK();
}

Status append(PyObject* elem, SequenceBuilder& builder, std::vector<PyObject*>& sublists,
    std::vector<PyObject*>& subtuples, std::vector<PyObject*>& subdicts,
    std::vector<PyObject*>& tensors_out) {
  // The bool case must precede the int case (PyInt_Check passes for bools)
  if (PyBool_Check(elem)) {
    RETURN_NOT_OK(builder.AppendBool(elem == Py_True));
  } else if (PyFloat_Check(elem)) {
    RETURN_NOT_OK(builder.AppendDouble(PyFloat_AS_DOUBLE(elem)));
  } else if (PyLong_Check(elem)) {
    int overflow = 0;
    int64_t data = PyLong_AsLongLongAndOverflow(elem, &overflow);
    if (!overflow) {
      RETURN_NOT_OK(builder.AppendInt64(data));
    } else {
      // Attempt to serialize the object using the custom callback.
      PyObject* serialized_object;
      // The reference count of serialized_object is incremented in the function
      // CallCustomSerializationCallback (if the call is successful), and it will
      // be decremented in SerializeDict in this file.
      RETURN_NOT_OK(CallCustomSerializationCallback(elem, &serialized_object));
      RETURN_NOT_OK(builder.AppendDict(PyDict_Size(serialized_object)));
      subdicts.push_back(serialized_object);
    }
#if PY_MAJOR_VERSION < 3
  } else if (PyInt_Check(elem)) {
    RETURN_NOT_OK(builder.AppendInt64(static_cast<int64_t>(PyInt_AS_LONG(elem))));
#endif
  } else if (PyBytes_Check(elem)) {
    auto data = reinterpret_cast<uint8_t*>(PyBytes_AS_STRING(elem));
    auto size = PyBytes_GET_SIZE(elem);
    RETURN_NOT_OK(builder.AppendBytes(data, size));
  } else if (PyUnicode_Check(elem)) {
    Py_ssize_t size;
#if PY_MAJOR_VERSION >= 3
    char* data = PyUnicode_AsUTF8AndSize(elem, &size);
    Status s = builder.AppendString(data, size);
#else
    PyObject* str = PyUnicode_AsUTF8String(elem);
    char* data = PyString_AS_STRING(str);
    size = PyString_GET_SIZE(str);
    Status s = builder.AppendString(data, size);
    Py_XDECREF(str);
#endif
    RETURN_NOT_OK(s);
  } else if (PyList_Check(elem)) {
    RETURN_NOT_OK(builder.AppendList(PyList_Size(elem)));
    sublists.push_back(elem);
  } else if (PyDict_Check(elem)) {
    RETURN_NOT_OK(builder.AppendDict(PyDict_Size(elem)));
    subdicts.push_back(elem);
  } else if (PyTuple_CheckExact(elem)) {
    RETURN_NOT_OK(builder.AppendTuple(PyTuple_Size(elem)));
    subtuples.push_back(elem);
  } else if (PyArray_IsScalar(elem, Generic)) {
    RETURN_NOT_OK(AppendScalar(elem, builder));
  } else if (PyArray_Check(elem)) {
    RETURN_NOT_OK(SerializeArray((PyArrayObject*)elem, builder, subdicts, tensors_out));
  } else if (elem == Py_None) {
    RETURN_NOT_OK(builder.AppendNone());
  } else {
    // Attempt to serialize the object using the custom callback.
    PyObject* serialized_object;
    // The reference count of serialized_object is incremented in the function
    // CallCustomSerializationCallback (if the call is successful), and it will
    // be decremented in SerializeDict in this file.
    RETURN_NOT_OK(CallCustomSerializationCallback(elem, &serialized_object));
    RETURN_NOT_OK(builder.AppendDict(PyDict_Size(serialized_object)));
    subdicts.push_back(serialized_object);
  }
  return Status::OK();
}

Status SerializeArray(PyArrayObject* array, SequenceBuilder& builder,
    std::vector<PyObject*>& subdicts, std::vector<PyObject*>& tensors_out) {
  int dtype = PyArray_TYPE(array);
  switch (dtype) {
    case NPY_BOOL:
    case NPY_UINT8:
    case NPY_INT8:
    case NPY_UINT16:
    case NPY_INT16:
    case NPY_UINT32:
    case NPY_INT32:
    case NPY_UINT64:
    case NPY_INT64:
    case NPY_FLOAT:
    case NPY_DOUBLE: {
      RETURN_NOT_OK(builder.AppendTensor(tensors_out.size()));
      tensors_out.push_back(reinterpret_cast<PyObject*>(array));
    } break;
    default:
      if (!pyarrow_serialize_callback) {
        std::stringstream stream;
        stream << "numpy data type not recognized: " << dtype;
        return Status::NotImplemented(stream.str());
      } else {
        PyObject* arglist = Py_BuildValue("(O)", array);
        // The reference count of the result of the call to PyObject_CallObject
        // must be decremented. This is done in SerializeDict in python.cc.
        PyObject* result = PyObject_CallObject(pyarrow_serialize_callback, arglist);
        Py_XDECREF(arglist);
        if (!result) { return Status::NotImplemented("python error"); }
        builder.AppendDict(PyDict_Size(result));
        subdicts.push_back(result);
      }
  }
  return Status::OK();
}

Status SerializeSequences(std::vector<PyObject*> sequences, int32_t recursion_depth,
    std::shared_ptr<Array>* out, std::vector<PyObject*>& tensors_out) {
  DCHECK(out);
  if (recursion_depth >= kMaxRecursionDepth) {
    return Status::NotImplemented(
        "This object exceeds the maximum recursion depth. It may contain itself "
        "recursively.");
  }
  SequenceBuilder builder(nullptr);
  std::vector<PyObject *> sublists, subtuples, subdicts;
  for (const auto& sequence : sequences) {
    PyObject* item;
    PyObject* iterator = PyObject_GetIter(sequence);
    while ((item = PyIter_Next(iterator))) {
      Status s = append(item, builder, sublists, subtuples, subdicts, tensors_out);
      Py_DECREF(item);
      // if an error occurs, we need to decrement the reference counts before returning
      if (!s.ok()) {
        Py_DECREF(iterator);
        return s;
      }
    }
    Py_DECREF(iterator);
  }
  std::shared_ptr<Array> list;
  if (sublists.size() > 0) {
    RETURN_NOT_OK(SerializeSequences(sublists, recursion_depth + 1, &list, tensors_out));
  }
  std::shared_ptr<Array> tuple;
  if (subtuples.size() > 0) {
    RETURN_NOT_OK(
        SerializeSequences(subtuples, recursion_depth + 1, &tuple, tensors_out));
  }
  std::shared_ptr<Array> dict;
  if (subdicts.size() > 0) {
    RETURN_NOT_OK(SerializeDict(subdicts, recursion_depth + 1, &dict, tensors_out));
  }
  return builder.Finish(list, tuple, dict, out);
}

Status SerializeDict(std::vector<PyObject*> dicts, int32_t recursion_depth,
    std::shared_ptr<Array>* out, std::vector<PyObject*>& tensors_out) {
  DictBuilder result;
  if (recursion_depth >= kMaxRecursionDepth) {
    return Status::NotImplemented(
        "This object exceeds the maximum recursion depth. It may contain itself "
        "recursively.");
  }
  std::vector<PyObject *> key_tuples, key_dicts, val_lists, val_tuples, val_dicts, dummy;
  for (const auto& dict : dicts) {
    PyObject *key, *value;
    Py_ssize_t pos = 0;
    while (PyDict_Next(dict, &pos, &key, &value)) {
      RETURN_NOT_OK(
          append(key, result.keys(), dummy, key_tuples, key_dicts, tensors_out));
      DCHECK(dummy.size() == 0);
      RETURN_NOT_OK(
          append(value, result.vals(), val_lists, val_tuples, val_dicts, tensors_out));
    }
  }
  std::shared_ptr<Array> key_tuples_arr;
  if (key_tuples.size() > 0) {
    RETURN_NOT_OK(SerializeSequences(
        key_tuples, recursion_depth + 1, &key_tuples_arr, tensors_out));
  }
  std::shared_ptr<Array> key_dicts_arr;
  if (key_dicts.size() > 0) {
    RETURN_NOT_OK(
        SerializeDict(key_dicts, recursion_depth + 1, &key_dicts_arr, tensors_out));
  }
  std::shared_ptr<Array> val_list_arr;
  if (val_lists.size() > 0) {
    RETURN_NOT_OK(
        SerializeSequences(val_lists, recursion_depth + 1, &val_list_arr, tensors_out));
  }
  std::shared_ptr<Array> val_tuples_arr;
  if (val_tuples.size() > 0) {
    RETURN_NOT_OK(SerializeSequences(
        val_tuples, recursion_depth + 1, &val_tuples_arr, tensors_out));
  }
  std::shared_ptr<Array> val_dict_arr;
  if (val_dicts.size() > 0) {
    RETURN_NOT_OK(
        SerializeDict(val_dicts, recursion_depth + 1, &val_dict_arr, tensors_out));
  }
  result.Finish(
      key_tuples_arr, key_dicts_arr, val_list_arr, val_tuples_arr, val_dict_arr, out);

  // This block is used to decrement the reference counts of the results
  // returned by the serialization callback, which is called in SerializeArray
  // in numpy.cc as well as in DeserializeDict and in append in this file.
  static PyObject* py_type = PyUnicode_FromString("_pytype_");
  for (const auto& dict : dicts) {
    if (PyDict_Contains(dict, py_type)) {
      // If the dictionary contains the key "_pytype_", then the user has to
      // have registered a callback.
      ARROW_CHECK(pyarrow_serialize_callback);
      Py_XDECREF(dict);
    }
  }

  return Status::OK();
}

std::shared_ptr<RecordBatch> MakeBatch(std::shared_ptr<Array> data) {
  auto field = std::make_shared<Field>("list", data->type());
  std::shared_ptr<Schema> schema(new Schema({field}));
  return std::shared_ptr<RecordBatch>(new RecordBatch(schema, data->length(), {data}));
}

} // namespace arrow

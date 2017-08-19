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

#include "arrow/python/python_to_arrow.h"
#include "arrow/python/numpy_interop.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <sstream>
#include <vector>

#include <numpy/arrayobject.h>
#include <numpy/arrayscalars.h>

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/io/interfaces.h"
#include "arrow/ipc/writer.h"
#include "arrow/python/common.h"
#include "arrow/python/helpers.h"
#include "arrow/python/numpy_convert.h"
#include "arrow/python/platform.h"
#include "arrow/util/logging.h"

constexpr int32_t kMaxRecursionDepth = 100;

extern "C" {
PyObject* pyarrow_serialize_callback = NULL;
PyObject* pyarrow_deserialize_callback = NULL;
}

namespace arrow {
namespace py {

#define UPDATE(OFFSET, TAG)                                     \
  if (TAG == -1) {                                              \
    TAG = num_tags;                                             \
    num_tags += 1;                                              \
  }                                                             \
  RETURN_NOT_OK(offsets_.Append(static_cast<int32_t>(OFFSET))); \
  RETURN_NOT_OK(types_.Append(TAG));                            \
  RETURN_NOT_OK(nones_.AppendToBitmap(true));

#define ADD_ELEMENT(VARNAME, TAG)                             \
  if (TAG != -1) {                                            \
    types[TAG] = std::make_shared<Field>("", VARNAME.type()); \
    RETURN_NOT_OK(VARNAME.Finish(&children[TAG]));            \
    RETURN_NOT_OK(nones_.AppendToBitmap(true));               \
    type_ids.push_back(TAG);                                  \
  }

#define ADD_SUBSEQUENCE(DATA, OFFSETS, BUILDER, TAG, NAME)                          \
  if (DATA) {                                                                       \
    DCHECK(DATA->length() == OFFSETS.back());                                       \
    std::shared_ptr<Array> offset_array;                                            \
    Int32Builder builder(pool_, std::make_shared<Int32Type>());                     \
    RETURN_NOT_OK(builder.Append(OFFSETS.data(), OFFSETS.size()));                  \
    RETURN_NOT_OK(builder.Finish(&offset_array));                                   \
    std::shared_ptr<Array> list_array;                                              \
    RETURN_NOT_OK(ListArray::FromArrays(*offset_array, *DATA, pool_, &list_array)); \
    auto field = std::make_shared<Field>(NAME, list_array->type());                 \
    auto type =                                                                     \
        std::make_shared<StructType>(std::vector<std::shared_ptr<Field>>({field})); \
    types[TAG] = std::make_shared<Field>("", type);                                 \
    children[TAG] = std::shared_ptr<StructArray>(                                   \
        new StructArray(type, list_array->length(), {list_array}));                 \
    RETURN_NOT_OK(nones_.AppendToBitmap(true));                                     \
    type_ids.push_back(TAG);                                                        \
  } else {                                                                          \
    DCHECK_EQ(OFFSETS.size(), 1);                                                   \
  }

/// A Sequence is a heterogeneous collections of elements. It can contain
/// scalar Python types, lists, tuples, dictionaries and tensors.
class SequenceBuilder {
 public:
  explicit SequenceBuilder(MemoryPool* pool = nullptr)
      : pool_(pool),
        types_(pool, ::arrow::int8()),
        offsets_(pool, ::arrow::int32()),
        nones_(pool),
        bools_(pool, ::arrow::boolean()),
        ints_(pool, ::arrow::int64()),
        bytes_(pool, ::arrow::binary()),
        strings_(pool),
        floats_(pool, ::arrow::float32()),
        doubles_(pool, ::arrow::float64()),
        tensor_indices_(pool, ::arrow::int32()),
        list_offsets_({0}),
        tuple_offsets_({0}),
        dict_offsets_({0}) {}

  /// Appending a none to the sequence
  Status AppendNone() {
    RETURN_NOT_OK(offsets_.Append(0));
    RETURN_NOT_OK(types_.Append(0));
    return nones_.AppendToBitmap(false);
  }

  /// Appending a boolean to the sequence
  Status AppendBool(bool data) {
    UPDATE(bools_.length(), bool_tag);
    return bools_.Append(data);
  }

  /// Appending an int64_t to the sequence
  Status AppendInt64(int64_t data) {
    UPDATE(ints_.length(), int_tag);
    return ints_.Append(data);
  }

  /// Appending an uint64_t to the sequence
  Status AppendUInt64(uint64_t data) {
    UPDATE(ints_.length(), int_tag);
    return ints_.Append(data);
  }

  /// Append a list of bytes to the sequence
  Status AppendBytes(const uint8_t* data, int32_t length) {
    UPDATE(bytes_.length(), bytes_tag);
    return bytes_.Append(data, length);
  }

  /// Appending a string to the sequence
  Status AppendString(const char* data, int32_t length) {
    UPDATE(strings_.length(), string_tag);
    return strings_.Append(data, length);
  }

  /// Appending a float to the sequence
  Status AppendFloat(float data) {
    UPDATE(floats_.length(), float_tag);
    return floats_.Append(data);
  }

  /// Appending a double to the sequence
  Status AppendDouble(double data) {
    UPDATE(doubles_.length(), double_tag);
    return doubles_.Append(data);
  }

  /// Appending a tensor to the sequence
  ///
  /// \param tensor_index Index of the tensor in the object.
  Status AppendTensor(int32_t tensor_index) {
    UPDATE(tensor_indices_.length(), tensor_tag);
    return tensor_indices_.Append(tensor_index);
  }

  /// Add a sublist to the sequence. The data contained in the sublist will be
  /// specified in the "Finish" method.
  ///
  /// To construct l = [[11, 22], 33, [44, 55]] you would for example run
  /// list = ListBuilder();
  /// list.AppendList(2);
  /// list.Append(33);
  /// list.AppendList(2);
  /// list.Finish([11, 22, 44, 55]);
  /// list.Finish();

  /// \param size
  /// The size of the sublist
  Status AppendList(Py_ssize_t size) {
    UPDATE(list_offsets_.size() - 1, list_tag);
    list_offsets_.push_back(list_offsets_.back() + static_cast<int32_t>(size));
    return Status::OK();
  }

  Status AppendTuple(Py_ssize_t size) {
    UPDATE(tuple_offsets_.size() - 1, tuple_tag);
    tuple_offsets_.push_back(tuple_offsets_.back() + static_cast<int32_t>(size));
    return Status::OK();
  }

  Status AppendDict(Py_ssize_t size) {
    UPDATE(dict_offsets_.size() - 1, dict_tag);
    dict_offsets_.push_back(dict_offsets_.back() + static_cast<int32_t>(size));
    return Status::OK();
  }

  /// Finish building the sequence and return the result.
  Status Finish(std::shared_ptr<Array> list_data, std::shared_ptr<Array> tuple_data,
                std::shared_ptr<Array> dict_data, std::shared_ptr<Array>* out) {
    std::vector<std::shared_ptr<Field>> types(num_tags);
    std::vector<std::shared_ptr<Array>> children(num_tags);
    std::vector<uint8_t> type_ids;

    ADD_ELEMENT(bools_, bool_tag);
    ADD_ELEMENT(ints_, int_tag);
    ADD_ELEMENT(strings_, string_tag);
    ADD_ELEMENT(bytes_, bytes_tag);
    ADD_ELEMENT(floats_, float_tag);
    ADD_ELEMENT(doubles_, double_tag);

    ADD_ELEMENT(tensor_indices_, tensor_tag);

    ADD_SUBSEQUENCE(list_data, list_offsets_, list_builder, list_tag, "list");
    ADD_SUBSEQUENCE(tuple_data, tuple_offsets_, tuple_builder, tuple_tag, "tuple");
    ADD_SUBSEQUENCE(dict_data, dict_offsets_, dict_builder, dict_tag, "dict");

    auto type = ::arrow::union_(types, type_ids, UnionMode::DENSE);
    out->reset(new UnionArray(type, types_.length(), children, types_.data(),
                              offsets_.data(), nones_.null_bitmap(),
                              nones_.null_count()));
    return Status::OK();
  }

 private:
  MemoryPool* pool_;

  Int8Builder types_;
  Int32Builder offsets_;

  NullBuilder nones_;
  BooleanBuilder bools_;
  Int64Builder ints_;
  BinaryBuilder bytes_;
  StringBuilder strings_;
  FloatBuilder floats_;
  DoubleBuilder doubles_;

  // We use an Int32Builder here to distinguish the tensor indices from
  // the ints_ above (see the case Type::INT32 in get_value in python.cc).
  // TODO(pcm): Replace this by using the union tags to distinguish between
  // these two cases.
  Int32Builder tensor_indices_;

  std::vector<int32_t> list_offsets_;
  std::vector<int32_t> tuple_offsets_;
  std::vector<int32_t> dict_offsets_;

  // Tags for members of the sequence. If they are set to -1 it means
  // they are not used and will not part be of the metadata when we call
  // SequenceBuilder::Finish. If a member with one of the tags is added,
  // the associated variable gets a unique index starting from 0. This
  // happens in the UPDATE macro in sequence.cc.
  int8_t bool_tag = -1;
  int8_t int_tag = -1;
  int8_t string_tag = -1;
  int8_t bytes_tag = -1;
  int8_t float_tag = -1;
  int8_t double_tag = -1;

  int8_t tensor_tag = -1;
  int8_t list_tag = -1;
  int8_t tuple_tag = -1;
  int8_t dict_tag = -1;

  int8_t num_tags = 0;
};

/// Constructing dictionaries of key/value pairs. Sequences of
/// keys and values are built separately using a pair of
/// SequenceBuilders. The resulting Arrow representation
/// can be obtained via the Finish method.
class DictBuilder {
 public:
  explicit DictBuilder(MemoryPool* pool = nullptr) : keys_(pool), vals_(pool) {}

  /// Builder for the keys of the dictionary
  SequenceBuilder& keys() { return keys_; }
  /// Builder for the values of the dictionary
  SequenceBuilder& vals() { return vals_; }

  /// Construct an Arrow StructArray representing the dictionary.
  /// Contains a field "keys" for the keys and "vals" for the values.

  /// \param list_data
  ///    List containing the data from nested lists in the value
  ///   list of the dictionary
  ///
  /// \param dict_data
  ///   List containing the data from nested dictionaries in the
  ///   value list of the dictionary
  Status Finish(std::shared_ptr<Array> key_tuple_data,
                std::shared_ptr<Array> key_dict_data,
                std::shared_ptr<Array> val_list_data,
                std::shared_ptr<Array> val_tuple_data,
                std::shared_ptr<Array> val_dict_data, std::shared_ptr<Array>* out) {
    // lists and dicts can't be keys of dicts in Python, that is why for
    // the keys we do not need to collect sublists
    std::shared_ptr<Array> keys, vals;
    RETURN_NOT_OK(keys_.Finish(nullptr, key_tuple_data, key_dict_data, &keys));
    RETURN_NOT_OK(vals_.Finish(val_list_data, val_tuple_data, val_dict_data, &vals));
    auto keys_field = std::make_shared<Field>("keys", keys->type());
    auto vals_field = std::make_shared<Field>("vals", vals->type());
    auto type = std::make_shared<StructType>(
        std::vector<std::shared_ptr<Field>>({keys_field, vals_field}));
    std::vector<std::shared_ptr<Array>> field_arrays({keys, vals});
    DCHECK(keys->length() == vals->length());
    out->reset(new StructArray(type, keys->length(), field_arrays));
    return Status::OK();
  }

 private:
  SequenceBuilder keys_;
  SequenceBuilder vals_;
};

Status CallCustomCallback(PyObject* callback, PyObject* elem, PyObject** result) {
  *result = NULL;
  if (!callback) {
    std::stringstream ss;
    ScopedRef repr(PyObject_Repr(elem));
    RETURN_IF_PYERROR();
    ScopedRef ascii(PyUnicode_AsASCIIString(repr.get()));
    ss << "error while calling callback on " << PyBytes_AsString(ascii.get())
       << ": handler not registered";
    return Status::NotImplemented(ss.str());
  } else {
    ScopedRef arglist(Py_BuildValue("(O)", elem));
    *result = PyObject_CallObject(callback, arglist.get());
    RETURN_IF_PYERROR();
  }
  return Status::OK();
}

void set_serialization_callbacks(PyObject* serialize_callback,
                                 PyObject* deserialize_callback) {
  pyarrow_serialize_callback = serialize_callback;
  pyarrow_deserialize_callback = deserialize_callback;
}

Status CallCustomSerializationCallback(PyObject* elem, PyObject** serialized_object) {
  RETURN_NOT_OK(CallCustomCallback(pyarrow_serialize_callback, elem, serialized_object));
  if (!PyDict_Check(*serialized_object)) {
    return Status::TypeError("serialization callback must return a valid dictionary");
  }
  return Status::OK();
}

Status SerializeDict(std::vector<PyObject*> dicts, int32_t recursion_depth,
                     std::shared_ptr<Array>* out, std::vector<PyObject*>* tensors_out);

Status SerializeArray(PyArrayObject* array, SequenceBuilder* builder,
                      std::vector<PyObject*>* subdicts,
                      std::vector<PyObject*>* tensors_out);

Status SerializeSequences(std::vector<PyObject*> sequences, int32_t recursion_depth,
                          std::shared_ptr<Array>* out,
                          std::vector<PyObject*>* tensors_out);

Status AppendScalar(PyObject* obj, SequenceBuilder* builder) {
  if (PyArray_IsScalar(obj, Bool)) {
    return builder->AppendBool(reinterpret_cast<PyBoolScalarObject*>(obj)->obval != 0);
  } else if (PyArray_IsScalar(obj, Float)) {
    return builder->AppendFloat(reinterpret_cast<PyFloatScalarObject*>(obj)->obval);
  } else if (PyArray_IsScalar(obj, Double)) {
    return builder->AppendDouble(reinterpret_cast<PyDoubleScalarObject*>(obj)->obval);
  }
  int64_t value = 0;
  if (PyArray_IsScalar(obj, Byte)) {
    value = reinterpret_cast<PyByteScalarObject*>(obj)->obval;
  } else if (PyArray_IsScalar(obj, UByte)) {
    value = reinterpret_cast<PyUByteScalarObject*>(obj)->obval;
  } else if (PyArray_IsScalar(obj, Short)) {
    value = reinterpret_cast<PyShortScalarObject*>(obj)->obval;
  } else if (PyArray_IsScalar(obj, UShort)) {
    value = reinterpret_cast<PyUShortScalarObject*>(obj)->obval;
  } else if (PyArray_IsScalar(obj, Int)) {
    value = reinterpret_cast<PyIntScalarObject*>(obj)->obval;
  } else if (PyArray_IsScalar(obj, UInt)) {
    value = reinterpret_cast<PyUIntScalarObject*>(obj)->obval;
  } else if (PyArray_IsScalar(obj, Long)) {
    value = reinterpret_cast<PyLongScalarObject*>(obj)->obval;
  } else if (PyArray_IsScalar(obj, ULong)) {
    value = reinterpret_cast<PyULongScalarObject*>(obj)->obval;
  } else if (PyArray_IsScalar(obj, LongLong)) {
    value = reinterpret_cast<PyLongLongScalarObject*>(obj)->obval;
  } else if (PyArray_IsScalar(obj, Int64)) {
    value = reinterpret_cast<PyInt64ScalarObject*>(obj)->obval;
  } else if (PyArray_IsScalar(obj, ULongLong)) {
    value = reinterpret_cast<PyULongLongScalarObject*>(obj)->obval;
  } else if (PyArray_IsScalar(obj, UInt64)) {
    value = reinterpret_cast<PyUInt64ScalarObject*>(obj)->obval;
  } else {
    DCHECK(false) << "scalar type not recognized";
  }
  return builder->AppendInt64(value);
}

Status Append(PyObject* elem, SequenceBuilder* builder, std::vector<PyObject*>* sublists,
              std::vector<PyObject*>* subtuples, std::vector<PyObject*>* subdicts,
              std::vector<PyObject*>* tensors_out) {
  // The bool case must precede the int case (PyInt_Check passes for bools)
  if (PyBool_Check(elem)) {
    RETURN_NOT_OK(builder->AppendBool(elem == Py_True));
  } else if (PyFloat_Check(elem)) {
    RETURN_NOT_OK(builder->AppendDouble(PyFloat_AS_DOUBLE(elem)));
  } else if (PyLong_Check(elem)) {
    int overflow = 0;
    int64_t data = PyLong_AsLongLongAndOverflow(elem, &overflow);
    if (!overflow) {
      RETURN_NOT_OK(builder->AppendInt64(data));
    } else {
      // Attempt to serialize the object using the custom callback.
      PyObject* serialized_object;
      // The reference count of serialized_object will be decremented in SerializeDict
      RETURN_NOT_OK(CallCustomSerializationCallback(elem, &serialized_object));
      RETURN_NOT_OK(builder->AppendDict(PyDict_Size(serialized_object)));
      subdicts->push_back(serialized_object);
    }
#if PY_MAJOR_VERSION < 3
  } else if (PyInt_Check(elem)) {
    RETURN_NOT_OK(builder->AppendInt64(static_cast<int64_t>(PyInt_AS_LONG(elem))));
#endif
  } else if (PyBytes_Check(elem)) {
    auto data = reinterpret_cast<uint8_t*>(PyBytes_AS_STRING(elem));
    const int64_t size = static_cast<int64_t>(PyBytes_GET_SIZE(elem));
    if (size > std::numeric_limits<int32_t>::max()) {
      return Status::Invalid("Cannot writes bytes over 2GB");
    }
    RETURN_NOT_OK(builder->AppendBytes(data, static_cast<int32_t>(size)));
  } else if (PyUnicode_Check(elem)) {
    Py_ssize_t size;
#if PY_MAJOR_VERSION >= 3
    char* data = PyUnicode_AsUTF8AndSize(elem, &size);
#else
    ScopedRef str(PyUnicode_AsUTF8String(elem));
    char* data = PyString_AS_STRING(str.get());
    size = PyString_GET_SIZE(str.get());
#endif
    if (size > std::numeric_limits<int32_t>::max()) {
      return Status::Invalid("Cannot writes bytes over 2GB");
    }
    RETURN_NOT_OK(builder->AppendString(data, static_cast<int32_t>(size)));
  } else if (PyList_Check(elem)) {
    RETURN_NOT_OK(builder->AppendList(PyList_Size(elem)));
    sublists->push_back(elem);
  } else if (PyDict_Check(elem)) {
    RETURN_NOT_OK(builder->AppendDict(PyDict_Size(elem)));
    subdicts->push_back(elem);
  } else if (PyTuple_CheckExact(elem)) {
    RETURN_NOT_OK(builder->AppendTuple(PyTuple_Size(elem)));
    subtuples->push_back(elem);
  } else if (PyArray_IsScalar(elem, Generic)) {
    RETURN_NOT_OK(AppendScalar(elem, builder));
  } else if (PyArray_Check(elem)) {
    RETURN_NOT_OK(SerializeArray(reinterpret_cast<PyArrayObject*>(elem), builder,
                                 subdicts, tensors_out));
  } else if (elem == Py_None) {
    RETURN_NOT_OK(builder->AppendNone());
  } else {
    // Attempt to serialize the object using the custom callback.
    PyObject* serialized_object;
    // The reference count of serialized_object will be decremented in SerializeDict
    RETURN_NOT_OK(CallCustomSerializationCallback(elem, &serialized_object));
    RETURN_NOT_OK(builder->AppendDict(PyDict_Size(serialized_object)));
    subdicts->push_back(serialized_object);
  }
  return Status::OK();
}

Status SerializeArray(PyArrayObject* array, SequenceBuilder* builder,
                      std::vector<PyObject*>* subdicts,
                      std::vector<PyObject*>* tensors_out) {
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
      RETURN_NOT_OK(builder->AppendTensor(static_cast<int32_t>(tensors_out->size())));
      tensors_out->push_back(reinterpret_cast<PyObject*>(array));
    } break;
    default: {
      PyObject* serialized_object;
      // The reference count of serialized_object will be decremented in SerializeDict
      RETURN_NOT_OK(CallCustomSerializationCallback(reinterpret_cast<PyObject*>(array),
                                                    &serialized_object));
      RETURN_NOT_OK(builder->AppendDict(PyDict_Size(serialized_object)));
      subdicts->push_back(serialized_object);
    }
  }
  return Status::OK();
}

Status SerializeSequences(std::vector<PyObject*> sequences, int32_t recursion_depth,
                          std::shared_ptr<Array>* out,
                          std::vector<PyObject*>* tensors_out) {
  DCHECK(out);
  if (recursion_depth >= kMaxRecursionDepth) {
    return Status::NotImplemented(
        "This object exceeds the maximum recursion depth. It may contain itself "
        "recursively.");
  }
  SequenceBuilder builder(nullptr);
  std::vector<PyObject *> sublists, subtuples, subdicts;
  for (const auto& sequence : sequences) {
    ScopedRef iterator(PyObject_GetIter(sequence));
    RETURN_IF_PYERROR();
    ScopedRef item;
    while (item.reset(PyIter_Next(iterator.get())), item.get()) {
      RETURN_NOT_OK(
          Append(item.get(), &builder, &sublists, &subtuples, &subdicts, tensors_out));
    }
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
                     std::shared_ptr<Array>* out, std::vector<PyObject*>* tensors_out) {
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
          Append(key, &result.keys(), &dummy, &key_tuples, &key_dicts, tensors_out));
      DCHECK_EQ(dummy.size(), 0);
      RETURN_NOT_OK(Append(value, &result.vals(), &val_lists, &val_tuples, &val_dicts,
                           tensors_out));
    }
  }
  std::shared_ptr<Array> key_tuples_arr;
  if (key_tuples.size() > 0) {
    RETURN_NOT_OK(SerializeSequences(key_tuples, recursion_depth + 1, &key_tuples_arr,
                                     tensors_out));
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
    RETURN_NOT_OK(SerializeSequences(val_tuples, recursion_depth + 1, &val_tuples_arr,
                                     tensors_out));
  }
  std::shared_ptr<Array> val_dict_arr;
  if (val_dicts.size() > 0) {
    RETURN_NOT_OK(
        SerializeDict(val_dicts, recursion_depth + 1, &val_dict_arr, tensors_out));
  }
  RETURN_NOT_OK(result.Finish(key_tuples_arr, key_dicts_arr, val_list_arr, val_tuples_arr,
                              val_dict_arr, out));

  // This block is used to decrement the reference counts of the results
  // returned by the serialization callback, which is called in SerializeArray,
  // in DeserializeDict and in Append
  static PyObject* py_type = PyUnicode_FromString("_pytype_");
  for (const auto& dict : dicts) {
    if (PyDict_Contains(dict, py_type)) {
      // If the dictionary contains the key "_pytype_", then the user has to
      // have registered a callback.
      if (pyarrow_serialize_callback == nullptr) {
        return Status::Invalid("No serialization callback set");
      }
      Py_XDECREF(dict);
    }
  }

  return Status::OK();
}

std::shared_ptr<RecordBatch> MakeBatch(std::shared_ptr<Array> data) {
  auto field = std::make_shared<Field>("list", data->type());
  auto schema = ::arrow::schema({field});
  return std::shared_ptr<RecordBatch>(new RecordBatch(schema, data->length(), {data}));
}

Status SerializePythonSequence(PyObject* sequence,
                               std::shared_ptr<RecordBatch>* batch_out,
                               std::vector<std::shared_ptr<Tensor>>* tensors_out) {
  PyAcquireGIL lock;
  std::vector<PyObject*> sequences = {sequence};
  std::shared_ptr<Array> array;
  std::vector<PyObject*> tensors;
  RETURN_NOT_OK(SerializeSequences(sequences, 0, &array, &tensors));
  *batch_out = MakeBatch(array);
  for (const auto& tensor : tensors) {
    std::shared_ptr<Tensor> out;
    RETURN_NOT_OK(NdarrayToTensor(default_memory_pool(), tensor, &out));
    tensors_out->push_back(out);
  }
  return Status::OK();
}

Status WriteSerializedPythonSequence(std::shared_ptr<RecordBatch> batch,
                                     std::vector<std::shared_ptr<Tensor>> tensors,
                                     io::OutputStream* dst) {
  int32_t num_tensors = static_cast<int32_t>(tensors.size());
  std::shared_ptr<ipc::RecordBatchStreamWriter> writer;
  int32_t metadata_length;
  int64_t body_length;

  RETURN_NOT_OK(dst->Write(reinterpret_cast<uint8_t*>(&num_tensors), sizeof(int32_t)));
  RETURN_NOT_OK(ipc::RecordBatchStreamWriter::Open(dst, batch->schema(), &writer));
  RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  RETURN_NOT_OK(writer->Close());

  for (const auto& tensor : tensors) {
    RETURN_NOT_OK(ipc::WriteTensor(*tensor, dst, &metadata_length, &body_length));
  }

  return Status::OK();
}

}  // namespace py
}  // namespace arrow

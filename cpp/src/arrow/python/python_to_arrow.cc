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
#include <string>
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
#include "arrow/tensor.h"
#include "arrow/util/logging.h"

constexpr int32_t kMaxRecursionDepth = 100;

namespace arrow {
namespace py {

/// A Sequence is a heterogeneous collections of elements. It can contain
/// scalar Python types, lists, tuples, dictionaries and tensors.
class SequenceBuilder {
 public:
  explicit SequenceBuilder(MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT)
      : pool_(pool),
        types_(::arrow::int8(), pool),
        offsets_(::arrow::int32(), pool),
        nones_(pool),
        bools_(::arrow::boolean(), pool),
        ints_(::arrow::int64(), pool),
        bytes_(::arrow::binary(), pool),
        strings_(pool),
        floats_(::arrow::float32(), pool),
        doubles_(::arrow::float64(), pool),
        tensor_indices_(::arrow::int32(), pool),
        list_offsets_({0}),
        tuple_offsets_({0}),
        dict_offsets_({0}) {}

  /// Appending a none to the sequence
  Status AppendNone() {
    RETURN_NOT_OK(offsets_.Append(0));
    RETURN_NOT_OK(types_.Append(0));
    return nones_.AppendToBitmap(false);
  }

  Status Update(int64_t offset, int8_t* tag) {
    if (*tag == -1) {
      *tag = num_tags_++;
    }
    RETURN_NOT_OK(offsets_.Append(static_cast<int32_t>(offset)));
    RETURN_NOT_OK(types_.Append(*tag));
    return nones_.AppendToBitmap(true);
  }

  template <typename BuilderType, typename T>
  Status AppendPrimitive(const T val, int8_t* tag, BuilderType* out) {
    RETURN_NOT_OK(Update(out->length(), tag));
    return out->Append(val);
  }

  /// Appending a boolean to the sequence
  Status AppendBool(const bool data) {
    return AppendPrimitive(data, &bool_tag_, &bools_);
  }

  /// Appending an int64_t to the sequence
  Status AppendInt64(const int64_t data) {
    return AppendPrimitive(data, &int_tag_, &ints_);
  }

  /// Appending an uint64_t to the sequence
  Status AppendUInt64(const uint64_t data) {
    // TODO(wesm): Bounds check
    return AppendPrimitive(static_cast<int64_t>(data), &int_tag_, &ints_);
  }

  /// Append a list of bytes to the sequence
  Status AppendBytes(const uint8_t* data, int32_t length) {
    RETURN_NOT_OK(Update(bytes_.length(), &bytes_tag_));
    return bytes_.Append(data, length);
  }

  /// Appending a string to the sequence
  Status AppendString(const char* data, int32_t length) {
    RETURN_NOT_OK(Update(strings_.length(), &string_tag_));
    return strings_.Append(data, length);
  }

  /// Appending a float to the sequence
  Status AppendFloat(const float data) {
    return AppendPrimitive(data, &float_tag_, &floats_);
  }

  /// Appending a double to the sequence
  Status AppendDouble(const double data) {
    return AppendPrimitive(data, &double_tag_, &doubles_);
  }

  /// Appending a tensor to the sequence
  ///
  /// \param tensor_index Index of the tensor in the object.
  Status AppendTensor(const int32_t tensor_index) {
    RETURN_NOT_OK(Update(tensor_indices_.length(), &tensor_tag_));
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
    RETURN_NOT_OK(Update(list_offsets_.size() - 1, &list_tag_));
    list_offsets_.push_back(list_offsets_.back() + static_cast<int32_t>(size));
    return Status::OK();
  }

  Status AppendTuple(Py_ssize_t size) {
    RETURN_NOT_OK(Update(tuple_offsets_.size() - 1, &tuple_tag_));
    tuple_offsets_.push_back(tuple_offsets_.back() + static_cast<int32_t>(size));
    return Status::OK();
  }

  Status AppendDict(Py_ssize_t size) {
    RETURN_NOT_OK(Update(dict_offsets_.size() - 1, &dict_tag_));
    dict_offsets_.push_back(dict_offsets_.back() + static_cast<int32_t>(size));
    return Status::OK();
  }

  template <typename BuilderType>
  Status AddElement(const int8_t tag, BuilderType* out) {
    if (tag != -1) {
      fields_[tag] = ::arrow::field("", out->type());
      RETURN_NOT_OK(out->Finish(&children_[tag]));
      RETURN_NOT_OK(nones_.AppendToBitmap(true));
      type_ids_.push_back(tag);
    }
    return Status::OK();
  }

  Status AddSubsequence(int8_t tag, const Array* data,
                        const std::vector<int32_t>& offsets, const std::string& name) {
    if (data != nullptr) {
      DCHECK(data->length() == offsets.back());
      std::shared_ptr<Array> offset_array;
      Int32Builder builder(::arrow::int32(), pool_);
      RETURN_NOT_OK(builder.Append(offsets.data(), offsets.size()));
      RETURN_NOT_OK(builder.Finish(&offset_array));
      std::shared_ptr<Array> list_array;
      RETURN_NOT_OK(ListArray::FromArrays(*offset_array, *data, pool_, &list_array));
      auto field = ::arrow::field(name, list_array->type());
      auto type = ::arrow::struct_({field});
      fields_[tag] = ::arrow::field("", type);
      children_[tag] = std::shared_ptr<StructArray>(
          new StructArray(type, list_array->length(), {list_array}));
      RETURN_NOT_OK(nones_.AppendToBitmap(true));
      type_ids_.push_back(tag);
    } else {
      DCHECK_EQ(offsets.size(), 1);
    }
    return Status::OK();
  }

  /// Finish building the sequence and return the result.
  /// Input arrays may be nullptr
  Status Finish(const Array* list_data, const Array* tuple_data, const Array* dict_data,
                std::shared_ptr<Array>* out) {
    fields_.resize(num_tags_);
    children_.resize(num_tags_);

    RETURN_NOT_OK(AddElement(bool_tag_, &bools_));
    RETURN_NOT_OK(AddElement(int_tag_, &ints_));
    RETURN_NOT_OK(AddElement(string_tag_, &strings_));
    RETURN_NOT_OK(AddElement(bytes_tag_, &bytes_));
    RETURN_NOT_OK(AddElement(float_tag_, &floats_));
    RETURN_NOT_OK(AddElement(double_tag_, &doubles_));
    RETURN_NOT_OK(AddElement(tensor_tag_, &tensor_indices_));

    RETURN_NOT_OK(AddSubsequence(list_tag_, list_data, list_offsets_, "list"));
    RETURN_NOT_OK(AddSubsequence(tuple_tag_, tuple_data, tuple_offsets_, "tuple"));
    RETURN_NOT_OK(AddSubsequence(dict_tag_, dict_data, dict_offsets_, "dict"));

    auto type = ::arrow::union_(fields_, type_ids_, UnionMode::DENSE);
    out->reset(new UnionArray(type, types_.length(), children_, types_.data(),
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
  int8_t bool_tag_ = -1;
  int8_t int_tag_ = -1;
  int8_t string_tag_ = -1;
  int8_t bytes_tag_ = -1;
  int8_t float_tag_ = -1;
  int8_t double_tag_ = -1;

  int8_t tensor_tag_ = -1;
  int8_t list_tag_ = -1;
  int8_t tuple_tag_ = -1;
  int8_t dict_tag_ = -1;

  int8_t num_tags_ = 0;

  // Members for the output union constructed in Finish
  std::vector<std::shared_ptr<Field>> fields_;
  std::vector<std::shared_ptr<Array>> children_;
  std::vector<uint8_t> type_ids_;
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
  Status Finish(const Array* key_tuple_data, const Array* key_dict_data,
                const Array* val_list_data, const Array* val_tuple_data,
                const Array* val_dict_data, std::shared_ptr<Array>* out) {
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

Status CallCustomCallback(PyObject* context, PyObject* method_name, PyObject* elem,
                          PyObject** result) {
  *result = NULL;
  if (context == Py_None) {
    std::stringstream ss;
    ScopedRef repr(PyObject_Repr(elem));
    RETURN_IF_PYERROR();
#if PY_MAJOR_VERSION >= 3
    ScopedRef ascii(PyUnicode_AsASCIIString(repr.get()));
    RETURN_IF_PYERROR();
    ss << "error while calling callback on " << PyBytes_AsString(ascii.get())
       << ": handler not registered";
#else
    ss << "error while calling callback on " << PyString_AsString(repr.get())
       << ": handler not registered";
#endif
    return Status::SerializationError(ss.str());
  } else {
    *result = PyObject_CallMethodObjArgs(context, method_name, elem, NULL);
    RETURN_IF_PYERROR();
  }
  return Status::OK();
}

Status CallSerializeCallback(PyObject* context, PyObject* value,
                             PyObject** serialized_object) {
  ScopedRef method_name(PyUnicode_FromString("_serialize_callback"));
  RETURN_NOT_OK(CallCustomCallback(context, method_name.get(), value, serialized_object));
  if (!PyDict_Check(*serialized_object)) {
    return Status::TypeError("serialization callback must return a valid dictionary");
  }
  return Status::OK();
}

Status CallDeserializeCallback(PyObject* context, PyObject* value,
                               PyObject** deserialized_object) {
  ScopedRef method_name(PyUnicode_FromString("_deserialize_callback"));
  return CallCustomCallback(context, method_name.get(), value, deserialized_object);
}

Status SerializeDict(PyObject* context, std::vector<PyObject*> dicts,
                     int32_t recursion_depth, std::shared_ptr<Array>* out,
                     std::vector<PyObject*>* tensors_out);

Status SerializeArray(PyObject* context, PyArrayObject* array, SequenceBuilder* builder,
                      std::vector<PyObject*>* subdicts,
                      std::vector<PyObject*>* tensors_out);

Status SerializeSequences(PyObject* context, std::vector<PyObject*> sequences,
                          int32_t recursion_depth, std::shared_ptr<Array>* out,
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

Status Append(PyObject* context, PyObject* elem, SequenceBuilder* builder,
              std::vector<PyObject*>* sublists, std::vector<PyObject*>* subtuples,
              std::vector<PyObject*>* subdicts, std::vector<PyObject*>* tensors_out) {
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
      RETURN_NOT_OK(CallSerializeCallback(context, elem, &serialized_object));
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
    RETURN_NOT_OK(SerializeArray(context, reinterpret_cast<PyArrayObject*>(elem), builder,
                                 subdicts, tensors_out));
  } else if (elem == Py_None) {
    RETURN_NOT_OK(builder->AppendNone());
  } else {
    // Attempt to serialize the object using the custom callback.
    PyObject* serialized_object;
    // The reference count of serialized_object will be decremented in SerializeDict
    RETURN_NOT_OK(CallSerializeCallback(context, elem, &serialized_object));
    RETURN_NOT_OK(builder->AppendDict(PyDict_Size(serialized_object)));
    subdicts->push_back(serialized_object);
  }
  return Status::OK();
}

Status SerializeArray(PyObject* context, PyArrayObject* array, SequenceBuilder* builder,
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
      RETURN_NOT_OK(CallSerializeCallback(context, reinterpret_cast<PyObject*>(array),
                                          &serialized_object));
      RETURN_NOT_OK(builder->AppendDict(PyDict_Size(serialized_object)));
      subdicts->push_back(serialized_object);
    }
  }
  return Status::OK();
}

Status SerializeSequences(PyObject* context, std::vector<PyObject*> sequences,
                          int32_t recursion_depth, std::shared_ptr<Array>* out,
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
      RETURN_NOT_OK(Append(context, item.get(), &builder, &sublists, &subtuples,
                           &subdicts, tensors_out));
    }
  }
  std::shared_ptr<Array> list;
  if (sublists.size() > 0) {
    RETURN_NOT_OK(
        SerializeSequences(context, sublists, recursion_depth + 1, &list, tensors_out));
  }
  std::shared_ptr<Array> tuple;
  if (subtuples.size() > 0) {
    RETURN_NOT_OK(
        SerializeSequences(context, subtuples, recursion_depth + 1, &tuple, tensors_out));
  }
  std::shared_ptr<Array> dict;
  if (subdicts.size() > 0) {
    RETURN_NOT_OK(
        SerializeDict(context, subdicts, recursion_depth + 1, &dict, tensors_out));
  }
  return builder.Finish(list.get(), tuple.get(), dict.get(), out);
}

Status SerializeDict(PyObject* context, std::vector<PyObject*> dicts,
                     int32_t recursion_depth, std::shared_ptr<Array>* out,
                     std::vector<PyObject*>* tensors_out) {
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
      RETURN_NOT_OK(Append(context, key, &result.keys(), &dummy, &key_tuples, &key_dicts,
                           tensors_out));
      DCHECK_EQ(dummy.size(), 0);
      RETURN_NOT_OK(Append(context, value, &result.vals(), &val_lists, &val_tuples,
                           &val_dicts, tensors_out));
    }
  }
  std::shared_ptr<Array> key_tuples_arr;
  if (key_tuples.size() > 0) {
    RETURN_NOT_OK(SerializeSequences(context, key_tuples, recursion_depth + 1,
                                     &key_tuples_arr, tensors_out));
  }
  std::shared_ptr<Array> key_dicts_arr;
  if (key_dicts.size() > 0) {
    RETURN_NOT_OK(SerializeDict(context, key_dicts, recursion_depth + 1, &key_dicts_arr,
                                tensors_out));
  }
  std::shared_ptr<Array> val_list_arr;
  if (val_lists.size() > 0) {
    RETURN_NOT_OK(SerializeSequences(context, val_lists, recursion_depth + 1,
                                     &val_list_arr, tensors_out));
  }
  std::shared_ptr<Array> val_tuples_arr;
  if (val_tuples.size() > 0) {
    RETURN_NOT_OK(SerializeSequences(context, val_tuples, recursion_depth + 1,
                                     &val_tuples_arr, tensors_out));
  }
  std::shared_ptr<Array> val_dict_arr;
  if (val_dicts.size() > 0) {
    RETURN_NOT_OK(SerializeDict(context, val_dicts, recursion_depth + 1, &val_dict_arr,
                                tensors_out));
  }
  RETURN_NOT_OK(result.Finish(key_tuples_arr.get(), key_dicts_arr.get(),
                              val_list_arr.get(), val_tuples_arr.get(),
                              val_dict_arr.get(), out));

  // This block is used to decrement the reference counts of the results
  // returned by the serialization callback, which is called in SerializeArray,
  // in DeserializeDict and in Append
  static PyObject* py_type = PyUnicode_FromString("_pytype_");
  for (const auto& dict : dicts) {
    if (PyDict_Contains(dict, py_type)) {
      // If the dictionary contains the key "_pytype_", then the user has to
      // have registered a callback.
      if (context == Py_None) {
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

Status SerializeObject(PyObject* context, PyObject* sequence, SerializedPyObject* out) {
  PyAcquireGIL lock;
  std::vector<PyObject*> sequences = {sequence};
  std::shared_ptr<Array> array;
  std::vector<PyObject*> py_tensors;
  RETURN_NOT_OK(SerializeSequences(context, sequences, 0, &array, &py_tensors));
  out->batch = MakeBatch(array);
  for (const auto& py_tensor : py_tensors) {
    std::shared_ptr<Tensor> arrow_tensor;
    RETURN_NOT_OK(NdarrayToTensor(default_memory_pool(), py_tensor, &arrow_tensor));
    out->tensors.push_back(arrow_tensor);
  }
  return Status::OK();
}

Status WriteSerializedObject(const SerializedPyObject& obj, io::OutputStream* dst) {
  int32_t num_tensors = static_cast<int32_t>(obj.tensors.size());
  RETURN_NOT_OK(dst->Write(reinterpret_cast<uint8_t*>(&num_tensors), sizeof(int32_t)));
  RETURN_NOT_OK(ipc::WriteRecordBatchStream({obj.batch}, dst));

  int32_t metadata_length;
  int64_t body_length;
  for (const auto& tensor : obj.tensors) {
    RETURN_NOT_OK(ipc::WriteTensor(*tensor, dst, &metadata_length, &body_length));
  }

  return Status::OK();
}

}  // namespace py
}  // namespace arrow

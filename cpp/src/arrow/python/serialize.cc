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

#include "arrow/python/serialize.h"
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
#include "arrow/io/memory.h"
#include "arrow/ipc/util.h"
#include "arrow/ipc/writer.h"
#include "arrow/memory_pool.h"
#include "arrow/record_batch.h"
#include "arrow/tensor.h"
#include "arrow/util/logging.h"

#include "arrow/python/common.h"
#include "arrow/python/helpers.h"
#include "arrow/python/iterators.h"
#include "arrow/python/numpy_convert.h"
#include "arrow/python/platform.h"
#include "arrow/python/pyarrow.h"
#include "arrow/python/util/datetime.h"

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
        py2_ints_(::arrow::int64(), pool),
        bytes_(::arrow::binary(), pool),
        strings_(pool),
        half_floats_(::arrow::float16(), pool),
        floats_(::arrow::float32(), pool),
        doubles_(::arrow::float64(), pool),
        date64s_(::arrow::date64(), pool),
        tensor_indices_(::arrow::int32(), pool),
        buffer_indices_(::arrow::int32(), pool),
        list_offsets_({0}),
        tuple_offsets_({0}),
        dict_offsets_({0}),
        set_offsets_({0}) {}

  /// Appending a none to the sequence
  Status AppendNone() {
    RETURN_NOT_OK(offsets_.Append(0));
    RETURN_NOT_OK(types_.Append(0));
    return nones_.AppendNull();
  }

  Status Update(int64_t offset, int8_t* tag) {
    if (*tag == -1) {
      *tag = num_tags_++;
    }
    int32_t offset32 = -1;
    RETURN_NOT_OK(internal::CastSize(offset, &offset32));
    DCHECK_GE(offset32, 0);
    RETURN_NOT_OK(offsets_.Append(offset32));
    RETURN_NOT_OK(types_.Append(*tag));
    return nones_.Append(true);
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

  /// Appending a python 2 int64_t to the sequence
  Status AppendPy2Int64(const int64_t data) {
    return AppendPrimitive(data, &py2_int_tag_, &py2_ints_);
  }

  /// Appending an int64_t to the sequence
  Status AppendInt64(const int64_t data) {
    return AppendPrimitive(data, &int_tag_, &ints_);
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

  /// Appending a half_float to the sequence
  Status AppendHalfFloat(const npy_half data) {
    return AppendPrimitive(data, &half_float_tag_, &half_floats_);
  }

  /// Appending a float to the sequence
  Status AppendFloat(const float data) {
    return AppendPrimitive(data, &float_tag_, &floats_);
  }

  /// Appending a double to the sequence
  Status AppendDouble(const double data) {
    return AppendPrimitive(data, &double_tag_, &doubles_);
  }

  /// Appending a Date64 timestamp to the sequence
  Status AppendDate64(const int64_t timestamp) {
    return AppendPrimitive(timestamp, &date64_tag_, &date64s_);
  }

  /// Appending a tensor to the sequence
  ///
  /// \param tensor_index Index of the tensor in the object.
  Status AppendTensor(const int32_t tensor_index) {
    RETURN_NOT_OK(Update(tensor_indices_.length(), &tensor_tag_));
    return tensor_indices_.Append(tensor_index);
  }

  /// Appending a buffer to the sequence
  ///
  /// \param buffer_index Indes of the buffer in the object.
  Status AppendBuffer(const int32_t buffer_index) {
    RETURN_NOT_OK(Update(buffer_indices_.length(), &buffer_tag_));
    return buffer_indices_.Append(buffer_index);
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
    int32_t offset;
    RETURN_NOT_OK(internal::CastSize(list_offsets_.back() + size, &offset));
    RETURN_NOT_OK(Update(list_offsets_.size() - 1, &list_tag_));
    list_offsets_.push_back(offset);
    return Status::OK();
  }

  Status AppendTuple(Py_ssize_t size) {
    int32_t offset;
    RETURN_NOT_OK(internal::CastSize(tuple_offsets_.back() + size, &offset));
    RETURN_NOT_OK(Update(tuple_offsets_.size() - 1, &tuple_tag_));
    tuple_offsets_.push_back(offset);
    return Status::OK();
  }

  Status AppendDict(Py_ssize_t size) {
    int32_t offset;
    RETURN_NOT_OK(internal::CastSize(dict_offsets_.back() + size, &offset));
    RETURN_NOT_OK(Update(dict_offsets_.size() - 1, &dict_tag_));
    dict_offsets_.push_back(offset);
    return Status::OK();
  }

  Status AppendSet(Py_ssize_t size) {
    int32_t offset;
    RETURN_NOT_OK(internal::CastSize(set_offsets_.back() + size, &offset));
    RETURN_NOT_OK(Update(set_offsets_.size() - 1, &set_tag_));
    set_offsets_.push_back(offset);
    return Status::OK();
  }

  template <typename BuilderType>
  Status AddElement(const int8_t tag, BuilderType* out, const std::string& name = "") {
    if (tag != -1) {
      fields_[tag] = ::arrow::field(name, out->type());
      RETURN_NOT_OK(out->Finish(&children_[tag]));
      RETURN_NOT_OK(nones_.Append(true));
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
      RETURN_NOT_OK(builder.AppendValues(offsets.data(), offsets.size()));
      RETURN_NOT_OK(builder.Finish(&offset_array));
      std::shared_ptr<Array> list_array;
      RETURN_NOT_OK(ListArray::FromArrays(*offset_array, *data, pool_, &list_array));
      auto field = ::arrow::field(name, list_array->type());
      auto type = ::arrow::struct_({field});
      fields_[tag] = ::arrow::field("", type);
      children_[tag] = std::shared_ptr<StructArray>(
          new StructArray(type, list_array->length(), {list_array}));
      RETURN_NOT_OK(nones_.Append(true));
      type_ids_.push_back(tag);
    } else {
      DCHECK_EQ(offsets.size(), 1);
    }
    return Status::OK();
  }

  /// Finish building the sequence and return the result.
  /// Input arrays may be nullptr
  Status Finish(const Array* list_data, const Array* tuple_data, const Array* dict_data,
                const Array* set_data, std::shared_ptr<Array>* out) {
    fields_.resize(num_tags_);
    children_.resize(num_tags_);

    RETURN_NOT_OK(AddElement(bool_tag_, &bools_));
    RETURN_NOT_OK(AddElement(int_tag_, &ints_));
    RETURN_NOT_OK(AddElement(py2_int_tag_, &py2_ints_, "py2_int"));
    RETURN_NOT_OK(AddElement(string_tag_, &strings_));
    RETURN_NOT_OK(AddElement(bytes_tag_, &bytes_));
    RETURN_NOT_OK(AddElement(half_float_tag_, &half_floats_));
    RETURN_NOT_OK(AddElement(float_tag_, &floats_));
    RETURN_NOT_OK(AddElement(double_tag_, &doubles_));
    RETURN_NOT_OK(AddElement(date64_tag_, &date64s_));
    RETURN_NOT_OK(AddElement(tensor_tag_, &tensor_indices_, "tensor"));
    RETURN_NOT_OK(AddElement(buffer_tag_, &buffer_indices_, "buffer"));

    RETURN_NOT_OK(AddSubsequence(list_tag_, list_data, list_offsets_, "list"));
    RETURN_NOT_OK(AddSubsequence(tuple_tag_, tuple_data, tuple_offsets_, "tuple"));
    RETURN_NOT_OK(AddSubsequence(dict_tag_, dict_data, dict_offsets_, "dict"));
    RETURN_NOT_OK(AddSubsequence(set_tag_, set_data, set_offsets_, "set"));

    std::shared_ptr<Array> types_array;
    RETURN_NOT_OK(types_.Finish(&types_array));
    const auto& types = checked_cast<const Int8Array&>(*types_array);

    std::shared_ptr<Array> offsets_array;
    RETURN_NOT_OK(offsets_.Finish(&offsets_array));
    const auto& offsets = checked_cast<const Int32Array&>(*offsets_array);

    std::shared_ptr<Array> nones_array;
    RETURN_NOT_OK(nones_.Finish(&nones_array));
    const auto& nones = checked_cast<const BooleanArray&>(*nones_array);

    auto type = ::arrow::union_(fields_, type_ids_, UnionMode::DENSE);
    out->reset(new UnionArray(type, types.length(), children_, types.values(),
                              offsets.values(), nones.null_bitmap(), nones.null_count()));
    return Status::OK();
  }

 private:
  MemoryPool* pool_;

  Int8Builder types_;
  Int32Builder offsets_;

  BooleanBuilder nones_;
  BooleanBuilder bools_;
  Int64Builder ints_;
  Int64Builder py2_ints_;
  BinaryBuilder bytes_;
  StringBuilder strings_;
  HalfFloatBuilder half_floats_;
  FloatBuilder floats_;
  DoubleBuilder doubles_;
  Date64Builder date64s_;

  Int32Builder tensor_indices_;
  Int32Builder buffer_indices_;

  std::vector<int32_t> list_offsets_;
  std::vector<int32_t> tuple_offsets_;
  std::vector<int32_t> dict_offsets_;
  std::vector<int32_t> set_offsets_;

  // Tags for members of the sequence. If they are set to -1 it means
  // they are not used and will not part be of the metadata when we call
  // SequenceBuilder::Finish. If a member with one of the tags is added,
  // the associated variable gets a unique index starting from 0. This
  // happens in the UPDATE macro in sequence.cc.
  int8_t bool_tag_ = -1;
  int8_t int_tag_ = -1;
  int8_t py2_int_tag_ = -1;
  int8_t string_tag_ = -1;
  int8_t bytes_tag_ = -1;
  int8_t half_float_tag_ = -1;
  int8_t float_tag_ = -1;
  int8_t double_tag_ = -1;
  int8_t date64_tag_ = -1;

  int8_t tensor_tag_ = -1;
  int8_t buffer_tag_ = -1;
  int8_t list_tag_ = -1;
  int8_t tuple_tag_ = -1;
  int8_t dict_tag_ = -1;
  int8_t set_tag_ = -1;

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
  /// \param val_list_data
  ///    List containing the data from nested lists in the value
  ///   list of the dictionary
  ///
  /// \param val_dict_data
  ///   List containing the data from nested dictionaries in the
  ///   value list of the dictionary
  Status Finish(const Array* key_tuple_data, const Array* key_dict_data,
                const Array* val_list_data, const Array* val_tuple_data,
                const Array* val_dict_data, const Array* val_set_data,
                std::shared_ptr<Array>* out) {
    // lists and sets can't be keys of dicts in Python, that is why for
    // the keys we do not need to collect sublists
    std::shared_ptr<Array> keys, vals;
    RETURN_NOT_OK(keys_.Finish(nullptr, key_tuple_data, key_dict_data, nullptr, &keys));
    RETURN_NOT_OK(
        vals_.Finish(val_list_data, val_tuple_data, val_dict_data, val_set_data, &vals));
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
    ss << "error while calling callback on " << internal::PyObject_StdStringRepr(elem)
       << ": handler not registered";
    return Status::SerializationError(ss.str());
  } else {
    *result = PyObject_CallMethodObjArgs(context, method_name, elem, NULL);
    return PassPyError();
  }
  return Status::OK();
}

Status CallSerializeCallback(PyObject* context, PyObject* value,
                             PyObject** serialized_object) {
  OwnedRef method_name(PyUnicode_FromString("_serialize_callback"));
  RETURN_NOT_OK(CallCustomCallback(context, method_name.obj(), value, serialized_object));
  if (!PyDict_Check(*serialized_object)) {
    return Status::TypeError("serialization callback must return a valid dictionary");
  }
  return Status::OK();
}

Status CallDeserializeCallback(PyObject* context, PyObject* value,
                               PyObject** deserialized_object) {
  OwnedRef method_name(PyUnicode_FromString("_deserialize_callback"));
  return CallCustomCallback(context, method_name.obj(), value, deserialized_object);
}

Status SerializeDict(PyObject* context, std::vector<PyObject*> dicts,
                     int32_t recursion_depth, std::shared_ptr<Array>* out,
                     SerializedPyObject* blobs_out);

Status SerializeArray(PyObject* context, PyArrayObject* array, SequenceBuilder* builder,
                      std::vector<PyObject*>* subdicts, SerializedPyObject* blobs_out);

Status SerializeSequences(PyObject* context, std::vector<PyObject*> sequences,
                          int32_t recursion_depth, std::shared_ptr<Array>* out,
                          SerializedPyObject* blobs_out);

template <typename NumpyScalarObject>
Status AppendIntegerScalar(PyObject* obj, SequenceBuilder* builder) {
  int64_t value = reinterpret_cast<NumpyScalarObject*>(obj)->obval;
  return builder->AppendInt64(value);
}

// Append a potentially 64-bit wide unsigned Numpy scalar.
// Must check for overflow as we reinterpret it as signed int64.
template <typename NumpyScalarObject>
Status AppendLargeUnsignedScalar(PyObject* obj, SequenceBuilder* builder) {
  constexpr uint64_t max_value = std::numeric_limits<int64_t>::max();

  uint64_t value = reinterpret_cast<NumpyScalarObject*>(obj)->obval;
  if (value > max_value) {
    return Status::Invalid("cannot serialize Numpy uint64 scalar >= 2**63");
  }
  return builder->AppendInt64(static_cast<int64_t>(value));
}

Status AppendScalar(PyObject* obj, SequenceBuilder* builder) {
  if (PyArray_IsScalar(obj, Bool)) {
    return builder->AppendBool(reinterpret_cast<PyBoolScalarObject*>(obj)->obval != 0);
  } else if (PyArray_IsScalar(obj, Half)) {
    return builder->AppendHalfFloat(reinterpret_cast<PyHalfScalarObject*>(obj)->obval);
  } else if (PyArray_IsScalar(obj, Float)) {
    return builder->AppendFloat(reinterpret_cast<PyFloatScalarObject*>(obj)->obval);
  } else if (PyArray_IsScalar(obj, Double)) {
    return builder->AppendDouble(reinterpret_cast<PyDoubleScalarObject*>(obj)->obval);
  }
  if (PyArray_IsScalar(obj, Byte)) {
    return AppendIntegerScalar<PyByteScalarObject>(obj, builder);
  } else if (PyArray_IsScalar(obj, Short)) {
    return AppendIntegerScalar<PyShortScalarObject>(obj, builder);
  } else if (PyArray_IsScalar(obj, Int)) {
    return AppendIntegerScalar<PyIntScalarObject>(obj, builder);
  } else if (PyArray_IsScalar(obj, Long)) {
    return AppendIntegerScalar<PyLongScalarObject>(obj, builder);
  } else if (PyArray_IsScalar(obj, LongLong)) {
    return AppendIntegerScalar<PyLongLongScalarObject>(obj, builder);
  } else if (PyArray_IsScalar(obj, Int64)) {
    return AppendIntegerScalar<PyInt64ScalarObject>(obj, builder);
  } else if (PyArray_IsScalar(obj, UByte)) {
    return AppendIntegerScalar<PyUByteScalarObject>(obj, builder);
  } else if (PyArray_IsScalar(obj, UShort)) {
    return AppendIntegerScalar<PyUShortScalarObject>(obj, builder);
  } else if (PyArray_IsScalar(obj, UInt)) {
    return AppendIntegerScalar<PyUIntScalarObject>(obj, builder);
  } else if (PyArray_IsScalar(obj, ULong)) {
    return AppendLargeUnsignedScalar<PyULongScalarObject>(obj, builder);
  } else if (PyArray_IsScalar(obj, ULongLong)) {
    return AppendLargeUnsignedScalar<PyULongLongScalarObject>(obj, builder);
  } else if (PyArray_IsScalar(obj, UInt64)) {
    return AppendLargeUnsignedScalar<PyUInt64ScalarObject>(obj, builder);
  }
  return Status::NotImplemented("Numpy scalar type not recognized");
}

Status Append(PyObject* context, PyObject* elem, SequenceBuilder* builder,
              std::vector<PyObject*>* sublists, std::vector<PyObject*>* subtuples,
              std::vector<PyObject*>* subdicts, std::vector<PyObject*>* subsets,
              SerializedPyObject* blobs_out) {
  // The bool case must precede the int case (PyInt_Check passes for bools)
  if (PyBool_Check(elem)) {
    RETURN_NOT_OK(builder->AppendBool(elem == Py_True));
  } else if (PyArray_DescrFromScalar(elem)->type_num == NPY_HALF) {
    npy_half halffloat = reinterpret_cast<PyHalfScalarObject*>(elem)->obval;
    RETURN_NOT_OK(builder->AppendHalfFloat(halffloat));
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
    RETURN_NOT_OK(builder->AppendPy2Int64(static_cast<int64_t>(PyInt_AS_LONG(elem))));
#endif
  } else if (PyBytes_Check(elem)) {
    auto data = reinterpret_cast<uint8_t*>(PyBytes_AS_STRING(elem));
    int32_t size;
    RETURN_NOT_OK(internal::CastSize(PyBytes_GET_SIZE(elem), &size));
    RETURN_NOT_OK(builder->AppendBytes(data, size));
  } else if (PyUnicode_Check(elem)) {
    PyBytesView view;
    RETURN_NOT_OK(view.FromString(elem));
    int32_t size;
    RETURN_NOT_OK(internal::CastSize(view.size, &size));
    RETURN_NOT_OK(builder->AppendString(view.bytes, size));
  } else if (PyList_CheckExact(elem)) {
    RETURN_NOT_OK(builder->AppendList(PyList_Size(elem)));
    sublists->push_back(elem);
  } else if (PyDict_CheckExact(elem)) {
    RETURN_NOT_OK(builder->AppendDict(PyDict_Size(elem)));
    subdicts->push_back(elem);
  } else if (PyTuple_CheckExact(elem)) {
    RETURN_NOT_OK(builder->AppendTuple(PyTuple_Size(elem)));
    subtuples->push_back(elem);
  } else if (PySet_Check(elem)) {
    RETURN_NOT_OK(builder->AppendSet(PySet_Size(elem)));
    subsets->push_back(elem);
  } else if (PyArray_IsScalar(elem, Generic)) {
    RETURN_NOT_OK(AppendScalar(elem, builder));
  } else if (PyArray_CheckExact(elem)) {
    RETURN_NOT_OK(SerializeArray(context, reinterpret_cast<PyArrayObject*>(elem), builder,
                                 subdicts, blobs_out));
  } else if (elem == Py_None) {
    RETURN_NOT_OK(builder->AppendNone());
  } else if (PyDateTime_Check(elem)) {
    PyDateTime_DateTime* datetime = reinterpret_cast<PyDateTime_DateTime*>(elem);
    RETURN_NOT_OK(builder->AppendDate64(PyDateTime_to_us(datetime)));
  } else if (is_buffer(elem)) {
    RETURN_NOT_OK(builder->AppendBuffer(static_cast<int32_t>(blobs_out->buffers.size())));
    std::shared_ptr<Buffer> buffer;
    RETURN_NOT_OK(unwrap_buffer(elem, &buffer));
    blobs_out->buffers.push_back(buffer);
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
                      std::vector<PyObject*>* subdicts, SerializedPyObject* blobs_out) {
  int dtype = PyArray_TYPE(array);
  switch (dtype) {
    case NPY_UINT8:
    case NPY_INT8:
    case NPY_UINT16:
    case NPY_INT16:
    case NPY_UINT32:
    case NPY_INT32:
    case NPY_UINT64:
    case NPY_INT64:
    case NPY_HALF:
    case NPY_FLOAT:
    case NPY_DOUBLE: {
      RETURN_NOT_OK(
          builder->AppendTensor(static_cast<int32_t>(blobs_out->tensors.size())));
      std::shared_ptr<Tensor> tensor;
      RETURN_NOT_OK(NdarrayToTensor(default_memory_pool(),
                                    reinterpret_cast<PyObject*>(array), &tensor));
      blobs_out->tensors.push_back(tensor);
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
                          SerializedPyObject* blobs_out) {
  DCHECK(out);
  if (recursion_depth >= kMaxRecursionDepth) {
    return Status::NotImplemented(
        "This object exceeds the maximum recursion depth. It may contain itself "
        "recursively.");
  }
  SequenceBuilder builder;
  std::vector<PyObject*> sublists, subtuples, subdicts, subsets;
  for (const auto& sequence : sequences) {
    RETURN_NOT_OK(internal::VisitIterable(
        sequence, [&](PyObject* obj, bool* keep_going /* unused */) {
          return Append(context, obj, &builder, &sublists, &subtuples, &subdicts,
                        &subsets, blobs_out);
        }));
  }
  std::shared_ptr<Array> list;
  if (sublists.size() > 0) {
    RETURN_NOT_OK(
        SerializeSequences(context, sublists, recursion_depth + 1, &list, blobs_out));
  }
  std::shared_ptr<Array> tuple;
  if (subtuples.size() > 0) {
    RETURN_NOT_OK(
        SerializeSequences(context, subtuples, recursion_depth + 1, &tuple, blobs_out));
  }
  std::shared_ptr<Array> dict;
  if (subdicts.size() > 0) {
    RETURN_NOT_OK(
        SerializeDict(context, subdicts, recursion_depth + 1, &dict, blobs_out));
  }
  std::shared_ptr<Array> set;
  if (subsets.size() > 0) {
    RETURN_NOT_OK(
        SerializeSequences(context, subsets, recursion_depth + 1, &set, blobs_out));
  }
  return builder.Finish(list.get(), tuple.get(), dict.get(), set.get(), out);
}

Status SerializeDict(PyObject* context, std::vector<PyObject*> dicts,
                     int32_t recursion_depth, std::shared_ptr<Array>* out,
                     SerializedPyObject* blobs_out) {
  DictBuilder result;
  if (recursion_depth >= kMaxRecursionDepth) {
    return Status::NotImplemented(
        "This object exceeds the maximum recursion depth. It may contain itself "
        "recursively.");
  }
  std::vector<PyObject*> key_tuples, key_dicts, val_lists, val_tuples, val_dicts,
      val_sets, dummy;
  for (const auto& dict : dicts) {
    PyObject* key;
    PyObject* value;
    Py_ssize_t pos = 0;
    while (PyDict_Next(dict, &pos, &key, &value)) {
      RETURN_NOT_OK(Append(context, key, &result.keys(), &dummy, &key_tuples, &key_dicts,
                           &dummy, blobs_out));
      DCHECK_EQ(dummy.size(), 0);
      RETURN_NOT_OK(Append(context, value, &result.vals(), &val_lists, &val_tuples,
                           &val_dicts, &val_sets, blobs_out));
    }
  }
  std::shared_ptr<Array> key_tuples_arr;
  if (key_tuples.size() > 0) {
    RETURN_NOT_OK(SerializeSequences(context, key_tuples, recursion_depth + 1,
                                     &key_tuples_arr, blobs_out));
  }
  std::shared_ptr<Array> key_dicts_arr;
  if (key_dicts.size() > 0) {
    RETURN_NOT_OK(SerializeDict(context, key_dicts, recursion_depth + 1, &key_dicts_arr,
                                blobs_out));
  }
  std::shared_ptr<Array> val_list_arr;
  if (val_lists.size() > 0) {
    RETURN_NOT_OK(SerializeSequences(context, val_lists, recursion_depth + 1,
                                     &val_list_arr, blobs_out));
  }
  std::shared_ptr<Array> val_tuples_arr;
  if (val_tuples.size() > 0) {
    RETURN_NOT_OK(SerializeSequences(context, val_tuples, recursion_depth + 1,
                                     &val_tuples_arr, blobs_out));
  }
  std::shared_ptr<Array> val_dict_arr;
  if (val_dicts.size() > 0) {
    RETURN_NOT_OK(
        SerializeDict(context, val_dicts, recursion_depth + 1, &val_dict_arr, blobs_out));
  }
  std::shared_ptr<Array> val_set_arr;
  if (val_sets.size() > 0) {
    RETURN_NOT_OK(SerializeSequences(context, val_sets, recursion_depth + 1, &val_set_arr,
                                     blobs_out));
  }
  RETURN_NOT_OK(result.Finish(key_tuples_arr.get(), key_dicts_arr.get(),
                              val_list_arr.get(), val_tuples_arr.get(),
                              val_dict_arr.get(), val_set_arr.get(), out));

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
  return RecordBatch::Make(schema, data->length(), {data});
}

Status SerializeObject(PyObject* context, PyObject* sequence, SerializedPyObject* out) {
  PyAcquireGIL lock;
  PyDateTime_IMPORT;
  import_pyarrow();
  std::vector<PyObject*> sequences = {sequence};
  std::shared_ptr<Array> array;
  RETURN_NOT_OK(SerializeSequences(context, sequences, 0, &array, out));
  out->batch = MakeBatch(array);
  return Status::OK();
}

Status SerializeTensor(std::shared_ptr<Tensor> tensor, SerializedPyObject* out) {
  std::shared_ptr<Array> array;
  SequenceBuilder builder;
  RETURN_NOT_OK(builder.AppendTensor(static_cast<int32_t>(out->tensors.size())));
  out->tensors.push_back(tensor);
  RETURN_NOT_OK(builder.Finish(nullptr, nullptr, nullptr, nullptr, &array));
  out->batch = MakeBatch(array);
  return Status::OK();
}

Status WriteTensorHeader(std::shared_ptr<DataType> dtype,
                         const std::vector<int64_t>& shape, int64_t tensor_num_bytes,
                         io::OutputStream* dst) {
  auto empty_tensor = std::make_shared<Tensor>(
      dtype, std::make_shared<Buffer>(nullptr, tensor_num_bytes), shape);
  SerializedPyObject serialized_tensor;
  RETURN_NOT_OK(SerializeTensor(empty_tensor, &serialized_tensor));
  return serialized_tensor.WriteTo(dst);
}

Status SerializedPyObject::WriteTo(io::OutputStream* dst) {
  int32_t num_tensors = static_cast<int32_t>(this->tensors.size());
  int32_t num_buffers = static_cast<int32_t>(this->buffers.size());
  RETURN_NOT_OK(
      dst->Write(reinterpret_cast<const uint8_t*>(&num_tensors), sizeof(int32_t)));
  RETURN_NOT_OK(
      dst->Write(reinterpret_cast<const uint8_t*>(&num_buffers), sizeof(int32_t)));
  RETURN_NOT_OK(ipc::WriteRecordBatchStream({this->batch}, dst));

  // Align stream to 64-byte offset so tensor bodies are 64-byte aligned
  RETURN_NOT_OK(ipc::AlignStream(dst, ipc::kTensorAlignment));

  int32_t metadata_length;
  int64_t body_length;
  for (const auto& tensor : this->tensors) {
    RETURN_NOT_OK(ipc::WriteTensor(*tensor, dst, &metadata_length, &body_length));
    RETURN_NOT_OK(ipc::AlignStream(dst, ipc::kTensorAlignment));
  }

  for (const auto& buffer : this->buffers) {
    int64_t size = buffer->size();
    RETURN_NOT_OK(dst->Write(reinterpret_cast<const uint8_t*>(&size), sizeof(int64_t)));
    RETURN_NOT_OK(dst->Write(buffer->data(), size));
  }

  return Status::OK();
}

Status SerializedPyObject::GetComponents(MemoryPool* memory_pool, PyObject** out) {
  PyAcquireGIL py_gil;

  OwnedRef result(PyDict_New());
  PyObject* buffers = PyList_New(0);

  // TODO(wesm): Not sure how pedantic we need to be about checking the return
  // values of these functions. There are other places where we do not check
  // PyDict_SetItem/SetItemString return value, but these failures would be
  // quite esoteric
  PyDict_SetItemString(result.obj(), "num_tensors",
                       PyLong_FromSize_t(this->tensors.size()));
  PyDict_SetItemString(result.obj(), "num_buffers",
                       PyLong_FromSize_t(this->buffers.size()));
  PyDict_SetItemString(result.obj(), "data", buffers);
  RETURN_IF_PYERROR();

  Py_DECREF(buffers);

  auto PushBuffer = [&buffers](const std::shared_ptr<Buffer>& buffer) {
    PyObject* wrapped_buffer = wrap_buffer(buffer);
    RETURN_IF_PYERROR();
    if (PyList_Append(buffers, wrapped_buffer) < 0) {
      Py_DECREF(wrapped_buffer);
      RETURN_IF_PYERROR();
    }
    Py_DECREF(wrapped_buffer);
    return Status::OK();
  };

  constexpr int64_t kInitialCapacity = 1024;

  // Write the record batch describing the object structure
  std::shared_ptr<io::BufferOutputStream> stream;
  std::shared_ptr<Buffer> buffer;

  py_gil.release();
  RETURN_NOT_OK(io::BufferOutputStream::Create(kInitialCapacity, memory_pool, &stream));
  RETURN_NOT_OK(ipc::WriteRecordBatchStream({this->batch}, stream.get()));
  RETURN_NOT_OK(stream->Finish(&buffer));
  py_gil.acquire();

  RETURN_NOT_OK(PushBuffer(buffer));

  // For each tensor, get a metadata buffer and a buffer for the body
  for (const auto& tensor : this->tensors) {
    std::unique_ptr<ipc::Message> message;
    RETURN_NOT_OK(ipc::GetTensorMessage(*tensor, memory_pool, &message));
    RETURN_NOT_OK(PushBuffer(message->metadata()));
    RETURN_NOT_OK(PushBuffer(message->body()));
  }

  for (const auto& buf : this->buffers) {
    RETURN_NOT_OK(PushBuffer(buf));
  }

  *out = result.detach();
  return Status::OK();
}

}  // namespace py
}  // namespace arrow

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
#include "arrow/array/builder_union.h"
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

using internal::checked_cast;

namespace py {

class SequenceBuilder;
class DictBuilder;

Status Append(PyObject* context, PyObject* elem, SequenceBuilder* builder,
              int32_t recursion_depth, SerializedPyObject* blobs_out);

// A Sequence is a heterogeneous collections of elements. It can contain
// scalar Python types, lists, tuples, dictionaries and tensors.
class SequenceBuilder {
 public:
  explicit SequenceBuilder(MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT)
      : pool_(pool),
        types_(::arrow::int8(), pool),
        offsets_(::arrow::int32(), pool),
        type_map_(PythonType::NUM_PYTHON_TYPES, -1) {
    builder_.reset(new DenseUnionBuilder(pool));
  }

  // Appending a none to the sequence
  Status AppendNone() { return builder_->AppendNull(); }

  template <typename BuilderType>
  Status Update(BuilderType* child_builder, int8_t tag) {
    int32_t offset32 = -1;
    RETURN_NOT_OK(internal::CastSize(child_builder->length(), &offset32));
    DCHECK_GE(offset32, 0);
    return builder_->Append(tag, offset32);
  }

  template <typename BuilderType, typename MakeBuilderFn>
  Status CreateAndUpdate(std::shared_ptr<BuilderType>* child_builder, int8_t tag,
                         MakeBuilderFn make_builder) {
    if (!*child_builder) {
      child_builder->reset(make_builder());
      // std::to_string is locale dependent, but should be ok for small integers
      type_map_[tag] = builder_->AppendChild(*child_builder, std::to_string(tag));
    }
    return Update(child_builder->get(), type_map_[tag]);
  }

  template <typename BuilderType, typename T>
  Status AppendPrimitive(std::shared_ptr<BuilderType>* child_builder, const T val,
                         int8_t tag) {
    RETURN_NOT_OK(
        CreateAndUpdate(child_builder, tag, [this]() { return new BuilderType(pool_); }));
    return (*child_builder)->Append(val);
  }

  // Appending a boolean to the sequence
  Status AppendBool(const bool data) {
    return AppendPrimitive(&bools_, data, PythonType::BOOL);
  }

  // Appending a python 2 int64_t to the sequence
  Status AppendPy2Int64(const int64_t data) {
    return AppendPrimitive(&py2_ints_, data, PythonType::PY2INT);
  }

  // Appending an int64_t to the sequence
  Status AppendInt64(const int64_t data) {
    return AppendPrimitive(&ints_, data, PythonType::INT);
  }

  // Append a list of bytes to the sequence
  Status AppendBytes(const uint8_t* data, int32_t length) {
    RETURN_NOT_OK(CreateAndUpdate(&bytes_, PythonType::BYTES,
                                  [this]() { return new BinaryBuilder(pool_); }));
    return bytes_->Append(data, length);
  }

  // Appending a string to the sequence
  Status AppendString(const char* data, int32_t length) {
    RETURN_NOT_OK(CreateAndUpdate(&strings_, PythonType::STRING,
                                  [this]() { return new StringBuilder(pool_); }));
    return strings_->Append(data, length);
  }

  // Appending a half_float to the sequence
  Status AppendHalfFloat(const npy_half data) {
    return AppendPrimitive(&half_floats_, data, PythonType::HALF_FLOAT);
  }

  // Appending a float to the sequence
  Status AppendFloat(const float data) {
    return AppendPrimitive(&floats_, data, PythonType::FLOAT);
  }

  // Appending a double to the sequence
  Status AppendDouble(const double data) {
    return AppendPrimitive(&doubles_, data, PythonType::DOUBLE);
  }

  // Appending a Date64 timestamp to the sequence
  Status AppendDate64(const int64_t timestamp) {
    return AppendPrimitive(&date64s_, timestamp, PythonType::DATE64);
  }

  // Appending a tensor to the sequence
  //
  // \param tensor_index Index of the tensor in the object.
  Status AppendTensor(const int32_t tensor_index) {
    RETURN_NOT_OK(CreateAndUpdate(&tensor_indices_, PythonType::TENSOR,
                                  [this]() { return new Int32Builder(pool_); }));
    return tensor_indices_->Append(tensor_index);
  }

  // Appending a numpy ndarray to the sequence
  //
  // \param tensor_index Index of the tensor in the object.
  Status AppendNdarray(const int32_t ndarray_index) {
    RETURN_NOT_OK(CreateAndUpdate(&ndarray_indices_, PythonType::NDARRAY,
                                  [this]() { return new Int32Builder(pool_); }));
    return ndarray_indices_->Append(ndarray_index);
  }

  // Appending a buffer to the sequence
  //
  // \param buffer_index Indes of the buffer in the object.
  Status AppendBuffer(const int32_t buffer_index) {
    RETURN_NOT_OK(CreateAndUpdate(&buffer_indices_, PythonType::BUFFER,
                                  [this]() { return new Int32Builder(pool_); }));
    return buffer_indices_->Append(buffer_index);
  }

  Status AppendSequence(PyObject* context, PyObject* sequence, int8_t tag,
                        std::shared_ptr<ListBuilder>& target_sequence,
                        std::unique_ptr<SequenceBuilder>& values, int32_t recursion_depth,
                        SerializedPyObject* blobs_out) {
    if (recursion_depth >= kMaxRecursionDepth) {
      return Status::NotImplemented(
          "This object exceeds the maximum recursion depth. It may contain itself "
          "recursively.");
    }
    RETURN_NOT_OK(CreateAndUpdate(&target_sequence, tag, [this, &values]() {
      values.reset(new SequenceBuilder(pool_));
      return new ListBuilder(pool_, values->builder());
    }));
    RETURN_NOT_OK(target_sequence->Append());
    return internal::VisitIterable(
        sequence, [&](PyObject* obj, bool* keep_going /* unused */) {
          return Append(context, obj, values.get(), recursion_depth, blobs_out);
        });
  }

  Status AppendList(PyObject* context, PyObject* list, int32_t recursion_depth,
                    SerializedPyObject* blobs_out) {
    return AppendSequence(context, list, PythonType::LIST, lists_, list_values_,
                          recursion_depth, blobs_out);
  }

  Status AppendTuple(PyObject* context, PyObject* tuple, int32_t recursion_depth,
                     SerializedPyObject* blobs_out) {
    return AppendSequence(context, tuple, PythonType::TUPLE, tuples_, tuple_values_,
                          recursion_depth, blobs_out);
  }

  Status AppendSet(PyObject* context, PyObject* set, int32_t recursion_depth,
                   SerializedPyObject* blobs_out) {
    return AppendSequence(context, set, PythonType::SET, sets_, set_values_,
                          recursion_depth, blobs_out);
  }

  Status AppendDict(PyObject* context, PyObject* dict, int32_t recursion_depth,
                    SerializedPyObject* blobs_out);

  // Finish building the sequence and return the result.
  // Input arrays may be nullptr
  Status Finish(std::shared_ptr<Array>* out) { return builder_->Finish(out); }

  std::shared_ptr<DenseUnionBuilder> builder() { return builder_; }

 private:
  MemoryPool* pool_;

  Int8Builder types_;
  Int32Builder offsets_;

  /// Mapping from PythonType to child index
  std::vector<int8_t> type_map_;

  std::shared_ptr<BooleanBuilder> bools_;
  std::shared_ptr<Int64Builder> ints_;
  std::shared_ptr<Int64Builder> py2_ints_;
  std::shared_ptr<BinaryBuilder> bytes_;
  std::shared_ptr<StringBuilder> strings_;
  std::shared_ptr<HalfFloatBuilder> half_floats_;
  std::shared_ptr<FloatBuilder> floats_;
  std::shared_ptr<DoubleBuilder> doubles_;
  std::shared_ptr<Date64Builder> date64s_;

  std::unique_ptr<SequenceBuilder> list_values_;
  std::shared_ptr<ListBuilder> lists_;
  std::unique_ptr<DictBuilder> dict_values_;
  std::shared_ptr<ListBuilder> dicts_;
  std::unique_ptr<SequenceBuilder> tuple_values_;
  std::shared_ptr<ListBuilder> tuples_;
  std::unique_ptr<SequenceBuilder> set_values_;
  std::shared_ptr<ListBuilder> sets_;

  std::shared_ptr<Int32Builder> tensor_indices_;
  std::shared_ptr<Int32Builder> ndarray_indices_;
  std::shared_ptr<Int32Builder> buffer_indices_;

  std::shared_ptr<DenseUnionBuilder> builder_;
};

// Constructing dictionaries of key/value pairs. Sequences of
// keys and values are built separately using a pair of
// SequenceBuilders. The resulting Arrow representation
// can be obtained via the Finish method.
class DictBuilder {
 public:
  explicit DictBuilder(MemoryPool* pool = nullptr) : keys_(pool), vals_(pool) {
    builder_.reset(
        new StructBuilder(nullptr, pool, {keys_.builder(), vals_.builder()}));
  }

  // Builder for the keys of the dictionary
  SequenceBuilder& keys() { return keys_; }
  // Builder for the values of the dictionary
  SequenceBuilder& vals() { return vals_; }

  // Construct an Arrow StructArray representing the dictionary.
  // Contains a field "keys" for the keys and "vals" for the values.
  Status Finish(std::shared_ptr<Array>* out) { return builder_->Finish(out); }

  std::shared_ptr<StructBuilder> builder() { return builder_; }

 private:
  SequenceBuilder keys_;
  SequenceBuilder vals_;
  std::shared_ptr<StructBuilder> builder_;
};

Status SequenceBuilder::AppendDict(PyObject* context, PyObject* dict,
                                   int32_t recursion_depth,
                                   SerializedPyObject* blobs_out) {
  if (recursion_depth >= kMaxRecursionDepth) {
    return Status::NotImplemented(
        "This object exceeds the maximum recursion depth. It may contain itself "
        "recursively.");
  }
  RETURN_NOT_OK(CreateAndUpdate(&dicts_, PythonType::DICT, [this]() {
    dict_values_.reset(new DictBuilder(pool_));
    return new ListBuilder(pool_, dict_values_->builder());
  }));
  RETURN_NOT_OK(dicts_->Append());
  PyObject* key;
  PyObject* value;
  Py_ssize_t pos = 0;
  while (PyDict_Next(dict, &pos, &key, &value)) {
    RETURN_NOT_OK(dict_values_->builder()->Append());
    RETURN_NOT_OK(
        Append(context, key, &dict_values_->keys(), recursion_depth + 1, blobs_out));
    RETURN_NOT_OK(
        Append(context, value, &dict_values_->vals(), recursion_depth + 1, blobs_out));
  }

  // This block is used to decrement the reference counts of the results
  // returned by the serialization callback, which is called in AppendArray,
  // in DeserializeDict and in Append
  static PyObject* py_type = PyUnicode_FromString("_pytype_");
  if (PyDict_Contains(dict, py_type)) {
    // If the dictionary contains the key "_pytype_", then the user has to
    // have registered a callback.
    if (context == Py_None) {
      return Status::Invalid("No serialization callback set");
    }
    Py_XDECREF(dict);
  }
  return Status::OK();
}

Status CallCustomCallback(PyObject* context, PyObject* method_name, PyObject* elem,
                          PyObject** result) {
  *result = NULL;
  if (context == Py_None) {
    return Status::SerializationError("error while calling callback on ",
                                      internal::PyObject_StdStringRepr(elem),
                                      ": handler not registered");
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

Status AppendArray(PyObject* context, PyArrayObject* array, SequenceBuilder* builder,
                   int32_t recursion_depth, SerializedPyObject* blobs_out);

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
              int32_t recursion_depth, SerializedPyObject* blobs_out) {
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
      RETURN_NOT_OK(
          builder->AppendDict(context, serialized_object, recursion_depth, blobs_out));
    }
#if PY_MAJOR_VERSION < 3
  } else if (PyInt_Check(elem)) {
    RETURN_NOT_OK(builder->AppendPy2Int64(static_cast<int64_t>(PyInt_AS_LONG(elem))));
#endif
  } else if (PyBytes_Check(elem)) {
    auto data = reinterpret_cast<uint8_t*>(PyBytes_AS_STRING(elem));
    int32_t size = -1;
    RETURN_NOT_OK(internal::CastSize(PyBytes_GET_SIZE(elem), &size));
    RETURN_NOT_OK(builder->AppendBytes(data, size));
  } else if (PyUnicode_Check(elem)) {
    PyBytesView view;
    RETURN_NOT_OK(view.FromString(elem));
    int32_t size = -1;
    RETURN_NOT_OK(internal::CastSize(view.size, &size));
    RETURN_NOT_OK(builder->AppendString(view.bytes, size));
  } else if (PyList_CheckExact(elem)) {
    RETURN_NOT_OK(builder->AppendList(context, elem, recursion_depth, blobs_out));
  } else if (PyDict_CheckExact(elem)) {
    RETURN_NOT_OK(builder->AppendDict(context, elem, recursion_depth, blobs_out));
  } else if (PyTuple_CheckExact(elem)) {
    RETURN_NOT_OK(builder->AppendTuple(context, elem, recursion_depth, blobs_out));
  } else if (PySet_Check(elem)) {
    RETURN_NOT_OK(builder->AppendSet(context, elem, recursion_depth, blobs_out));
  } else if (PyArray_IsScalar(elem, Generic)) {
    RETURN_NOT_OK(AppendScalar(elem, builder));
  } else if (PyArray_CheckExact(elem)) {
    RETURN_NOT_OK(AppendArray(context, reinterpret_cast<PyArrayObject*>(elem), builder,
                              recursion_depth, blobs_out));
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
  } else if (is_tensor(elem)) {
    RETURN_NOT_OK(builder->AppendTensor(static_cast<int32_t>(blobs_out->tensors.size())));
    std::shared_ptr<Tensor> tensor;
    RETURN_NOT_OK(unwrap_tensor(elem, &tensor));
    blobs_out->tensors.push_back(tensor);
  } else {
    // Attempt to serialize the object using the custom callback.
    PyObject* serialized_object;
    // The reference count of serialized_object will be decremented in SerializeDict
    RETURN_NOT_OK(CallSerializeCallback(context, elem, &serialized_object));
    RETURN_NOT_OK(
        builder->AppendDict(context, serialized_object, recursion_depth, blobs_out));
  }
  return Status::OK();
}

Status AppendArray(PyObject* context, PyArrayObject* array, SequenceBuilder* builder,
                   int32_t recursion_depth, SerializedPyObject* blobs_out) {
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
          builder->AppendNdarray(static_cast<int32_t>(blobs_out->ndarrays.size())));
      std::shared_ptr<Tensor> tensor;
      RETURN_NOT_OK(NdarrayToTensor(default_memory_pool(),
                                    reinterpret_cast<PyObject*>(array), &tensor));
      blobs_out->ndarrays.push_back(tensor);
    } break;
    default: {
      PyObject* serialized_object;
      // The reference count of serialized_object will be decremented in SerializeDict
      RETURN_NOT_OK(CallSerializeCallback(context, reinterpret_cast<PyObject*>(array),
                                          &serialized_object));
      RETURN_NOT_OK(builder->AppendDict(context, serialized_object, recursion_depth + 1,
                                        blobs_out));
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
  SequenceBuilder builder;
  RETURN_NOT_OK(internal::VisitIterable(
      sequence, [&](PyObject* obj, bool* keep_going /* unused */) {
        return Append(context, obj, &builder, 0, out);
      }));
  std::shared_ptr<Array> array;
  RETURN_NOT_OK(builder.Finish(&array));
  out->batch = MakeBatch(array);
  return Status::OK();
}

Status SerializeNdarray(std::shared_ptr<Tensor> tensor, SerializedPyObject* out) {
  std::shared_ptr<Array> array;
  SequenceBuilder builder;
  RETURN_NOT_OK(builder.AppendNdarray(static_cast<int32_t>(out->ndarrays.size())));
  out->ndarrays.push_back(tensor);
  RETURN_NOT_OK(builder.Finish(&array));
  out->batch = MakeBatch(array);
  return Status::OK();
}

Status WriteNdarrayHeader(std::shared_ptr<DataType> dtype,
                          const std::vector<int64_t>& shape, int64_t tensor_num_bytes,
                          io::OutputStream* dst) {
  auto empty_tensor = std::make_shared<Tensor>(
      dtype, std::make_shared<Buffer>(nullptr, tensor_num_bytes), shape);
  SerializedPyObject serialized_tensor;
  RETURN_NOT_OK(SerializeNdarray(empty_tensor, &serialized_tensor));
  return serialized_tensor.WriteTo(dst);
}

Status SerializedPyObject::WriteTo(io::OutputStream* dst) {
  int32_t num_tensors = static_cast<int32_t>(this->tensors.size());
  int32_t num_ndarrays = static_cast<int32_t>(this->ndarrays.size());
  int32_t num_buffers = static_cast<int32_t>(this->buffers.size());
  RETURN_NOT_OK(
      dst->Write(reinterpret_cast<const uint8_t*>(&num_tensors), sizeof(int32_t)));
  RETURN_NOT_OK(
      dst->Write(reinterpret_cast<const uint8_t*>(&num_ndarrays), sizeof(int32_t)));
  RETURN_NOT_OK(
      dst->Write(reinterpret_cast<const uint8_t*>(&num_buffers), sizeof(int32_t)));

  // Align stream to 8-byte offset
  RETURN_NOT_OK(ipc::AlignStream(dst, ipc::kArrowIpcAlignment));
  RETURN_NOT_OK(ipc::WriteRecordBatchStream({this->batch}, dst));

  // Align stream to 64-byte offset so tensor bodies are 64-byte aligned
  RETURN_NOT_OK(ipc::AlignStream(dst, ipc::kTensorAlignment));

  int32_t metadata_length;
  int64_t body_length;
  for (const auto& tensor : this->tensors) {
    RETURN_NOT_OK(ipc::WriteTensor(*tensor, dst, &metadata_length, &body_length));
    RETURN_NOT_OK(ipc::AlignStream(dst, ipc::kTensorAlignment));
  }

  for (const auto& tensor : this->ndarrays) {
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
  PyDict_SetItemString(result.obj(), "num_ndarrays",
                       PyLong_FromSize_t(this->ndarrays.size()));
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

  // For each ndarray, get a metadata buffer and a buffer for the body
  for (const auto& ndarray : this->ndarrays) {
    std::unique_ptr<ipc::Message> message;
    RETURN_NOT_OK(ipc::GetTensorMessage(*ndarray, memory_pool, &message));
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

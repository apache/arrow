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

#include <Python.h>
#include <datetime.h>
#include <sstream>

#include "pyarrow/adapters/builtin.h"

#include "arrow/api.h"
#include "arrow/status.h"

#include "pyarrow/helpers.h"
#include "pyarrow/util/datetime.h"

using arrow::ArrayBuilder;
using arrow::DataType;
using arrow::Status;
using arrow::Type;

namespace pyarrow {

static inline bool IsPyInteger(PyObject* obj) {
#if PYARROW_IS_PY2
  return PyLong_Check(obj) || PyInt_Check(obj);
#else
  return PyLong_Check(obj);
#endif
}

class ScalarVisitor {
 public:
  ScalarVisitor()
      : total_count_(0),
        none_count_(0),
        bool_count_(0),
        int_count_(0),
        date_count_(0),
        timestamp_count_(0),
        float_count_(0),
        binary_count_(0),
        unicode_count_(0) {}

  void Visit(PyObject* obj) {
    ++total_count_;
    if (obj == Py_None) {
      ++none_count_;
    } else if (PyBool_Check(obj)) {
      ++bool_count_;
    } else if (PyFloat_Check(obj)) {
      ++float_count_;
    } else if (IsPyInteger(obj)) {
      ++int_count_;
    } else if (PyDate_CheckExact(obj)) {
      ++date_count_;
    } else if (PyDateTime_CheckExact(obj)) {
      ++timestamp_count_;
    } else if (PyBytes_Check(obj)) {
      ++binary_count_;
    } else if (PyUnicode_Check(obj)) {
      ++unicode_count_;
    } else {
      // TODO(wesm): accumulate error information somewhere
    }
  }

  std::shared_ptr<DataType> GetType() {
    // TODO(wesm): handling mixed-type cases
    if (float_count_) {
      return arrow::float64();
    } else if (int_count_) {
      // TODO(wesm): tighter type later
      return arrow::int64();
    } else if (date_count_) {
      return arrow::date();
    } else if (timestamp_count_) {
      return arrow::timestamp(arrow::TimeUnit::MICRO);
    } else if (bool_count_) {
      return arrow::boolean();
    } else if (binary_count_) {
      return arrow::binary();
    } else if (unicode_count_) {
      return arrow::utf8();
    } else {
      return arrow::null();
    }
  }

  int64_t total_count() const { return total_count_; }

 private:
  int64_t total_count_;
  int64_t none_count_;
  int64_t bool_count_;
  int64_t int_count_;
  int64_t date_count_;
  int64_t timestamp_count_;
  int64_t float_count_;
  int64_t binary_count_;
  int64_t unicode_count_;

  // Place to accumulate errors
  // std::vector<Status> errors_;
};

static constexpr int MAX_NESTING_LEVELS = 32;

class SeqVisitor {
 public:
  SeqVisitor() : max_nesting_level_(0) {
    memset(nesting_histogram_, 0, MAX_NESTING_LEVELS * sizeof(int));
  }

  Status Visit(PyObject* obj, int level = 0) {
    Py_ssize_t size = PySequence_Size(obj);

    if (level > max_nesting_level_) { max_nesting_level_ = level; }

    for (int64_t i = 0; i < size; ++i) {
      // TODO(wesm): Error checking?
      // TODO(wesm): Specialize for PyList_GET_ITEM?
      OwnedRef item_ref(PySequence_GetItem(obj, i));
      PyObject* item = item_ref.obj();

      if (PyList_Check(item)) {
        RETURN_NOT_OK(Visit(item, level + 1));
      } else if (PyDict_Check(item)) {
        return Status::NotImplemented("No type inference for dicts");
      } else {
        // We permit nulls at any level of nesting
        if (item == Py_None) {
          // TODO
        } else {
          ++nesting_histogram_[level];
          scalars_.Visit(item);
        }
      }
    }
    return Status::OK();
  }

  std::shared_ptr<DataType> GetType() {
    if (scalars_.total_count() == 0) {
      if (max_nesting_level_ == 0) {
        return arrow::null();
      } else {
        return nullptr;
      }
    } else {
      std::shared_ptr<DataType> result = scalars_.GetType();
      for (int i = 0; i < max_nesting_level_; ++i) {
        result = std::make_shared<arrow::ListType>(result);
      }
      return result;
    }
  }

  Status Validate() const {
    if (scalars_.total_count() > 0) {
      if (num_nesting_levels() > 1) {
        return Status::Invalid("Mixed nesting levels not supported");
      } else if (max_observed_level() < max_nesting_level_) {
        return Status::Invalid("Mixed nesting levels not supported");
      }
    }
    return Status::OK();
  }

  int max_observed_level() const {
    int result = 0;
    for (int i = 0; i < MAX_NESTING_LEVELS; ++i) {
      if (nesting_histogram_[i] > 0) { result = i; }
    }
    return result;
  }

  int num_nesting_levels() const {
    int result = 0;
    for (int i = 0; i < MAX_NESTING_LEVELS; ++i) {
      if (nesting_histogram_[i] > 0) { ++result; }
    }
    return result;
  }

 private:
  ScalarVisitor scalars_;

  // Track observed
  int max_nesting_level_;
  int nesting_histogram_[MAX_NESTING_LEVELS];
};

// Non-exhaustive type inference
static Status InferArrowType(
    PyObject* obj, int64_t* size, std::shared_ptr<DataType>* out_type) {
  *size = PySequence_Size(obj);
  if (PyErr_Occurred()) {
    // Not a sequence
    PyErr_Clear();
    return Status::TypeError("Object is not a sequence");
  }

  // For 0-length sequences, refuse to guess
  if (*size == 0) { *out_type = arrow::null(); }

  SeqVisitor seq_visitor;
  RETURN_NOT_OK(seq_visitor.Visit(obj));
  RETURN_NOT_OK(seq_visitor.Validate());

  *out_type = seq_visitor.GetType();

  if (*out_type == nullptr) { return Status::TypeError("Unable to determine data type"); }

  return Status::OK();
}

// Marshal Python sequence (list, tuple, etc.) to Arrow array
class SeqConverter {
 public:
  virtual Status Init(const std::shared_ptr<ArrayBuilder>& builder) {
    builder_ = builder;
    return Status::OK();
  }

  virtual Status AppendData(PyObject* seq) = 0;

 protected:
  std::shared_ptr<ArrayBuilder> builder_;
};

template <typename BuilderType>
class TypedConverter : public SeqConverter {
 public:
  Status Init(const std::shared_ptr<ArrayBuilder>& builder) override {
    builder_ = builder;
    typed_builder_ = static_cast<BuilderType*>(builder.get());
    return Status::OK();
  }

 protected:
  BuilderType* typed_builder_;
};

class BoolConverter : public TypedConverter<arrow::BooleanBuilder> {
 public:
  Status AppendData(PyObject* seq) override {
    Py_ssize_t size = PySequence_Size(seq);
    RETURN_NOT_OK(typed_builder_->Reserve(size));
    for (int64_t i = 0; i < size; ++i) {
      OwnedRef item(PySequence_GetItem(seq, i));
      if (item.obj() == Py_None) {
        typed_builder_->AppendNull();
      } else {
        if (item.obj() == Py_True) {
          typed_builder_->Append(true);
        } else {
          typed_builder_->Append(false);
        }
      }
    }
    return Status::OK();
  }
};

class Int64Converter : public TypedConverter<arrow::Int64Builder> {
 public:
  Status AppendData(PyObject* seq) override {
    int64_t val;
    Py_ssize_t size = PySequence_Size(seq);
    RETURN_NOT_OK(typed_builder_->Reserve(size));
    for (int64_t i = 0; i < size; ++i) {
      OwnedRef item(PySequence_GetItem(seq, i));
      if (item.obj() == Py_None) {
        typed_builder_->AppendNull();
      } else {
        val = PyLong_AsLongLong(item.obj());
        RETURN_IF_PYERROR();
        typed_builder_->Append(val);
      }
    }
    return Status::OK();
  }
};

class DateConverter : public TypedConverter<arrow::DateBuilder> {
 public:
  Status AppendData(PyObject* seq) override {
    Py_ssize_t size = PySequence_Size(seq);
    RETURN_NOT_OK(typed_builder_->Reserve(size));
    for (int64_t i = 0; i < size; ++i) {
      OwnedRef item(PySequence_GetItem(seq, i));
      if (item.obj() == Py_None) {
        typed_builder_->AppendNull();
      } else {
        PyDateTime_Date* pydate = reinterpret_cast<PyDateTime_Date*>(item.obj());
        typed_builder_->Append(PyDate_to_ms(pydate));
      }
    }
    return Status::OK();
  }
};

class TimestampConverter : public TypedConverter<arrow::TimestampBuilder> {
 public:
  Status AppendData(PyObject* seq) override {
    Py_ssize_t size = PySequence_Size(seq);
    RETURN_NOT_OK(typed_builder_->Reserve(size));
    for (int64_t i = 0; i < size; ++i) {
      OwnedRef item(PySequence_GetItem(seq, i));
      if (item.obj() == Py_None) {
        typed_builder_->AppendNull();
      } else {
        PyDateTime_DateTime* pydatetime =
            reinterpret_cast<PyDateTime_DateTime*>(item.obj());
        struct tm datetime = {0};
        datetime.tm_year = PyDateTime_GET_YEAR(pydatetime) - 1900;
        datetime.tm_mon = PyDateTime_GET_MONTH(pydatetime) - 1;
        datetime.tm_mday = PyDateTime_GET_DAY(pydatetime);
        datetime.tm_hour = PyDateTime_DATE_GET_HOUR(pydatetime);
        datetime.tm_min = PyDateTime_DATE_GET_MINUTE(pydatetime);
        datetime.tm_sec = PyDateTime_DATE_GET_SECOND(pydatetime);
        int us = PyDateTime_DATE_GET_MICROSECOND(pydatetime);
        RETURN_IF_PYERROR();
        struct tm epoch = {0};
        epoch.tm_year = 70;
        epoch.tm_mday = 1;
        // Microseconds since the epoch
        int64_t val = lrint(difftime(mktime(&datetime), mktime(&epoch))) * 1000000 + us;
        typed_builder_->Append(val);
      }
    }
    return Status::OK();
  }
};

class DoubleConverter : public TypedConverter<arrow::DoubleBuilder> {
 public:
  Status AppendData(PyObject* seq) override {
    double val;
    Py_ssize_t size = PySequence_Size(seq);
    RETURN_NOT_OK(typed_builder_->Reserve(size));
    for (int64_t i = 0; i < size; ++i) {
      OwnedRef item(PySequence_GetItem(seq, i));
      if (item.obj() == Py_None) {
        typed_builder_->AppendNull();
      } else {
        val = PyFloat_AsDouble(item.obj());
        RETURN_IF_PYERROR();
        typed_builder_->Append(val);
      }
    }
    return Status::OK();
  }
};

class BytesConverter : public TypedConverter<arrow::BinaryBuilder> {
 public:
  Status AppendData(PyObject* seq) override {
    PyObject* item;
    PyObject* bytes_obj;
    OwnedRef tmp;
    const char* bytes;
    int32_t length;
    Py_ssize_t size = PySequence_Size(seq);
    for (int64_t i = 0; i < size; ++i) {
      item = PySequence_GetItem(seq, i);
      OwnedRef holder(item);

      if (item == Py_None) {
        RETURN_NOT_OK(typed_builder_->AppendNull());
        continue;
      } else if (PyUnicode_Check(item)) {
        tmp.reset(PyUnicode_AsUTF8String(item));
        RETURN_IF_PYERROR();
        bytes_obj = tmp.obj();
      } else if (PyBytes_Check(item)) {
        bytes_obj = item;
      } else {
        return Status::TypeError("Non-string value encountered");
      }
      // No error checking
      length = PyBytes_GET_SIZE(bytes_obj);
      bytes = PyBytes_AS_STRING(bytes_obj);
      RETURN_NOT_OK(typed_builder_->Append(bytes, length));
    }
    return Status::OK();
  }
};

class UTF8Converter : public TypedConverter<arrow::StringBuilder> {
 public:
  Status AppendData(PyObject* seq) override {
    PyObject* item;
    PyObject* bytes_obj;
    OwnedRef tmp;
    const char* bytes;
    int32_t length;
    Py_ssize_t size = PySequence_Size(seq);
    for (int64_t i = 0; i < size; ++i) {
      item = PySequence_GetItem(seq, i);
      OwnedRef holder(item);

      if (item == Py_None) {
        RETURN_NOT_OK(typed_builder_->AppendNull());
        continue;
      } else if (!PyUnicode_Check(item)) {
        return Status::TypeError("Non-unicode value encountered");
      }
      tmp.reset(PyUnicode_AsUTF8String(item));
      RETURN_IF_PYERROR();
      bytes_obj = tmp.obj();

      // No error checking
      length = PyBytes_GET_SIZE(bytes_obj);
      bytes = PyBytes_AS_STRING(bytes_obj);
      RETURN_NOT_OK(typed_builder_->Append(bytes, length));
    }
    return Status::OK();
  }
};

class ListConverter : public TypedConverter<arrow::ListBuilder> {
 public:
  Status Init(const std::shared_ptr<ArrayBuilder>& builder) override;

  Status AppendData(PyObject* seq) override {
    Py_ssize_t size = PySequence_Size(seq);
    for (int64_t i = 0; i < size; ++i) {
      OwnedRef item(PySequence_GetItem(seq, i));
      if (item.obj() == Py_None) {
        RETURN_NOT_OK(typed_builder_->AppendNull());
      } else {
        typed_builder_->Append();
        RETURN_NOT_OK(value_converter_->AppendData(item.obj()));
      }
    }
    return Status::OK();
  }

 protected:
  std::shared_ptr<SeqConverter> value_converter_;
};

// Dynamic constructor for sequence converters
std::shared_ptr<SeqConverter> GetConverter(const std::shared_ptr<DataType>& type) {
  switch (type->type) {
    case Type::BOOL:
      return std::make_shared<BoolConverter>();
    case Type::INT64:
      return std::make_shared<Int64Converter>();
    case Type::DATE:
      return std::make_shared<DateConverter>();
    case Type::TIMESTAMP:
      return std::make_shared<TimestampConverter>();
    case Type::DOUBLE:
      return std::make_shared<DoubleConverter>();
    case Type::BINARY:
      return std::make_shared<BytesConverter>();
    case Type::STRING:
      return std::make_shared<UTF8Converter>();
    case Type::LIST:
      return std::make_shared<ListConverter>();
    case Type::STRUCT:
    default:
      return nullptr;
      break;
  }
}

Status ListConverter::Init(const std::shared_ptr<ArrayBuilder>& builder) {
  builder_ = builder;
  typed_builder_ = static_cast<arrow::ListBuilder*>(builder.get());

  value_converter_ =
      GetConverter(static_cast<arrow::ListType*>(builder->type().get())->value_type());
  if (value_converter_ == nullptr) {
    return Status::NotImplemented("value type not implemented");
  }

  value_converter_->Init(typed_builder_->value_builder());
  return Status::OK();
}

Status ConvertPySequence(PyObject* obj, std::shared_ptr<arrow::Array>* out) {
  std::shared_ptr<DataType> type;
  int64_t size;
  PyDateTime_IMPORT;
  RETURN_NOT_OK(InferArrowType(obj, &size, &type));

  // Handle NA / NullType case
  if (type->type == Type::NA) {
    out->reset(new arrow::NullArray(type, size));
    return Status::OK();
  }

  std::shared_ptr<SeqConverter> converter = GetConverter(type);
  if (converter == nullptr) {
    std::stringstream ss;
    ss << "No type converter implemented for " << type->ToString();
    return Status::NotImplemented(ss.str());
  }

  // Give the sequence converter an array builder
  std::shared_ptr<ArrayBuilder> builder;
  RETURN_NOT_OK(arrow::MakeBuilder(get_memory_pool(), type, &builder));
  converter->Init(builder);

  RETURN_NOT_OK(converter->AppendData(obj));

  return builder->Finish(out);
}

}  // namespace pyarrow

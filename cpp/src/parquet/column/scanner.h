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

#ifndef PARQUET_COLUMN_SCANNER_H
#define PARQUET_COLUMN_SCANNER_H

#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "parquet/column/reader.h"
#include "parquet/thrift/parquet_types.h"

namespace parquet_cpp {

static constexpr size_t DEFAULT_SCANNER_BATCH_SIZE = 128;

class Scanner {
 public:
  explicit Scanner(std::shared_ptr<ColumnReader> reader,
      size_t batch_size = DEFAULT_SCANNER_BATCH_SIZE) :
      batch_size_(batch_size),
      level_offset_(0),
      levels_buffered_(0),
      value_offset_(0),
      values_buffered_(0),
      reader_(reader) {
    // TODO: don't allocate for required fields
    def_levels_.resize(batch_size_);
    rep_levels_.resize(batch_size_);
  }

  virtual ~Scanner() {}

  static std::shared_ptr<Scanner> Make(std::shared_ptr<ColumnReader> col_reader,
      size_t batch_size = DEFAULT_SCANNER_BATCH_SIZE);

  virtual void PrintNext(std::ostream& out, int width) = 0;

  bool HasNext() {
    return value_offset_ < values_buffered_ || reader_->HasNext();
  }

  const parquet::SchemaElement* schema() const {
    return reader_->schema();
  }

  size_t batch_size() const { return batch_size_;}

  void SetBatchSize(size_t batch_size) {
    batch_size_ = batch_size;
  }

 protected:
  size_t batch_size_;

  std::vector<int16_t> def_levels_;
  std::vector<int16_t> rep_levels_;
  size_t level_offset_;
  size_t levels_buffered_;

  std::vector<uint8_t> value_buffer_;
  size_t value_offset_;
  size_t values_buffered_;

 private:
  std::shared_ptr<ColumnReader> reader_;
};


template <int TYPE>
class TypedScanner : public Scanner {
 public:
  typedef typename type_traits<TYPE>::value_type T;

  explicit TypedScanner(std::shared_ptr<ColumnReader> reader,
      size_t batch_size = DEFAULT_SCANNER_BATCH_SIZE) :
      Scanner(reader, batch_size) {
    typed_reader_ = static_cast<TypedColumnReader<TYPE>*>(reader.get());
    size_t value_byte_size = type_traits<TYPE>::value_byte_size;
    value_buffer_.resize(batch_size_ * value_byte_size);
    values_ = reinterpret_cast<T*>(&value_buffer_[0]);
  }

  virtual ~TypedScanner() {}

  bool NextLevels(int16_t* def_level, int16_t* rep_level) {
    if (level_offset_ == levels_buffered_) {
      levels_buffered_ = typed_reader_->ReadBatch(batch_size_, &def_levels_[0],
          &rep_levels_[0], values_, &values_buffered_);

      // TODO: repetition levels

      level_offset_ = 0;
      if (!levels_buffered_) {
        return false;
      }
    }
    *def_level = def_levels_[level_offset_++];
    *rep_level = 1;
    return true;
  }

  // Returns true if there is a next value
  bool NextValue(T* val, bool* is_null) {
    if (value_offset_ == values_buffered_) {
      if (!HasNext()) {
        // Out of data pages
        return false;
      }
    }

    // Out of values
    int16_t def_level;
    int16_t rep_level;
    NextLevels(&def_level, &rep_level);
    *is_null = def_level < rep_level;

    if (*is_null) {
      return true;
    }

    if (value_offset_ == values_buffered_) {
      throw ParquetException("Value was non-null, but has not been buffered");
    }
    *val = values_[value_offset_++];
    return true;
  }

  virtual void PrintNext(std::ostream& out, int width) {
    T val;
    bool is_null;

    char buffer[25];
    NextValue(&val, &is_null);

    if (is_null) {
      std::string null_fmt = format_fwf<parquet::Type::BYTE_ARRAY>(width);
      snprintf(buffer, sizeof(buffer), null_fmt.c_str(), "NULL");
    } else {
      FormatValue(&val, buffer, sizeof(buffer), width);
    }
    out << buffer;
  }

 private:
  // The ownership of this object is expressed through the reader_ variable in the base
  TypedColumnReader<TYPE>* typed_reader_;

  inline void FormatValue(void* val, char* buffer, size_t bufsize, size_t width);

  T* values_;
};


template <int TYPE>
inline void TypedScanner<TYPE>::FormatValue(void* val, char* buffer,
    size_t bufsize, size_t width) {
  std::string fmt = format_fwf<TYPE>(width);
  snprintf(buffer, bufsize, fmt.c_str(), *reinterpret_cast<T*>(val));
}

template <>
inline void TypedScanner<parquet::Type::INT96>::FormatValue(
    void* val, char* buffer, size_t bufsize, size_t width) {
  std::string fmt = format_fwf<parquet::Type::INT96>(width);
  std::string result = Int96ToString(*reinterpret_cast<Int96*>(val));
  snprintf(buffer, bufsize, fmt.c_str(), result.c_str());
}

template <>
inline void TypedScanner<parquet::Type::BYTE_ARRAY>::FormatValue(
    void* val, char* buffer, size_t bufsize, size_t width) {
  std::string fmt = format_fwf<parquet::Type::BYTE_ARRAY>(width);
  std::string result = ByteArrayToString(*reinterpret_cast<ByteArray*>(val));
  snprintf(buffer, bufsize, fmt.c_str(), result.c_str());
}

template <>
inline void TypedScanner<parquet::Type::FIXED_LEN_BYTE_ARRAY>::FormatValue(
    void* val, char* buffer, size_t bufsize, size_t width) {
  std::string fmt = format_fwf<parquet::Type::FIXED_LEN_BYTE_ARRAY>(width);
  std::string result = FixedLenByteArrayToString(
      *reinterpret_cast<FixedLenByteArray*>(val),
      schema()->type_length);
  snprintf(buffer, bufsize, fmt.c_str(), result.c_str());
}

typedef TypedScanner<parquet::Type::BOOLEAN> BoolScanner;
typedef TypedScanner<parquet::Type::INT32> Int32Scanner;
typedef TypedScanner<parquet::Type::INT64> Int64Scanner;
typedef TypedScanner<parquet::Type::INT96> Int96Scanner;
typedef TypedScanner<parquet::Type::FLOAT> FloatScanner;
typedef TypedScanner<parquet::Type::DOUBLE> DoubleScanner;
typedef TypedScanner<parquet::Type::BYTE_ARRAY> ByteArrayScanner;
typedef TypedScanner<parquet::Type::FIXED_LEN_BYTE_ARRAY> FixedLenByteArrayScanner;

} // namespace parquet_cpp

#endif // PARQUET_COLUMN_SCANNER_H

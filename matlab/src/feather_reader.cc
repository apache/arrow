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

#include <algorithm>
#include <cmath>

#include <arrow/io/file.h>
#include <arrow/ipc/feather.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/util/bit-util.h>

#include <mex.h>

#include "feather_reader.h"
#include "matlab_traits.h"
#include "util/handle_status.h"
#include "util/unicode_conversion.h"

namespace arrow {
namespace matlab {
namespace internal {

// Read the name of variable i from the Feather file as a mxArray*.
mxArray* ReadVariableName(const std::shared_ptr<Column>& column) {
  return matlab::util::ConvertUTF8StringToUTF16CharMatrix(column->name());
}

template <typename ArrowDataType>
mxArray* ReadNumericVariableData(const std::shared_ptr<Column>& column) {
  using MatlabType = typename MatlabTraits<ArrowDataType>::MatlabType;
  using ArrowArrayType = typename TypeTraits<ArrowDataType>::ArrayType;

  std::shared_ptr<ChunkedArray> chunked_array = column->data();
  const int32_t num_chunks = chunked_array->num_chunks();

  const mxClassID matlab_class_id = MatlabTraits<ArrowDataType>::matlab_class_id;
  // Allocate a numeric mxArray* with the correct mxClassID based on the type of the
  // arrow::Column.
  mxArray* variable_data =
      mxCreateNumericMatrix(column->length(), 1, matlab_class_id, mxREAL);

  int64_t mx_array_offset = 0;
  // Iterate over each arrow::Array in the arrow::ChunkedArray.
  for (int32_t i = 0; i < num_chunks; ++i) {
    std::shared_ptr<Array> array = chunked_array->chunk(i);
    const int64_t chunk_length = array->length();
    std::shared_ptr<ArrowArrayType> integer_array = std::static_pointer_cast<ArrowArrayType>(array);

    // Get a raw pointer to the Arrow array data.
    const MatlabType* source = integer_array->raw_values();

    // Get a mutable pointer to the MATLAB array data and std::copy the
    // Arrow array data into it.
    MatlabType* destination = MatlabTraits<ArrowDataType>::GetData(variable_data);
    std::copy(source, source + chunk_length, destination + mx_array_offset);
    mx_array_offset += chunk_length;
  }

  return variable_data;
}

// Read the data of variable i from the Feather file as a mxArray*.
mxArray* ReadVariableData(const std::shared_ptr<Column>& column) {
  std::shared_ptr<DataType> type = column->type();

  switch (type->id()) {
    case Type::FLOAT:
      return ReadNumericVariableData<FloatType>(column);
    case Type::DOUBLE:
      return ReadNumericVariableData<DoubleType>(column);
    case Type::UINT8:
      return ReadNumericVariableData<UInt8Type>(column);
    case Type::UINT16:
      return ReadNumericVariableData<UInt16Type>(column);
    case Type::UINT32:
      return ReadNumericVariableData<UInt32Type>(column);
    case Type::UINT64:
      return ReadNumericVariableData<UInt64Type>(column);
    case Type::INT8:
      return ReadNumericVariableData<Int8Type>(column);
    case Type::INT16:
      return ReadNumericVariableData<Int16Type>(column);
    case Type::INT32:
      return ReadNumericVariableData<Int32Type>(column);
    case Type::INT64:
      return ReadNumericVariableData<Int64Type>(column);
    default: {
      mexErrMsgIdAndTxt("MATLAB:arrow:UnsupportedArrowType",
                        "Unsupported arrow::Type '%s' for variable '%s'",
                        type->name().c_str(), column->name().c_str());
      break;
    }
  }

  return nullptr;
}

// arrow::Buffers are bit-packed, while mxLogical arrays aren't. This utility
// uses an Arrow utility to copy each bit of an arrow::Buffer into each byte
// of an mxLogical array.
void BitUnpackBuffer(const std::shared_ptr<Buffer>& source, int64_t length,
                     mxLogical* destination) {
  const uint8_t* source_data = source->data();

  // Call into an Arrow utility to visit each bit in the bitmap.
  auto visitFcn = [&](mxLogical is_valid) { *destination++ = is_valid; };

  const int64_t start_offset = 0;
  arrow::internal::VisitBitsUnrolled(source_data, start_offset, length, visitFcn);
}

// Populates the validity bitmap from an arrow::Array or an arrow::Column,
// writes to a zero-initialized destination buffer.
// Implements a fast path for the fully-valid and fully-invalid cases.
// Returns true if the destination buffer was successfully populated.
template <typename ArrowType>
bool TryBitUnpackFastPath(const std::shared_ptr<ArrowType>& array, mxLogical* destination) {
  const int64_t null_count = array->null_count();
  const int64_t length = array->length();

  if (null_count == length) {
    // The source array/column is filled with invalid values. Since mxCreateLogicalMatrix
    // zero-initializes the destination buffer, we can return without changing anything
    // in the destination buffer.
    return true;
  } else if (null_count == 0) {
    // The source array/column contains only valid values. Fill the destination buffer
    // with 'true'.
    std::fill(destination, destination + length, true);
    return true;
  }

  // Return false to indicate that we couldn't fill the entire validity bitmap.
  return false;
}

// Read the validity (null) bitmap of variable i from the Feather
// file as an mxArray*.
mxArray* ReadVariableValidityBitmap(const std::shared_ptr<Column>& column) {
  // Allocate an mxLogical array to store the validity (null) bitmap values.
  // Note: All Arrow arrays can have an associated validity (null) bitmap.
  // The Apache Arrow specification defines 0 (false) to represent an
  // invalid (null) array entry and 1 (true) to represent a valid
  // (non-null) array entry.
  mxArray* validity_bitmap = mxCreateLogicalMatrix(column->length(), 1);
  mxLogical* validity_bitmap_unpacked = mxGetLogicals(validity_bitmap);

  // The Apache Arrow specification allows validity (null) bitmaps
  // to be unallocated if there are no null values. In this case,
  // we simply return a logical array filled with the value true.
  if (TryBitUnpackFastPath(column, validity_bitmap_unpacked)) {
    // Return early since the validity bitmap was already filled.
    return validity_bitmap;
  }

  std::shared_ptr<ChunkedArray> chunked_array = column->data();
  const int32_t num_chunks = chunked_array->num_chunks();

  int64_t mx_array_offset = 0;
  // Iterate over each arrow::Array in the arrow::ChunkedArray.
  for (int32_t chunk_index = 0; chunk_index < num_chunks; ++chunk_index) {
    std::shared_ptr<Array> array = chunked_array->chunk(chunk_index);
    const int64_t array_length = array->length();

    if (!TryBitUnpackFastPath(array, validity_bitmap_unpacked + mx_array_offset)) {
      // Couldn't fill the full validity bitmap at once. Call an optimized loop-unrolled
      // implementation instead that goes byte-by-byte and populates the validity bitmap.
      BitUnpackBuffer(array->null_bitmap(), array_length,
                      validity_bitmap_unpacked + mx_array_offset);
    }

    mx_array_offset += array_length;
  }

  return validity_bitmap;
}

// Read the type name of an Arrow column as an mxChar array.
mxArray* ReadVariableType(const std::shared_ptr<Column>& column) {
  return util::ConvertUTF8StringToUTF16CharMatrix(column->type()->name());
}

// MATLAB arrays cannot be larger than 2^48 elements.
static constexpr uint64_t MAX_MATLAB_SIZE = static_cast<uint64_t>(0x01) << 48;

}  // namespace internal

Status FeatherReader::Open(const std::string& filename,
                           std::shared_ptr<FeatherReader>* feather_reader) {
  *feather_reader = std::shared_ptr<FeatherReader>(new FeatherReader());
 
  // Open file with given filename as a ReadableFile.
  std::shared_ptr<io::ReadableFile> readable_file(nullptr);
  
  RETURN_NOT_OK(io::ReadableFile::Open(filename, &readable_file));
  
  // TableReader expects a RandomAccessFile.
  std::shared_ptr<io::RandomAccessFile> random_access_file(readable_file);

  // Open the Feather file for reading with a TableReader.
  RETURN_NOT_OK(ipc::feather::TableReader::Open(
      random_access_file, &(*feather_reader)->table_reader_));

  // Read the table metadata from the Feather file.
  (*feather_reader)->num_rows_ = (*feather_reader)->table_reader_->num_rows();
  (*feather_reader)->num_variables_ = (*feather_reader)->table_reader_->num_columns();
  (*feather_reader)->description_ =
      (*feather_reader)->table_reader_->HasDescription()
          ? (*feather_reader)->table_reader_->GetDescription()
          : "";

  if ((*feather_reader)->num_rows_ > internal::MAX_MATLAB_SIZE ||
      (*feather_reader)->num_variables_ > internal::MAX_MATLAB_SIZE) {
    mexErrMsgIdAndTxt("MATLAB:arrow:SizeTooLarge",
                      "The table size exceeds MATLAB limits: %u x %u",
                      (*feather_reader)->num_rows_, (*feather_reader)->num_variables_);
  }

  return Status::OK();
}

// Read the table metadata from the Feather file as a mxArray*.
mxArray* FeatherReader::ReadMetadata() const {
  const int32_t num_metadata_fields = 3;
  const char* fieldnames[] = {"NumRows", "NumVariables", "Description"};

  // Create a mxArray struct array containing the table metadata to be passed back to
  // MATLAB.
  mxArray* metadata = mxCreateStructMatrix(1, 1, num_metadata_fields, fieldnames);

  // Returning double values to MATLAB since that is the default type.

  // Set the number of rows.
  mxSetField(metadata, 0, "NumRows",
             mxCreateDoubleScalar(static_cast<double>(num_rows_)));

  // Set the number of variables.
  mxSetField(metadata, 0, "NumVariables",
             mxCreateDoubleScalar(static_cast<double>(num_variables_)));

  // Set the description.
  mxSetField(metadata, 0, "Description",
             util::ConvertUTF8StringToUTF16CharMatrix(description_));

  return metadata;
}

// Read the table variables from the Feather file as a mxArray*.
mxArray* FeatherReader::ReadVariables() const {
  const int32_t num_variable_fields = 4;
  const char* fieldnames[] = {"Name", "Type", "Data", "Valid"};

  // Create an mxArray* struct array containing the table variables to be passed back to
  // MATLAB.
  mxArray* variables =
      mxCreateStructMatrix(1, num_variables_, num_variable_fields, fieldnames);

  // Read all the table variables in the Feather file into memory.
  for (int64_t i = 0; i < num_variables_; ++i) {
    std::shared_ptr<Column> column(nullptr);
    util::HandleStatus(table_reader_->GetColumn(i, &column));

    // set the struct fields data
    mxSetField(variables, i, "Name", internal::ReadVariableName(column));
    mxSetField(variables, i, "Type", internal::ReadVariableType(column));
    mxSetField(variables, i, "Data", internal::ReadVariableData(column));
    mxSetField(variables, i, "Valid", internal::ReadVariableValidityBitmap(column));
  }

  return variables;
}

}  // namespace matlab
}  // namespace arrow

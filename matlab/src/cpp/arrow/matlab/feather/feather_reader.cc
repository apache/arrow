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

#include "feather_reader.h"

#include <arrow/array/array_base.h>
#include <arrow/array/builder_base.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/io/file.h>
#include <arrow/ipc/feather.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/type_traits.h>
#include <arrow/util/bitmap_visit.h>
#include <mex.h>

#include <algorithm>
#include <cmath>

#include "matlab_traits.h"
#include "util/handle_status.h"
#include "util/unicode_conversion.h"

namespace arrow {
namespace matlab {
namespace feather {
namespace internal {

// Read the name of variable i from the Feather file as a mxArray*.
mxArray* ReadVariableName(const std::string& column_name) {
  return util::ConvertUTF8StringToUTF16CharMatrix(column_name);
}

template <typename ArrowDataType>
mxArray* ReadNumericVariableData(const std::shared_ptr<Array>& column) {
  using MatlabType = typename MatlabTraits<ArrowDataType>::MatlabType;
  using ArrowArrayType = typename TypeTraits<ArrowDataType>::ArrayType;

  const mxClassID matlab_class_id = MatlabTraits<ArrowDataType>::matlab_class_id;
  // Allocate a numeric mxArray* with the correct mxClassID based on the type of the
  // arrow::Array.
  mxArray* variable_data =
      mxCreateNumericMatrix(column->length(), 1, matlab_class_id, mxREAL);

  auto arrow_numeric_array = std::static_pointer_cast<ArrowArrayType>(column);

  // Get a raw pointer to the Arrow array data.
  const MatlabType* source = arrow_numeric_array->raw_values();

  // Get a mutable pointer to the MATLAB array data and std::copy the
  // Arrow array data into it.
  MatlabType* destination = MatlabTraits<ArrowDataType>::GetData(variable_data);
  std::copy(source, source + column->length(), destination);

  return variable_data;
}

// Read the data of variable i from the Feather file as a mxArray*.
mxArray* ReadVariableData(const std::shared_ptr<Array>& column,
                          const std::string& column_name) {
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
                        type->name().c_str(), column_name.c_str());
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

// Populates the validity bitmap from an arrow::Array.
// writes to a zero-initialized destination buffer.
// Implements a fast path for the fully-valid and fully-invalid cases.
// Returns true if the destination buffer was successfully populated.
bool TryBitUnpackFastPath(const std::shared_ptr<Array>& array, mxLogical* destination) {
  const int64_t null_count = array->null_count();
  const int64_t length = array->length();

  if (null_count == length) {
    // The source array is filled with invalid values. Since mxCreateLogicalMatrix
    // zero-initializes the destination buffer, we can return without changing anything
    // in the destination buffer.
    return true;
  } else if (null_count == 0) {
    // The source array contains only valid values. Fill the destination buffer
    // with 'true'.
    std::fill(destination, destination + length, true);
    return true;
  }

  // Return false to indicate that we couldn't fill the entire validity bitmap.
  return false;
}

// Read the validity (null) bitmap of variable i from the Feather
// file as an mxArray*.
mxArray* ReadVariableValidityBitmap(const std::shared_ptr<Array>& column) {
  // Allocate an mxLogical array to store the validity (null) bitmap values.
  // Note: All Arrow arrays can have an associated validity (null) bitmap.
  // The Apache Arrow specification defines 0 (false) to represent an
  // invalid (null) array entry and 1 (true) to represent a valid
  // (non-null) array entry.
  mxArray* validity_bitmap = mxCreateLogicalMatrix(column->length(), 1);
  mxLogical* validity_bitmap_unpacked = mxGetLogicals(validity_bitmap);

  if (!TryBitUnpackFastPath(column, validity_bitmap_unpacked)) {
    // Couldn't fill the full validity bitmap at once. Call an optimized loop-unrolled
    // implementation instead that goes byte-by-byte and populates the validity bitmap.
    BitUnpackBuffer(column->null_bitmap(), column->length(), validity_bitmap_unpacked);
  }

  return validity_bitmap;
}

// Read the type name of an arrow::Array as an mxChar array.
mxArray* ReadVariableType(const std::shared_ptr<Array>& column) {
  return util::ConvertUTF8StringToUTF16CharMatrix(column->type()->name());
}

// MATLAB arrays cannot be larger than 2^48 elements.
static constexpr uint64_t MAX_MATLAB_SIZE = static_cast<uint64_t>(0x01) << 48;

}  // namespace internal

Status FeatherReader::Open(const std::string& filename,
                           std::shared_ptr<FeatherReader>* feather_reader) {
  *feather_reader = std::shared_ptr<FeatherReader>(new FeatherReader());

  // Open file with given filename as a ReadableFile.
  ARROW_ASSIGN_OR_RAISE(auto readable_file, io::ReadableFile::Open(filename));

  // Open the Feather file for reading with a TableReader.
  ARROW_ASSIGN_OR_RAISE(auto reader, ipc::feather::Reader::Open(readable_file));

  // Set the internal reader_ object.
  (*feather_reader)->reader_ = reader;

  // Check the feather file version
  auto version = reader->version();
  if (version == ipc::feather::kFeatherV2Version) {
    return Status::NotImplemented("Support for Feather V2 has not been implemented.");
  } else if (version != ipc::feather::kFeatherV1Version) {
    return Status::Invalid("Unknown Feather format version.");
  }

  // read the table metadata from the Feather file
  (*feather_reader)->num_variables_ = reader->schema()->num_fields();
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

  return metadata;
}

// Read the table variables from the Feather file as a mxArray*.
mxArray* FeatherReader::ReadVariables() {
  const int32_t num_variable_fields = 4;
  const char* fieldnames[] = {"Name", "Type", "Data", "Valid"};

  // Create an mxArray* struct array containing the table variables to be passed back to
  // MATLAB.
  mxArray* variables =
      mxCreateStructMatrix(1, num_variables_, num_variable_fields, fieldnames);

  std::shared_ptr<arrow::Table> table;
  auto status = reader_->Read(&table);
  if (!status.ok()) {
    mexErrMsgIdAndTxt("MATLAB:arrow:FeatherReader::FailedToReadTable",
                      "Failed to read arrow::Table from Feather file. Reason: %s",
                      status.message().c_str());
  }

  // Set the number of rows
  num_rows_ = table->num_rows();

  if (num_rows_ > internal::MAX_MATLAB_SIZE ||
      num_variables_ > internal::MAX_MATLAB_SIZE) {
    mexErrMsgIdAndTxt("MATLAB:arrow:SizeTooLarge",
                      "The table size exceeds MATLAB limits: %u x %u", num_rows_,
                      num_variables_);
  }

  auto column_names = table->ColumnNames();

  for (int64_t i = 0; i < num_variables_; ++i) {
    auto column = table->column(i);
    if (column->num_chunks() != 1) {
      mexErrMsgIdAndTxt("MATLAB:arrow:FeatherReader::ReadVariables",
                        "Chunked columns not yet supported");
    }
    std::shared_ptr<Array> chunk = column->chunk(0);
    const std::string column_name = column_names[i];

    // set the struct fields data
    mxSetField(variables, i, "Name", internal::ReadVariableName(column_name));
    mxSetField(variables, i, "Type", internal::ReadVariableType(chunk));
    mxSetField(variables, i, "Data", internal::ReadVariableData(chunk, column_name));
    mxSetField(variables, i, "Valid", internal::ReadVariableValidityBitmap(chunk));
  }

  return variables;
}

}  // namespace feather
}  // namespace matlab
}  // namespace arrow
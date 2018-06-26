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

#include <arrow/io/file.h>
#include <arrow/ipc/feather.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/type.h>

#include <mex.h>

#include "FeatherReader.h"
#include "FeatherType.h"

namespace matlab {
namespace arrow {
namespace ipc {
namespace feather {

// MATLAB Arrays cannot be larger than 2^48
static const uint64_t MAX_MATLAB_SIZE = static_cast<uint64_t>(0x01) << 48;

// FeatherReader Constructor.
FeatherReader::FeatherReader(const std::string& filename) : filename_{filename} {
  // Open the given Feather file for reading.
  Open();

  // Read the table metadata from the Feather file.
  num_rows_ = table_reader_->num_rows();
  num_variables_ = table_reader_->num_columns();
  description_ = table_reader_->HasDescription() ? table_reader_->GetDescription() : "";
  version_ = table_reader_->version();

  if (num_rows_ > MAX_MATLAB_SIZE || num_variables_ > MAX_MATLAB_SIZE) {
    mexErrMsgIdAndTxt("MATLAB:feather:read:SizeTooLarge",
                      "The table size exceeds MATLAB limits: %u x %u", num_rows_,
                      num_variables_);
  }
}

// Open the Feather file for reading.
void FeatherReader::Open() {
  // Open file with given filename as a ReadableFile.
  std::shared_ptr<::arrow::io::ReadableFile> readable_file(nullptr);
  HandleStatus(::arrow::io::ReadableFile::Open(filename_, &readable_file));

  // TableReader expects a RandomAccessFile.
  std::shared_ptr<::arrow::io::RandomAccessFile> random_access_file(readable_file);
  // Open the Feather file for reading with a TableReader.
  HandleStatus(
      ::arrow::ipc::feather::TableReader::Open(random_access_file, &table_reader_));
}

// Read the table metadata from the Feather file as a mxArray*.
mxArray* FeatherReader::ReadMetadata() const {
  const int num_metadata_fields = 4;
  const char* fieldnames[] = {"NumRows", "NumVariables", "Description", "Version"};

  // Create a mxArray struct array containing the table metadata to be passed back to
  // MATLAB.
  mxArray* metadata = mxCreateStructMatrix(1, 1, num_metadata_fields, fieldnames);

  // Returning double values to MATLAB since that is the default type.
  // Note: MATLAB's size on tables prevents importing a table any larger than 2^52 (many
  // fewer rows/variables in fact)
  // files with more data that will error earlier.

  // Set the number of rows.
  mxSetField(metadata, 0, "NumRows",
             mxCreateDoubleScalar(static_cast<double>(num_rows_)));

  // Set the number of variables.
  mxSetField(metadata, 0, "NumVariables",
             mxCreateDoubleScalar(static_cast<double>(num_variables_)));

  // Set the description.
  mxSetField(metadata, 0, "Description", mxCreateString(description_.c_str()));

  // Set the version.
  mxSetField(metadata, 0, "Version", mxCreateDoubleScalar(static_cast<double>(version_)));

  return metadata;
}

// Read the table variables from the Feather file as a mxArray*.
mxArray* FeatherReader::ReadVariables() const {
  const int num_variable_fields = 4;
  const char* fieldnames[] = {"Name", "Data", "Nulls", "Type"};

  // Create an mxArray struct array containing the table variables to be passed back to
  // MATLAB.
  mxArray* variables =
      mxCreateStructMatrix(1, num_variables_, num_variable_fields, fieldnames);

  // Read all the table variables in the Feather file into memory.
  for (int64_t i = 0; i < num_variables_; ++i) {
    std::shared_ptr<::arrow::Column> column(nullptr);
    HandleStatus(table_reader_->GetColumn(i, &column));

    // set the struct fields data
    mxSetField(variables, i, "Name", ReadVariableName(column));
    mxSetField(variables, i, "Data", ReadVariableData(column));
    mxSetField(variables, i, "Nulls", ReadVariableNulls(column));
    mxSetField(variables, i, "Type", ReadVariableType(column));
  }

  return variables;
}

// Terminates execution of Feather file reading and returns to the MATLAB prompt,
// displaying
// an error message if the given status indicates that an error has occurred.
void FeatherReader::HandleStatus(const ::arrow::Status& status) const {
  const char* arrow_error_message = "Arrow error: %s";
  switch (status.code()) {
    case ::arrow::StatusCode::OK: {
      break;
    }
    case ::arrow::StatusCode::OutOfMemory: {
      mexErrMsgIdAndTxt("MATLAB:feather:read:arrow:status:OutOfMemory",
                        arrow_error_message, status.ToString().c_str());
      break;
    }
    case ::arrow::StatusCode::KeyError: {
      mexErrMsgIdAndTxt("MATLAB:feather:read:arrow:status:KeyError", arrow_error_message,
                        status.ToString().c_str());
      break;
    }
    case ::arrow::StatusCode::TypeError: {
      mexErrMsgIdAndTxt("MATLAB:feather:read:arrow:status:TypeError", arrow_error_message,
                        status.ToString().c_str());
      break;
    }
    case ::arrow::StatusCode::Invalid: {
      mexErrMsgIdAndTxt("MATLAB:feather:read:arrow:status:Invalid", arrow_error_message,
                        status.ToString().c_str());
      break;
    }
    case ::arrow::StatusCode::IOError: {
      mexErrMsgIdAndTxt("MATLAB:feather:read:arrow:status:IOError", arrow_error_message,
                        status.ToString().c_str());
      break;
    }
    case ::arrow::StatusCode::CapacityError: {
      mexErrMsgIdAndTxt("MATLAB:feather:read:arrow:status:CapacityError",
                        arrow_error_message, status.ToString().c_str());
      break;
    }
    case ::arrow::StatusCode::UnknownError: {
      mexErrMsgIdAndTxt("MATLAB:feather:read:arrow:status:UnknownError",
                        arrow_error_message, status.ToString().c_str());
      break;
    }
    case ::arrow::StatusCode::NotImplemented: {
      mexErrMsgIdAndTxt("MATLAB:feather:read:arrow:status:NotImplemented",
                        arrow_error_message, status.ToString().c_str());
      break;
    }
    case ::arrow::StatusCode::SerializationError: {
      mexErrMsgIdAndTxt("MATLAB:feather:read:arrow:status:SerializationError",
                        arrow_error_message, status.ToString().c_str());
      break;
    }
    case ::arrow::StatusCode::PythonError: {
      mexErrMsgIdAndTxt("MATLAB:feather:read:arrow:status:PythonError",
                        arrow_error_message, status.ToString().c_str());
      break;
    }
    case ::arrow::StatusCode::PlasmaObjectExists: {
      mexErrMsgIdAndTxt("MATLAB:feather:read:arrow:status:PlasmaObjectExists",
                        arrow_error_message, status.ToString().c_str());
      break;
    }
    case ::arrow::StatusCode::PlasmaObjectNonexistent: {
      mexErrMsgIdAndTxt("MATLAB:feather:read:arrow:status:PlasmaObjectNonexistent",
                        arrow_error_message, status.ToString().c_str());
      break;
    }
    case ::arrow::StatusCode::PlasmaStoreFull: {
      mexErrMsgIdAndTxt("MATLAB:feather:read:arrow:status:PlasmaStoreFull",
                        arrow_error_message, status.ToString().c_str());
      break;
    }
    case ::arrow::StatusCode::PlasmaObjectAlreadySealed: {
      mexErrMsgIdAndTxt("MATLAB:feather:read:arrow:status:PlasmaObjectAlreadySealed",
                        arrow_error_message, status.ToString().c_str());
      break;
    }
    default: {
      mexErrMsgIdAndTxt("MATLAB:feather:read:arrow:status:UnknownStatus",
                        arrow_error_message, "Unknown status");
      break;
    }
  }
}

// Read the name of variable i from the Feather file as a mxArray*.
mxArray* FeatherReader::ReadVariableName(
    const std::shared_ptr<::arrow::Column>& column) const {
  return mxCreateString(column->name().c_str());
}

template <::arrow::Type::type ArrowTypeID>
mxArray* FeatherReader::ReadNumericVariableData(
    const std::shared_ptr<::arrow::Column>& column) const {
  typedef typename FeatherType<ArrowTypeID>::ArrowArrayType ArrowArrayType;
  typedef typename FeatherType<ArrowTypeID>::MatlabType MatlabType;

  std::shared_ptr<::arrow::ChunkedArray> chunked_array = column->data();
  const int num_chunks = chunked_array->num_chunks();

  const mxClassID matlab_class_id = FeatherType<ArrowTypeID>::matlab_class_id;
  // Allocate a numeric mxArray* with the correct mxClassID based on the type of the
  // arrow::Column.
  mxArray* variable_data = mxCreateNumericMatrix(num_rows_, 1, matlab_class_id, mxREAL);

  int64_t mx_array_offset = 0;
  // Iterate over each arrow::Array in the arrow::ChunkedArray.
  for (int i = 0; i < num_chunks; ++i) {
    std::shared_ptr<::arrow::Array> array = chunked_array->chunk(i);
    const int64_t chunk_length = array->length();
    std::shared_ptr<ArrowArrayType> arr = std::static_pointer_cast<ArrowArrayType>(array);
    const auto data = arr->raw_values();
    MatlabType* dt = FeatherType<ArrowTypeID>::GetData(variable_data);
    std::copy(data, data + chunk_length, dt + mx_array_offset);
    mx_array_offset += chunk_length;
  }

  return variable_data;
}

// Read the data of variable i from the Feather file as a mxArray*.
mxArray* FeatherReader::ReadVariableData(
    const std::shared_ptr<::arrow::Column>& column) const {
  std::shared_ptr<::arrow::DataType> type = column->type();

  switch (type->id()) {
    case ::arrow::Type::FLOAT:
      return ReadNumericVariableData<::arrow::Type::FLOAT>(column);
    case ::arrow::Type::DOUBLE:
      return ReadNumericVariableData<::arrow::Type::DOUBLE>(column);
    case ::arrow::Type::UINT8:
      return ReadNumericVariableData<::arrow::Type::UINT8>(column);
    case ::arrow::Type::UINT16:
      return ReadNumericVariableData<::arrow::Type::UINT16>(column);
    case ::arrow::Type::UINT32:
      return ReadNumericVariableData<::arrow::Type::UINT32>(column);
    case ::arrow::Type::UINT64:
      return ReadNumericVariableData<::arrow::Type::UINT64>(column);
    case ::arrow::Type::INT8:
      return ReadNumericVariableData<::arrow::Type::INT8>(column);
    case ::arrow::Type::INT16:
      return ReadNumericVariableData<::arrow::Type::INT16>(column);
    case ::arrow::Type::INT32:
      return ReadNumericVariableData<::arrow::Type::INT32>(column);
    case ::arrow::Type::INT64:
      return ReadNumericVariableData<::arrow::Type::INT64>(column);

    default: {
      mexErrMsgIdAndTxt("MATLAB:feather:read:UnsupportedArrowType",
                        "Unsupported arrow::Type '%s' for variable '%s'",
                        type->name().c_str(), column->name().c_str());
      break;
    }
  }

  return nullptr;
}

// Read the nulls of variable i from the Feather file as a mxArray*.
mxArray* FeatherReader::ReadVariableNulls(
    const std::shared_ptr<::arrow::Column>& column) const {
  // TODO: Implement proper null value support. For the time being,
  // we will simply return a zero initialized logical array to MATLAB.
  return mxCreateLogicalMatrix(num_rows_, 1);
}

// Read the type of variable i from the Feather file as a mxArray*.
mxArray* FeatherReader::ReadVariableType(
    const std::shared_ptr<::arrow::Column>& column) const {
  return mxCreateString(column->type()->name().c_str());
}

}  // namespace feather
}  // namespace ipc
}  // namespace arrow
}  // namespace matlab

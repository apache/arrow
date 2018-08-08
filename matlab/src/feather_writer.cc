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

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/io/file.h>
#include <arrow/ipc/feather.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/type.h>

#include <mex.h>

#include "feather_writer.h"
#include "matlab_traits.h"
#include "util/handle_status.h"

namespace mlarrow {

namespace internal {

// Verify that the input mxStructArray has the right field names and types.
// Returns void since these errors should stop execution and throw in the MATLAB layer.
void ValidateMxStructSchema(const mxArray* array, int num_fields, const char** fieldnames,
                            const mxClassID* class_ids) {
  // Check that the input mxArray is a struct array.
  if (!mxIsStruct(array)) {
    mexErrMsgIdAndTxt("MATLAB:arrow:IncorrectDimensionsOrType",
                      "Input needs to be a struct array");
  }

  // Iterate over desired fieldnames and verify existence in the input struct array.
  for (int i = 0; i < num_fields; ++i) {
    mxArray* field = mxGetField(array, 0, fieldnames[i]);

    if (!field) {
      mexErrMsgIdAndTxt("MATLAB:arrow:MissingStructField",
                        "Missing field '%s' in input struct array", fieldnames[i]);
    }

    mxClassID class_id = mxGetClassID(field);

    // Avoid type check if an mxUNKNOWN_CLASS is passed in since we use this to
    // signify genericity in the input type
    if (class_ids[i] != mxUNKNOWN_CLASS) {
      if (class_ids[i] != class_id) {
        mexErrMsgIdAndTxt("MATLAB:arrow:MissingStructField",
                          "Incorrect type '%s' for struct array field '%s'",
                          mxGetClassName(field), fieldnames[i]);
      }
    }

    // Avoid empty check for mxChar arrays since Description can be empty.
    if (class_ids[i] != mxCHAR_CLASS) {
      // Ensure that each individual mxStructArray field is non-empty.
      // we can call mxGetData after this without needing another null-check
      if (mxIsEmpty(field)) {
        mexErrMsgIdAndTxt("MATLAB:arrow:EmptyStructField",
                          "Struct array field '%s' cannot be empty", fieldnames[i]);
      }
    }
  }
}

// Utility function to convert mxChar mxArray* to std::string while preserving
// Unicode code points.
std::string MxArrayToString(const mxArray* array) {
  // Return empty std::string if a mxChar array is not passed in.
  if (!mxIsChar(array)) {
    return std::string();
  }

  // Convert mxArray first to a C-style char array, then copy into a std::string.
  char* utf8_array = mxArrayToUTF8String(array);
  std::string output(utf8_array);

  // Free the allocated char* from the MEX runtime.
  mxFree(utf8_array);

  return output;
}

}  // namespace internal

arrow::Status FeatherWriter::Open(const std::string& filename,
                                  std::shared_ptr<FeatherWriter>* feather_writer) {
  // Allocate shared_ptr out parameter.
  *feather_writer = std::shared_ptr<FeatherWriter>(new FeatherWriter());

  // Open a FileOutputStream corresponding to the provided filename.
  std::shared_ptr<arrow::io::OutputStream> writable_file(nullptr);
  ARROW_RETURN_NOT_OK(arrow::io::FileOutputStream::Open(filename, &writable_file));

  // TableWriter::Open expects a shared_ptr to an OutputStream.
  // Open the Feather file for writing with a TableWriter
  ARROW_RETURN_NOT_OK(arrow::ipc::feather::TableWriter::Open(
      writable_file, &(*feather_writer)->table_writer_));

  return arrow::Status::OK();
}

// Write table metadata to the Feather file from a mxArray*.
arrow::Status FeatherWriter::WriteMetadata(const mxArray* metadata) {
  // Input metadata mxStructArray* must contain these fields.
  const int num_fields = 4;
  const char* fieldnames[num_fields] = {"Description", "NumRows", "NumVariables",
                                        "Version"};
  const mxClassID class_ids[num_fields] = {mxCHAR_CLASS, mxDOUBLE_CLASS, mxDOUBLE_CLASS,
                                           mxDOUBLE_CLASS};

  // Verify that all required fieldnames are provided.
  internal::ValidateMxStructSchema(metadata, num_fields, fieldnames, class_ids);

  // Convert Description to a std::string and set on FeatherWriter and TableWriter.
  std::string description =
      internal::MxArrayToString(mxGetField(metadata, 0, "Description"));
  this->description_ = description;
  this->table_writer_->SetDescription(description);

  // Get the NumRows field in the struct array and set on TableWriter.
  this->num_rows_ = static_cast<int64_t>(mxGetScalar(mxGetField(metadata, 0, "NumRows")));
  this->table_writer_->SetNumRows(this->num_rows_);

  // Get the total number of variables.
  this->num_variables_ =
      static_cast<int64_t>(mxGetScalar(mxGetField(metadata, 0, "NumVariables")));

  // Store the version information so we could handle possible future API changes.
  this->version_ = static_cast<int>(mxGetScalar(mxGetField(metadata, 0, "Version")));

  return arrow::Status::OK();
}

// Write mxArrays from MATLAB into a Feather file.
arrow::Status FeatherWriter::WriteVariables(const mxArray* variables) {
  // Required fields for the input struct array.
  const int num_fields = 4;
  const char* fieldnames[num_fields] = {"Name", "Type", "Data", "Nulls"};
  const mxClassID class_ids[num_fields] = {mxCHAR_CLASS, mxCHAR_CLASS, mxUNKNOWN_CLASS,
                                           mxLOGICAL_CLASS};

  // Verify that all required fieldnames are provided.
  internal::ValidateMxStructSchema(variables, num_fields, fieldnames, class_ids);

  // Get the number of table columns in the struct array.
  int num_columns = (mxGetM(variables) == 1) ? mxGetN(variables) : mxGetM(variables);

  // Iterate over the input columns and generate arrow arrays.
  for (int idx = 0; idx < num_columns; ++idx) {
    const mxArray* name = mxGetField(variables, idx, "Name");
    const mxArray* data = mxGetField(variables, idx, "Data");
    const mxArray* type = mxGetField(variables, idx, "Type");
    const mxArray* nulls = mxGetField(variables, idx, "Nulls");

    // Pass mxArray data to internal function that uses the Arrow libraries.
    ARROW_RETURN_NOT_OK(this->WriteVariableData(name, data, type, nulls));
  }

  // Write the Feather file metadata to the end of the file.
  arrow::Status status = this->table_writer_->Finalize();

  return status;
}

// Dispatch MATLAB table data to the correct column writer.
arrow::Status FeatherWriter::WriteVariableData(const mxArray* name, const mxArray* data,
                                               const mxArray* type,
                                               const mxArray* nulls) {
  // Get the underlying type of the mxArray data.
  const mxClassID mxclass = mxGetClassID(data);

  switch (mxclass) {
    case mxSINGLE_CLASS:
      return WriteNumericData<arrow::FloatType>(name, data, nulls);
    case mxDOUBLE_CLASS:
      return WriteNumericData<arrow::DoubleType>(name, data, nulls);
    case mxUINT8_CLASS:
      return WriteNumericData<arrow::UInt8Type>(name, data, nulls);
    case mxUINT16_CLASS:
      return WriteNumericData<arrow::UInt16Type>(name, data, nulls);
    case mxUINT32_CLASS:
      return WriteNumericData<arrow::UInt32Type>(name, data, nulls);
    case mxUINT64_CLASS:
      return WriteNumericData<arrow::UInt64Type>(name, data, nulls);
    case mxINT8_CLASS:
      return WriteNumericData<arrow::Int8Type>(name, data, nulls);
    case mxINT16_CLASS:
      return WriteNumericData<arrow::Int16Type>(name, data, nulls);
    case mxINT32_CLASS:
      return WriteNumericData<arrow::Int32Type>(name, data, nulls);
    case mxINT64_CLASS:
      return WriteNumericData<arrow::Int64Type>(name, data, nulls);

    default: {
      mexErrMsgIdAndTxt("MATLAB:arrow:UnsupportedArrowType",
                        "Unsupported arrow::Type '%s' for variable '%s'",
                        mxGetClassName(data), mxArrayToUTF8String(name));
    }
  }

  return arrow::Status::UnknownError("unknown error occurred in unreachable branch");
}

// Write numeric datatypes to the Feather file.
template <typename ArrowDataType>
arrow::Status FeatherWriter::WriteNumericData(const mxArray* name, const mxArray* data,
                                              const mxArray* nulls) {
  // Alias the type name for the underlying MATLAB type.
  using MatlabType = typename MatlabTraits<ArrowDataType>::MatlabType;

  // Get a pointer to the underlying mxArray data.
  // We need to (temporarily) cast away const here since the mxGet* functions do not
  // accept a const input parameter for compatibility reasons.
  const MatlabType* dt = MatlabTraits<ArrowDataType>::GetData(const_cast<mxArray*>(data));

  // Construct an arrow::Buffer that points to the underlying mxArray without copying.
  // - The lifetime of the mxArray buffer exceeds that of the arrow::Buffer here since
  //   MATLAB should only free this region on garbage-collection after the MEX function
  //   is executed. Therefore it is safe for arrow::Buffer to point to this location.
  // - However arrow::Buffer must not free this region by itself, since that could cause
  //   segfaults if the input array is used later in MATLAB.
  //   - The Doxygen doc for arrow::Buffer's constructor implies that it is not an RAII
  //     type, so this should be safe from possible double-free here.
  std::shared_ptr<arrow::Buffer> buffer(
      new arrow::Buffer(reinterpret_cast<const uint8_t*>(dt),
                        mxGetElementSize(data) * mxGetNumberOfElements(data)));

  // TODO : add support for nulls in data

  // Construct arrow::NumericArray specialization using arrow::Buffer.
  const arrow::NumericArray<ArrowDataType> arr(mxGetNumberOfElements(data), buffer);

  // Convert column name to a std::string from mxArray*.
  std::string name_str = internal::MxArrayToString(name);

  // Append new column to table writer.
  this->table_writer_->Append(name_str, arr);

  return arrow::Status::OK();
}

}  // namespace mlarrow

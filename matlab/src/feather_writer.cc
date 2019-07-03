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

#include <cmath>
#include <functional> /* for std::multiplies */
#include <numeric>    /* for std::accumulate */

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/io/file.h>
#include <arrow/ipc/feather.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/util/bit-util.h>

#include <mex.h>

#include "feather_writer.h"
#include "matlab_traits.h"
#include "util/handle_status.h"

namespace arrow {
namespace matlab {
namespace internal {

// Utility that helps verify the input mxArray struct field name and type.
// Returns void since any errors will throw and terminate MEX execution.
void ValidateMxStructField(const mxArray* struct_array, const char* fieldname,
                           mxClassID expected_class_id, bool can_be_empty) {
  // Check that the input mxArray is a struct array.
  if (!mxIsStruct(struct_array)) {
    mexErrMsgIdAndTxt("MATLAB:arrow:IncorrectDimensionsOrType",
                      "Input needs to be a struct array");
  }

  // Return early if an empty table is provided as input.
  if (mxIsEmpty(struct_array)) {
    return;
  }

  mxArray* field = mxGetField(struct_array, 0, fieldname);

  if (!field) {
    mexErrMsgIdAndTxt("MATLAB:arrow:MissingStructField",
                      "Missing field '%s' in input struct array", fieldname);
  }

  mxClassID actual_class_id = mxGetClassID(field);

  // Avoid type check if an mxUNKNOWN_CLASS is provided since the UNKNOWN type is used to
  // signify genericity in the input type.
  if (expected_class_id != mxUNKNOWN_CLASS) {
    if (expected_class_id != actual_class_id) {
      mexErrMsgIdAndTxt("MATLAB:arrow:MissingStructField",
                        "Incorrect type '%s' for struct array field '%s'",
                        mxGetClassName(field), fieldname);
    }
  }

  // Some struct fields (like the table description) can be empty, while others 
  // (like NumRows) should never be empty. This conditional helps account for both cases.
  if (!can_be_empty) {
    // Ensure that individual mxStructArray fields are non-empty.
    // We can call mxGetData after this without needing another null check.
    if (mxIsEmpty(field)) {
      mexErrMsgIdAndTxt("MATLAB:arrow:EmptyStructField",
                        "Struct array field '%s' cannot be empty", fieldname);
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

// Compare number of columns and exit out to the MATLAB layer if incorrect.
void ValidateNumColumns(int64_t actual, int64_t expected) {
  if (actual != expected) {
    mexErrMsgIdAndTxt("MATLAB:arrow:IncorrectNumberOfColumns",
                      "Received only '%d' columns but expected '%d' columns", actual,
                      expected);
  }
}

// Compare number of rows and exit out to the MATLAB layer if incorrect.
void ValidateNumRows(int64_t actual, int64_t expected) {
  if (actual != expected) {
    mexErrMsgIdAndTxt("MATLAB:arrow:IncorrectNumberOfRows",
                      "Received only '%d' rows but expected '%d' rows", actual, expected);
  }
}

// Calculate the number of bytes required in the bit-packed validity buffer.
constexpr int64_t BitPackedLength(int64_t num_elements) {
  // Since mxLogicalArray encodes [0, 1] in a full byte, we can compress that byte
  // down to a bit...therefore dividing the mxLogicalArray length by 8 here.
  return static_cast<int64_t>(std::ceil(num_elements / 8.0));
}

// Calculate the total number of elements in an mxArray
// We have to do this separately since mxGetNumberOfElements only works in numeric arrays
size_t GetNumberOfElements(const mxArray* array) {
  // Get the dimensions and the total number of dimensions from the mxArray*.
  const size_t num_dimensions = mxGetNumberOfDimensions(array);
  const size_t* dimensions = mxGetDimensions(array);

  // Iterate over the dimensions array and accumulate the total number of elements.
  return std::accumulate(dimensions, dimensions + num_dimensions, 1,
                         std::multiplies<size_t>());
}

// Write an mxLogicalArray* into a bit-packed arrow::MutableBuffer
void BitPackBuffer(const mxArray* logical_array,
                   std::shared_ptr<MutableBuffer> packed_buffer) {
  // Error out if the incorrect type is passed in.
  if (!mxIsLogical(logical_array)) {
    mexErrMsgIdAndTxt(
        "MATLAB:arrow:IncorrectType",
        "Expected mxLogical array as input but received mxArray of class '%s'",
        mxGetClassName(logical_array));
  }

  // Validate that the input arrow::Buffer has sufficent size to store a full bit-packed
  // representation of the input mxLogicalArray
  int64_t unpacked_buffer_length = GetNumberOfElements(logical_array);
  if (BitPackedLength(unpacked_buffer_length) > packed_buffer->capacity()) {
    mexErrMsgIdAndTxt("MATLAB:arrow:BufferSizeExceeded",
                      "Buffer of size %d bytes cannot store %d bytes of data",
                      packed_buffer->capacity(), BitPackedLength(unpacked_buffer_length));
  }

  // Get pointers to the internal uint8_t arrays behind arrow::Buffer and mxArray
  uint8_t* packed_buffer_ptr = packed_buffer->mutable_data();
  mxLogical* unpacked_buffer_ptr = mxGetLogicals(logical_array);

  // Iterate over the mxLogical array and write bit-packed bools to the arrow::Buffer.
  // Call into a loop-unrolled Arrow utility for better performance when bit-packing.
  auto generator = [&]() -> uint8_t { return *unpacked_buffer_ptr++; };
  const int64_t start_offset = 0;
  arrow::internal::GenerateBitsUnrolled(packed_buffer_ptr, start_offset,
                                        unpacked_buffer_length, generator);
}

// Write numeric datatypes to the Feather file.
template <typename ArrowDataType>
std::unique_ptr<Array> WriteNumericData(const mxArray* data,
                                        const std::shared_ptr<Buffer> validity_bitmap) {
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
  std::shared_ptr<Buffer> buffer =
      std::make_shared<Buffer>(reinterpret_cast<const uint8_t*>(dt),
                               mxGetElementSize(data) * mxGetNumberOfElements(data));

  // Construct arrow::NumericArray specialization using arrow::Buffer.
  // Pass in nulls information...we could compute and provide the number of nulls here too,
  // but passing -1 for now so that Arrow recomputes it if necessary.
  return std::unique_ptr<Array>(new NumericArray<ArrowDataType>(
      mxGetNumberOfElements(data), buffer, validity_bitmap, -1));
}

// Dispatch MATLAB column data to the correct arrow::Array converter.
std::unique_ptr<Array> WriteVariableData(const mxArray* data, const std::string& type,
                                         const std::shared_ptr<Buffer> validity_bitmap) {
  // Get the underlying type of the mxArray data.
  const mxClassID mxclass = mxGetClassID(data);

  switch (mxclass) {
    case mxSINGLE_CLASS:
      return WriteNumericData<FloatType>(data, validity_bitmap);
    case mxDOUBLE_CLASS:
      return WriteNumericData<DoubleType>(data, validity_bitmap);
    case mxUINT8_CLASS:
      return WriteNumericData<UInt8Type>(data, validity_bitmap);
    case mxUINT16_CLASS:
      return WriteNumericData<UInt16Type>(data, validity_bitmap);
    case mxUINT32_CLASS:
      return WriteNumericData<UInt32Type>(data, validity_bitmap);
    case mxUINT64_CLASS:
      return WriteNumericData<UInt64Type>(data, validity_bitmap);
    case mxINT8_CLASS:
      return WriteNumericData<Int8Type>(data, validity_bitmap);
    case mxINT16_CLASS:
      return WriteNumericData<Int16Type>(data, validity_bitmap);
    case mxINT32_CLASS:
      return WriteNumericData<Int32Type>(data, validity_bitmap);
    case mxINT64_CLASS:
      return WriteNumericData<Int64Type>(data, validity_bitmap);

    default: {
      mexErrMsgIdAndTxt("MATLAB:arrow:UnsupportedArrowType",
                        "Unsupported arrow::Type '%s' for variable '%s'",
                        mxGetClassName(data), type.c_str());
    }
  }

  // We shouldn't ever reach this branch, but if we do, return nullptr.
  return nullptr;
}

}  // namespace internal

Status FeatherWriter::Open(const std::string& filename,
                           std::shared_ptr<FeatherWriter>* feather_writer) {
  // Allocate shared_ptr out parameter.
  *feather_writer = std::shared_ptr<FeatherWriter>(new FeatherWriter());

  // Open a FileOutputStream corresponding to the provided filename.
  std::shared_ptr<io::OutputStream> writable_file(nullptr);
  ARROW_RETURN_NOT_OK(io::FileOutputStream::Open(filename, &writable_file));

  // TableWriter::Open expects a shared_ptr to an OutputStream.
  // Open the Feather file for writing with a TableWriter.
  return ipc::feather::TableWriter::Open(writable_file,
                                         &(*feather_writer)->table_writer_);
}

// Write table metadata to the Feather file from a mxArray*.
void FeatherWriter::WriteMetadata(const mxArray* metadata) {
  // Verify that all required fieldnames are provided.
  internal::ValidateMxStructField(metadata, "Description", mxCHAR_CLASS, true);
  internal::ValidateMxStructField(metadata, "NumRows", mxDOUBLE_CLASS, false);
  internal::ValidateMxStructField(metadata, "NumVariables", mxDOUBLE_CLASS, false);

  // Convert Description to a std::string and set on FeatherWriter and TableWriter.
  std::string description =
      internal::MxArrayToString(mxGetField(metadata, 0, "Description"));
  this->description_ = description;
  this->table_writer_->SetDescription(description);

  // Get the NumRows field in the struct array and set on TableWriter.
  this->num_rows_ = static_cast<int64_t>(mxGetScalar(mxGetField(metadata, 0, "NumRows")));
  this->table_writer_->SetNumRows(this->num_rows_);

  // Get the total number of variables. This is checked later for consistency with
  // the provided number of columns before finishing the file write.
  this->num_variables_ =
      static_cast<int64_t>(mxGetScalar(mxGetField(metadata, 0, "NumVariables")));
}

// Write mxArrays from MATLAB into a Feather file.
Status FeatherWriter::WriteVariables(const mxArray* variables) {
  // Verify that all required fieldnames are provided.
  internal::ValidateMxStructField(variables, "Name", mxCHAR_CLASS, true);
  internal::ValidateMxStructField(variables, "Type", mxCHAR_CLASS, false);
  internal::ValidateMxStructField(variables, "Data", mxUNKNOWN_CLASS, true);
  internal::ValidateMxStructField(variables, "Valid", mxLOGICAL_CLASS, true);

  // Get the number of columns in the struct array.
  size_t num_columns = internal::GetNumberOfElements(variables);

  // Verify that we have all the columns required for writing
  // Currently we need all columns to be passed in together in the WriteVariables method.
  internal::ValidateNumColumns(static_cast<int64_t>(num_columns), this->num_variables_);

  // Allocate a packed validity bitmap for later arrow::Buffers to reference and populate.
  // Since this is defined in the enclosing scope around any arrow::Buffer usage, this
  // should outlive any arrow::Buffers created on this range, thus avoiding dangling
  // references.
  std::shared_ptr<ResizableBuffer> validity_bitmap;
  ARROW_RETURN_NOT_OK(AllocateResizableBuffer(internal::BitPackedLength(this->num_rows_),
                                              &validity_bitmap));

  // Iterate over the input columns and generate arrow arrays.
  for (int idx = 0; idx < num_columns; ++idx) {
    // Unwrap constituent mxArray*s from the mxStructArray*. This is safe since we
    // already checked for existence and non-nullness of these types.
    const mxArray* name = mxGetField(variables, idx, "Name");
    const mxArray* data = mxGetField(variables, idx, "Data");
    const mxArray* type = mxGetField(variables, idx, "Type");
    const mxArray* valid = mxGetField(variables, idx, "Valid");

    // Convert column and type name to a std::string from mxArray*.
    std::string name_str = internal::MxArrayToString(name);
    std::string type_str = internal::MxArrayToString(type);

    // Populate bit-packed arrow::Buffer using validity data in the mxArray*.
    internal::BitPackBuffer(valid, validity_bitmap);

    // Wrap mxArray data in an arrow::Array of the equivalent type.
    std::unique_ptr<Array> array =
        internal::WriteVariableData(data, type_str, validity_bitmap);

    // Verify that the arrow::Array has the right number of elements.
    internal::ValidateNumRows(array->length(), this->num_rows_);

    // Write another column to the Feather file.
    ARROW_RETURN_NOT_OK(this->table_writer_->Append(name_str, *array));
  }

  // Write the Feather file metadata to the end of the file.
  return this->table_writer_->Finalize();
}

}  // namespace matlab
}  // namespace arrow

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

#include "feather_writer.h"

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/io/file.h>
#include <arrow/ipc/feather.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/util/bit_util.h>
#include <arrow/util/bitmap_generate.h>
#include <arrow/util/key_value_metadata.h>
#include <mex.h>

#include "matlab_traits.h"
#include "util/handle_status.h"

namespace arrow {
namespace matlab {
namespace internal {

// Returns the arrow::DataType that corresponds to the input type string
std::shared_ptr<arrow::DataType> ConvertMatlabTypeStringToArrowDataType(
    const std::string& t) {
  if (t == "double") {
    return arrow::float64();
  } else if (t == "single") {
    return arrow::float32();
  } else if (t == "uint64") {
    return arrow::uint64();
  } else if (t == "uint32") {
    return arrow::uint32();
  } else if (t == "uint16") {
    return arrow::uint16();
  } else if (t == "uint8") {
    return arrow::uint8();
  } else if (t == "int64") {
    return arrow::int64();
  } else if (t == "int32") {
    return arrow::int32();
  } else if (t == "int16") {
    return arrow::int16();
  } else if (t == "int8") {
    return arrow::int8();
  }
  mexErrMsgIdAndTxt("MATLAB:arrow:UnsupportedMatlabTypeString",
                    "Unsupported MATLAB type string: '%s'", t.c_str());

  // mexErrMsgIdAndTxt throws unconditionally so we should never reach this line
  return nullptr;
}

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
  // Some struct fields (like Data) can be empty, while others
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
int64_t BitPackedLength(int64_t num_elements) {
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
  return std::accumulate(dimensions, dimensions + num_dimensions, size_t{1},
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

  // Validate that the input arrow::Buffer has sufficient size to store a full bit-packed
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
  auto generator = [&]() -> bool { return *(unpacked_buffer_ptr++); };
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
  // Pass in nulls information...we could compute and provide the number of nulls here
  // too, but passing -1 for now so that Arrow recomputes it if necessary.
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
  ARROW_ASSIGN_OR_RAISE((*feather_writer)->file_output_stream_,
      io::FileOutputStream::Open(filename, &((*feather_writer)->file_output_stream_)));
  return Status::OK();
}

// Write mxArrays from MATLAB into a Feather file.
Status FeatherWriter::WriteVariables(const mxArray* variables, const mxArray* metadata) {
  // Verify that all required fieldnames are provided.
  internal::ValidateMxStructField(variables, "Name", mxCHAR_CLASS, true);
  internal::ValidateMxStructField(variables, "Type", mxCHAR_CLASS, false);
  internal::ValidateMxStructField(variables, "Data", mxUNKNOWN_CLASS, true);
  internal::ValidateMxStructField(variables, "Valid", mxLOGICAL_CLASS, true);

  // Verify that all required fieldnames are provided.
  internal::ValidateMxStructField(metadata, "NumRows", mxDOUBLE_CLASS, false);
  internal::ValidateMxStructField(metadata, "NumVariables", mxDOUBLE_CLASS, false);

  // Get the number of columns in the struct array.
  size_t num_columns = internal::GetNumberOfElements(variables);

  // Get the NumRows field in the struct array and set on TableWriter.
  num_rows_ = static_cast<int64_t>(mxGetScalar(mxGetField(metadata, 0, "NumRows")));
  // Get the total number of variables. This is checked later for consistency with
  // the provided number of columns before finishing the file write.
  num_variables_ =
      static_cast<int64_t>(mxGetScalar(mxGetField(metadata, 0, "NumVariables")));

  // Verify that we have all the columns required for writing
  // Currently we need all columns to be passed in together in the WriteVariables method.
  internal::ValidateNumColumns(static_cast<int64_t>(num_columns), num_variables_);

  arrow::SchemaBuilder schema_builder;
  std::vector<std::shared_ptr<arrow::Array>> table_columns;

  const int64_t bitpacked_length = internal::BitPackedLength(num_rows_);

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

    auto datatype = internal::ConvertMatlabTypeStringToArrowDataType(type_str);
    auto field = std::make_shared<arrow::Field>(name_str, datatype);

    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ResizableBuffer> validity_bitmap,
        arrow::AllocateResizableBuffer(internal::BitPackedLength(num_rows_)));

    // Populate bit-packed arrow::Buffer using validity data in the mxArray*.
    internal::BitPackBuffer(valid, validity_bitmap);

    // Wrap mxArray data in an arrow::Array of the equivalent type.
    auto array =
        internal::WriteVariableData(data, type_str, validity_bitmap);

    // Verify that the arrow::Array has the right number of elements.
    internal::ValidateNumRows(array->length(), num_rows_);

    // Append the field to the schema builder
    RETURN_NOT_OK(schema_builder.AddField(field));

    // Store the table column
    table_columns.push_back(std::move(array));
  }
  // Create the table schema
  ARROW_ASSIGN_OR_RAISE(auto table_schema, schema_builder.Finish());

  // Specify the feather file format version as V1
  arrow::ipc::feather::WriteProperties write_props;
  write_props.version = arrow::ipc::feather::kFeatherV1Version;

  std::shared_ptr<arrow::Table> table = arrow::Table::Make(table_schema, table_columns);
  // Write the Feather file metadata to the end of the file.
  return ipc::feather::WriteTable(*table, file_output_stream_.get(), write_props);
}

}  // namespace matlab
}  // namespace arrow

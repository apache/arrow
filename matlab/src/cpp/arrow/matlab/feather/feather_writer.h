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

#pragma once

#include <arrow/ipc/feather.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <matrix.h>

#include <memory>
#include <string>

namespace arrow {
namespace matlab {
namespace feather {

class FeatherWriter {
 public:
  ~FeatherWriter() = default;

  /// \brief Write mxArrays to a Feather file. The first input must be a N-by-1 mxStruct
  ///         array with the following fields:
  ///         - "Name" :: Nx1 mxChar array, name of the column
  ///         - "Type" :: Nx1 mxChar array, the variable's MATLAB datatype
  ///         - "Data" :: Nx1 mxArray, data for this variable
  ///         - "Valid" :: Nx1 mxLogical array, 0 represents invalid (null) values and
  ///                                           1 represents valid (non-null) values
  ///        The second input must be a scalar mxStruct  with the following
  ///        fields:
  ///         - "NumRows" :: scalar mxDouble array, number of rows in table
  ///         - "NumVariables" :: scalar mxDouble array, total number of variables
  /// \param[in] variables mxArray* struct array containing table variable data
  /// \param[in] metadata mxArray* scalar struct containing table-level metadata
  /// \return status
  Status WriteVariables(const mxArray* variables, const mxArray* metadata);

  /// \brief Initialize a FeatherWriter object that writes to a Feather file
  /// \param[in] filename path to the new Feather file
  /// \param[out] feather_writer uninitialized FeatherWriter object
  /// \return status
  static Status Open(const std::string& filename,
                     std::shared_ptr<FeatherWriter>* feather_writer);

 private:
  FeatherWriter() = default;

  int64_t num_rows_;
  int64_t num_variables_;
  std::string description_;
  std::shared_ptr<arrow::io::OutputStream> file_output_stream_;
};

}  // namespace feather
}  // namespace matlab
}  // namespace arrow
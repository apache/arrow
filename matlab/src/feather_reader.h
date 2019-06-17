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

#ifndef ARROW_MATLAB_FEATHER_READER_H
#define ARROW_MATLAB_FEATHER_READER_H

#include <memory>
#include <string>

#include <arrow/ipc/feather.h>
#include <arrow/status.h>
#include <arrow/type.h>

#include <matrix.h>

namespace arrow {
namespace matlab {

class FeatherReader {
 public:
  ~FeatherReader() = default;

  /// \brief Read the table metadata as an mxArray* struct from the given
  ///        Feather file.
  ///        The returned mxArray* struct contains the following fields:
  ///         - "Description"  :: Nx1 mxChar array, table-level description
  ///         - "NumRows"      :: scalar mxDouble array, number of rows in the
  ///                             table
  ///         - "NumVariables" :: scalar mxDouble array, number of variables in
  ///                             the table
  ///        Clients are responsible for freeing the returned mxArray memory
  ///        when it is no longer needed, or passing it to MATLAB to be managed.
  /// \return metadata mxArray* scalar struct containing table level metadata
  mxArray* ReadMetadata() const;

  /// \brief Read the table variable data as an mxArray* struct array from the
  ///        given Feather file.
  ///        The returned mxArray* struct array has the following fields:
  ///         - "Name"  :: Nx1 mxChar array, name of the variable
  ///         - "Type"  :: Nx1 mxChar array, the variable's Arrow datatype
  ///         - "Data"  :: Nx1 mxArray, data for the variable
  ///         - "Valid" :: Nx1 mxLogical array, validity (null) bitmap
  ///        Clients are responsible for freeing the returned mxArray memory
  ///        when it is no longer needed, or passing it to MATLAB to be managed.
  /// \return variables mxArray* struct array containing table variable data
  mxArray* ReadVariables() const;

  /// \brief Initialize a FeatherReader object from a given Feather file.
  /// \param[in] filename path to a Feather file
  /// \param[out] feather_reader uninitialized FeatherReader object
  static Status Open(const std::string& filename,
                     std::shared_ptr<FeatherReader>* feather_reader);

 private:
  FeatherReader() = default;
  std::unique_ptr<ipc::feather::TableReader> table_reader_;
  int64_t num_rows_;
  int64_t num_variables_;
  std::string description_;
};

}  // namespace matlab
}  // namespace arrow

#endif  // ARROW_MATLAB_FEATHER_READER_H

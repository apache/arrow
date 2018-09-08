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

#ifndef MLARROW_FEATHER_READER_H
#define MLARROW_FEATHER_READER_H

#include <memory>
#include <string>

#include <arrow/ipc/feather.h>
#include <arrow/status.h>
#include <arrow/type.h>

#include <matrix.h>

namespace mlarrow {

class FeatherReader {
 public:
  ~FeatherReader() = default;

  /// \brief Read the table metadata as a mxArray* struct from the given Feather file.
  ///        The returned struct includes fields describing the number of rows
  ///        in the table (NumRows), the number of variables (NumVariables), the
  ///        Feather file version (Version), and the table description (Description).
  ///        Callers are responsible for freeing the returned mxArray memory
  ///        when it is no longer needed, or passing it to MATLAB to be managed.
  /// \return metadata mxArray* scalar struct containing table level metadata
  mxArray* ReadMetadata() const;

  /// \brief Read the table metadata as a mxArray* struct array from the given
  ///        Feather file. Each struct includes fields for variable data (Data),
  ///        null values (Nulls), variable name (Name), and original Feather
  ///        datatype (Type). Callers are responsible for freeing the returned
  ///        mxArray memory when it is no longer needed, or passing it to MATLAB
  ///        to be managed.
  /// \return variables mxArray* struct array containing table variable data
  mxArray* ReadVariables() const;

  /// \brief Initialize a FeatherReader object from a given Feather file.
  /// \param[in] filename path to a Feather file
  /// \param[out] feather_reader uninitialized FeatherReader object
  static arrow::Status Open(const std::string& filename,
                            std::shared_ptr<FeatherReader>* feather_reader);

 private:
  FeatherReader() = default;
  std::unique_ptr<arrow::ipc::feather::TableReader> table_reader_;
  int64_t num_rows_;
  int64_t num_variables_;
  std::string description_;
  int version_;
};

}  // namespace mlarrow

#endif  // MLARROW_FEATHER_READER_H

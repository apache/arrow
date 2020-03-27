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

#include <memory>

#include "arrow/status.h"
#include "arrow/util/visibility.h"

namespace arrow {

class RecordBatch;
class Schema;

/// \class Receiver
/// \brief A general receiver class to receive objects.
///
/// You must implement receiver methods for objects you want to receive.
class ARROW_EXPORT Receiver {
 public:
  virtual ~Receiver() = default;

  /// \brief Called when end-of-stream is received.
  ///
  /// The default implementation just returns arrow::Status::OK().
  ///
  /// \return Status
  ///
  /// \see RecordBatchStreamEmitter
  virtual Status EosReceived();

  /// \brief Called when a record batch is received.
  ///
  /// The default implementation just returns
  /// arrow::Status::NotImplemented().
  ///
  /// \param[in] record_batch a record batch received
  /// \return Status
  ///
  /// \see RecordBatchStreamEmitter
  virtual Status RecordBatchReceived(std::shared_ptr<RecordBatch> record_batch);

  /// \brief Called when a schema is received.
  ///
  /// The default implementation just returns arrow::Status::OK().
  ///
  /// \param[in] schema a schema received
  /// \return Status
  ///
  /// \see RecordBatchStreamEmitter
  virtual Status SchemaReceived(std::shared_ptr<Schema> schema);
};

}  // namespace arrow

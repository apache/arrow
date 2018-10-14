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
#include <string>
#include <vector>

#include "arrow/dbi/hiveserver2/columnar-row-set.h"
#include "arrow/dbi/hiveserver2/types.h"

#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Status;

namespace hiveserver2 {

struct ThriftRPC;

// Maps directly to TFetchOrientation in the HiveServer2 interface.
enum class FetchOrientation {
  NEXT,      // supported
  PRIOR,     // not supported
  RELATIVE,  // not supported
  ABSOLUTE,  // not supported
  FIRST,     // supported if query result caching is enabled in Impala
  LAST       // not supported
};

// Represents a single HiveServer2 operation. Used to monitor the status of an operation
// and to retrieve its results. The only Operation function that will block is Fetch,
// which blocks if there aren't any results ready yet.
//
// Operations are created using Session functions, eg. ExecuteStatement. They must
// have Close called on them before they can be deleted.
//
// This class is not thread-safe.
class ARROW_EXPORT Operation {
 public:
  // Maps directly to TOperationState in the HiveServer2 interface.
  enum class State {
    INITIALIZED,
    RUNNING,
    FINISHED,
    CANCELED,
    CLOSED,
    ERROR,
    UNKNOWN,
    PENDING,
  };

  ~Operation();

  // Fetches the current state of this operation. If successful, sets the operation state
  // in 'out' and returns an OK status, otherwise an error status is returned. May be
  // called after successfully creating the operation and before calling Close.
  Status GetState(Operation::State* out) const;

  // May be called after successfully creating the operation and before calling Close.
  Status GetLog(std::string* out) const;

  // May be called after successfully creating the operation and before calling Close.
  Status GetProfile(std::string* out) const;

  // Fetches metadata for the columns in the output of this operation, such as the
  // names and types of the columns, and returns it as a list of column descriptions.
  // May be called after successfully creating the operation and before calling Close.
  Status GetResultSetMetadata(std::vector<ColumnDesc>* column_descs) const;

  // Fetches a batch of results, stores them in 'results', and sets has_more_rows.
  // Fetch will block if there aren't any results that are ready.
  Status Fetch(std::unique_ptr<ColumnarRowSet>* results, bool* has_more_rows) const;
  Status Fetch(int max_rows, FetchOrientation orientation,
               std::unique_ptr<ColumnarRowSet>* results, bool* has_more_rows) const;

  // May be called after successfully creating the operation and before calling Close.
  Status Cancel() const;

  // Closes the operation. Must be called before the operation is deleted. May be safely
  // called on an invalid or already closed operation - will only return an error if the
  // operation is open but the close rpc fails.
  Status Close();

  // May be called after successfully creating the operation and before calling Close.
  bool HasResultSet() const;

  // Returns true iff this operation's results will be returned in a columnar format.
  // May be called at any time.
  bool IsColumnar() const;

 protected:
  // Hides Thrift objects from the header.
  struct OperationImpl;

  explicit Operation(const std::shared_ptr<ThriftRPC>& rpc);

  std::unique_ptr<OperationImpl> impl_;
  std::shared_ptr<ThriftRPC> rpc_;

  // True iff this operation has been successfully created and has not been closed yet,
  // corresponding to when the operation has a valid operation handle.
  bool open_;

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(Operation);
};

}  // namespace hiveserver2
}  // namespace arrow

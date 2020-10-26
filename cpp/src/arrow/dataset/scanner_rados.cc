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

#include "arrow/dataset/scanner_rados.h"
#include "arrow/dataset/rados_utils.h"
#include "arrow/dataset/filter.h"
#include "arrow/table.h"
#include "arrow/util/iterator.h"

namespace arrow {
namespace dataset {

Result<RecordBatchIterator> RadosScanTask::Execute() {   
  librados::bufferlist in, out;

  /// Serialize the filter Expression and projection Schema into 
  /// a librados bufferlist. 
  ARROW_RETURN_NOT_OK(serialize_scan_request_to_bufferlist(
    options_->filter,
    options_->projector.schema(),
    in
  ));

  /// Trigger a CLS function and pass the serialized operations
  /// down to the storage. The resultant Table will be available inside the `out`
  /// bufferlist subsequently.
  int e = rados_options_->io_ctx_interface_->exec(object_->id(), 
                                                  rados_options_->cls_name_.c_str(), 
                                                  rados_options_->cls_method_.c_str(), 
                                                  in, out);
  if (e != 0) {
    return Status::ExecutionError("call to exec() returned non-zero exit code.");
  }

  /// Deserialize the result Table from the `out` bufferlist.
  std::shared_ptr<Table> result_table;
  ARROW_RETURN_NOT_OK(deserialize_table_from_bufferlist(&result_table, out));

  /// Verify whether the Schema of the resultant Table is what was asked for,
  if (!options_->schema()->Equals(*(result_table->schema()))) {
    return Status::Invalid("the schema of the result table doesn't match the schema of the requested projection.");
  }

  /// Read the result Table into a RecordBatchVector to return to the RadosScanTask
  auto table_reader = std::make_shared<TableBatchReader>(*result_table);
  RecordBatchVector batches;
  ARROW_RETURN_NOT_OK(table_reader->ReadAll(&batches));
  return MakeVectorIterator(batches);
}

} // namespace dataset
} // namespace arrow
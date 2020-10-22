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

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/util/macros.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/dataset/rados.h"
#include "arrow/api.h"
#include "arrow/ipc/api.h"
#include "arrow/dataset/api.h"
#include "arrow/io/api.h"

namespace arrow {
namespace dataset {

class ARROW_DS_EXPORT ScanRequest {
 public:
  std::shared_ptr<Expression> filter;
  std::shared_ptr<RecordBatchProjector> projector;
  int64_t batch_size;
  int64_t seq_num;
};

ARROW_DS_EXPORT Result<std::shared_ptr<RecordBatch>> wrap_rados_scan_request(
            std::shared_ptr<Expression> filter,
            const Schema& projection_schema,
            int64_t batch_size,
            int64_t seq_num
        );

ARROW_DS_EXPORT Result<ScanRequest> unwrap_rados_scan_request(
        std::shared_ptr<RecordBatch> request
);

ARROW_DS_EXPORT Status int64_to_char(uint8_t *num_buffer, int64_t num);
ARROW_DS_EXPORT Status char_to_int64(uint8_t *num_buffer, int64_t &num);

ARROW_DS_EXPORT Status serialize_scan_request_to_bufferlist(
        std::shared_ptr<Expression> filter, 
        std::shared_ptr<Schema> schema,
        librados::bufferlist &bl);

ARROW_DS_EXPORT Status deserialize_scan_request_from_bufferlist(
        std::shared_ptr<Expression> *filter, 
        std::shared_ptr<Schema> *schema,
        librados::bufferlist bl);

ARROW_DS_EXPORT Status read_table_from_bufferlist(
        std::shared_ptr<Table> *table,
        librados::bufferlist bl);

ARROW_DS_EXPORT Status write_table_to_bufferlist(
        std::shared_ptr<Table> &table,
        librados::bufferlist &bl);

ARROW_DS_EXPORT Status scan_batches(
	std::shared_ptr<Expression> &filter,
	std::shared_ptr<Schema> &schema,
	RecordBatchVector &batches,
	std::shared_ptr<Table> *table);

ARROW_DS_EXPORT Status extract_batches_from_bufferlist(
	RecordBatchVector *batches,
	librados::bufferlist &bl);

}  // namespace dataset
}  // namespace arrow
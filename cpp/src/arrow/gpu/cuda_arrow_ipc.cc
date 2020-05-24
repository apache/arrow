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

#include "arrow/gpu/cuda_arrow_ipc.h"

#include <cstdint>
#include <memory>
#include <sstream>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/ipc/dictionary.h"
#include "arrow/ipc/message.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/util/visibility.h"

#include "generated/Message_generated.h"

#include "arrow/gpu/cuda_context.h"
#include "arrow/gpu/cuda_memory.h"

namespace arrow {

namespace flatbuf = org::apache::arrow::flatbuf;

namespace cuda {

Result<std::shared_ptr<CudaBuffer>> SerializeRecordBatch(const RecordBatch& batch,
                                                         CudaContext* ctx) {
  ARROW_ASSIGN_OR_RAISE(auto buf,
                        ipc::SerializeRecordBatch(batch, ctx->memory_manager()));
  return CudaBuffer::FromBuffer(buf);
}

Result<std::shared_ptr<RecordBatch>> ReadRecordBatch(
    const std::shared_ptr<Schema>& schema, const ipc::DictionaryMemo* dictionary_memo,
    const std::shared_ptr<CudaBuffer>& buffer, MemoryPool* pool) {
  CudaBufferReader cuda_reader(buffer);

  // The pool is only used for metadata allocation
  ARROW_ASSIGN_OR_RAISE(auto message, ipc::ReadMessage(&cuda_reader, pool));
  if (!message) {
    return Status::Invalid("End of stream (message has length 0)");
  }

  // Zero-copy read on device memory
  return ipc::ReadRecordBatch(*message, schema, dictionary_memo,
                              ipc::IpcReadOptions::Defaults());
}

}  // namespace cuda
}  // namespace arrow

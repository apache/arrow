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
#include "arrow/ipc/Message_generated.h"
#include "arrow/ipc/message.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/util/visibility.h"

#include "arrow/gpu/cuda_context.h"
#include "arrow/gpu/cuda_memory.h"

namespace arrow {

namespace flatbuf = org::apache::arrow::flatbuf;

namespace gpu {

Status SerializeRecordBatch(const RecordBatch& batch, CudaContext* ctx,
                            std::shared_ptr<CudaBuffer>* out) {
  int64_t size = 0;
  RETURN_NOT_OK(ipc::GetRecordBatchSize(batch, &size));

  std::shared_ptr<CudaBuffer> buffer;
  RETURN_NOT_OK(ctx->Allocate(size, &buffer));

  CudaBufferWriter stream(buffer);

  // Use 8MB buffering, which yields generally good performance
  RETURN_NOT_OK(stream.SetBufferSize(1 << 23));

  // We use the default memory pool here since any allocations are ephemeral
  RETURN_NOT_OK(ipc::SerializeRecordBatch(batch, default_memory_pool(), &stream));
  RETURN_NOT_OK(stream.Close());
  *out = buffer;
  return Status::OK();
}

Status ReadMessage(CudaBufferReader* reader, MemoryPool* pool,
                   std::unique_ptr<ipc::Message>* out) {
  int32_t message_length = 0;
  int64_t bytes_read = 0;

  RETURN_NOT_OK(reader->Read(sizeof(int32_t), &bytes_read,
                             reinterpret_cast<uint8_t*>(&message_length)));
  if (bytes_read != sizeof(int32_t)) {
    *out = nullptr;
    return Status::OK();
  }

  if (message_length == 0) {
    // Optional 0 EOS control message
    *out = nullptr;
    return Status::OK();
  }

  std::shared_ptr<MutableBuffer> metadata;
  RETURN_NOT_OK(AllocateBuffer(pool, message_length, &metadata));
  RETURN_NOT_OK(reader->Read(message_length, &bytes_read, metadata->mutable_data()));
  if (bytes_read != message_length) {
    std::stringstream ss;
    ss << "Expected " << message_length << " metadata bytes, but only got " << bytes_read;
    return Status::IOError(ss.str());
  }

  return ipc::Message::ReadFrom(metadata, reader, out);
}

Status ReadRecordBatch(const std::shared_ptr<Schema>& schema,
                       const std::shared_ptr<CudaBuffer>& buffer, MemoryPool* pool,
                       std::shared_ptr<RecordBatch>* out) {
  CudaBufferReader cuda_reader(buffer);

  std::unique_ptr<ipc::Message> message;
  RETURN_NOT_OK(ReadMessage(&cuda_reader, pool, &message));

  if (!message) {
    return Status::Invalid("Message is length 0");
  }

  // Zero-copy read on device memory
  return ipc::ReadRecordBatch(*message, schema, out);
}

}  // namespace gpu
}  // namespace arrow

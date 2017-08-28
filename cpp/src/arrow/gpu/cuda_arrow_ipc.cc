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

#include <cstdint>
#include <memory>

#include "arrow/buffer.h"
#include "arrow/ipc/message.h"
#include "arrow/ipc/writer.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/util/visibility.h"

#include "arrow/gpu/cuda_context.h"
#include "arrow/gpu/cuda_memory.h"

namespace arrow {
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
  RETURN_NOT_OK(ipc::SerializeRecordBatch(batch, default_memory_pool(),
                                          &stream));
  *out = buffer;
  return Status::OK();
}

Status ReadMessage(CudaBufferReader* stream, MemoryPool* pool,
                   std::unique_ptr<Message>* message) {
  uint8_t length_buf[4] = {0};

  int64_t bytes_read = 0;
  RETURN_NOT_OK(file->Read(sizeof(int32_t), &bytes_read, length_buf));
  if (bytes_read != sizeof(int32_t)) {
    *message = nullptr;
    return Status::OK();
  }

  const int32_t metadata_length = *reinterpret_cast<const int32_t*>(length_buf);

  if (metadata_length == 0) {
    // Optional 0 EOS control message
    *message = nullptr;
    return Status::OK();
  }

  std::shared_ptr<MutableBuffer> metadata;
  RETURN_NOT_OK(AllocateBuffer(pool, metadata_length, &metadata));
  RETURN_NOT_OK(file->Read(message_length, &bytes_read, metadata->mutable_data()));
  if (bytes_read != metadata_length) {
    return Status::IOError("Unexpected end of stream trying to read message");
  }

  auto fb_message = flatbuf::GetMessage(metadata->data());

  int64_t body_length = fb_message->bodyLength();

  // Zero copy
  std::shared_ptr<Buffer> body;
  RETURN_NOT_OK(stream->Read(body_length, &body));
  if (body->size() < body_length) {
    std::stringstream ss;
    ss << "Expected to be able to read " << body_length << " bytes for message body, got "
       << body->size();
    return Status::IOError(ss.str());
  }

  return Message::Open(metadata, body, message);
}

}  // namespace gpu
}  // namespace arrow

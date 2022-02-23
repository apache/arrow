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

#include <gtest/gtest.h>

#include <memory>

#include "arrow/array.h"
#include "arrow/flight/client.h"
#include "arrow/flight/server.h"
#include "arrow/flight/test_util.h"
#include "arrow/gpu/cuda_api.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace flight {

Status CheckBuffersOnDevice(const Array& array, const Device& device) {
  if (array.num_fields() != 0) {
    return Status::NotImplemented("Nested arrays");
  }
  for (const auto& buffer : array.data()->buffers) {
    if (!buffer) continue;
    if (!buffer->device()->Equals(device)) {
      return Status::Invalid("Expected buffer on device: ", device.ToString(),
                             ". Was allocated on device: ", buffer->device()->ToString());
    }
  }
  return Status::OK();
}

// Copy a record batch to host memory.
arrow::Result<std::shared_ptr<RecordBatch>> CopyBatchToHost(const RecordBatch& batch) {
  auto mm = CPUDevice::Instance()->default_memory_manager();
  ArrayVector arrays;
  for (const auto& column : batch.columns()) {
    std::shared_ptr<ArrayData> data = column->data()->Copy();
    if (data->child_data.size() != 0) {
      return Status::NotImplemented("Nested arrays");
    }

    for (size_t i = 0; i < data->buffers.size(); i++) {
      const auto& buffer = data->buffers[i];
      if (!buffer || buffer->is_cpu()) continue;
      ARROW_ASSIGN_OR_RAISE(data->buffers[i], Buffer::Copy(buffer, mm));
    }
    arrays.push_back(MakeArray(data));
  }
  return RecordBatch::Make(batch.schema(), batch.num_rows(), std::move(arrays));
}

class CudaTestServer : public FlightServerBase {
 public:
  explicit CudaTestServer(std::shared_ptr<Device> device) : device_(std::move(device)) {}

  Status DoGet(const ServerCallContext&, const Ticket&,
               std::unique_ptr<FlightDataStream>* data_stream) override {
    RecordBatchVector batches;
    RETURN_NOT_OK(ExampleIntBatches(&batches));
    ARROW_ASSIGN_OR_RAISE(auto batch_reader, RecordBatchReader::Make(batches));
    *data_stream = std::unique_ptr<FlightDataStream>(new RecordBatchStream(batch_reader));
    return Status::OK();
  }

  Status DoPut(const ServerCallContext&, std::unique_ptr<FlightMessageReader> reader,
               std::unique_ptr<FlightMetadataWriter> writer) override {
    RecordBatchVector batches;
    RETURN_NOT_OK(reader->ReadAll(&batches));
    for (const auto& batch : batches) {
      for (const auto& column : batch->columns()) {
        RETURN_NOT_OK(CheckBuffersOnDevice(*column, *device_));
      }
    }
    return Status::OK();
  }

  Status DoExchange(const ServerCallContext& context,
                    std::unique_ptr<FlightMessageReader> reader,
                    std::unique_ptr<FlightMessageWriter> writer) override {
    FlightStreamChunk chunk;
    bool begun = false;
    while (true) {
      RETURN_NOT_OK(reader->Next(&chunk));
      if (!chunk.data) break;
      if (!begun) {
        begun = true;
        RETURN_NOT_OK(writer->Begin(chunk.data->schema()));
      }
      for (const auto& column : chunk.data->columns()) {
        RETURN_NOT_OK(CheckBuffersOnDevice(*column, *device_));
      }
      RETURN_NOT_OK(writer->WriteRecordBatch(*chunk.data));
    }
    return Status::OK();
  }

 private:
  std::shared_ptr<Device> device_;
};

class TestCuda : public ::testing::Test {
 public:
  void SetUp() {
    ASSERT_OK_AND_ASSIGN(manager_, cuda::CudaDeviceManager::Instance());
    ASSERT_OK_AND_ASSIGN(device_, manager_->GetDevice(0));
    ASSERT_OK_AND_ASSIGN(context_, device_->GetContext());

    ASSERT_OK(MakeServer<CudaTestServer>(
        &server_, &client_,
        [this](FlightServerOptions* options) {
          options->memory_manager = device_->default_memory_manager();
          return Status::OK();
        },
        [](FlightClientOptions* options) { return Status::OK(); }, device_));
  }
  void TearDown() { ASSERT_OK(server_->Shutdown()); }

 protected:
  cuda::CudaDeviceManager* manager_;
  std::shared_ptr<cuda::CudaDevice> device_;
  std::shared_ptr<cuda::CudaContext> context_;

  std::unique_ptr<FlightClient> client_;
  std::unique_ptr<FlightServerBase> server_;
};

TEST_F(TestCuda, DoGet) {
  // Check that we can allocate the results of DoGet with a custom
  // memory manager.
  FlightCallOptions options;
  options.memory_manager = device_->default_memory_manager();

  Ticket ticket{""};
  std::unique_ptr<FlightStreamReader> stream;
  ASSERT_OK(client_->DoGet(options, ticket, &stream));
  std::shared_ptr<Table> table;
  ASSERT_OK(stream->ReadAll(&table));

  for (const auto& column : table->columns()) {
    for (const auto& chunk : column->chunks()) {
      ASSERT_OK(CheckBuffersOnDevice(*chunk, *device_));
    }
  }
}

TEST_F(TestCuda, DoPut) {
  // Check that we can send a record batch containing references to
  // GPU buffers.
  RecordBatchVector batches;
  ASSERT_OK(ExampleIntBatches(&batches));

  std::unique_ptr<FlightStreamWriter> writer;
  std::unique_ptr<FlightMetadataReader> reader;
  auto descriptor = FlightDescriptor::Path({""});
  ASSERT_OK(client_->DoPut(descriptor, batches[0]->schema(), &writer, &reader));

  ipc::DictionaryMemo memo;
  for (const auto& batch : batches) {
    ASSERT_OK_AND_ASSIGN(auto buffer, cuda::SerializeRecordBatch(*batch, context_.get()));
    ASSERT_OK_AND_ASSIGN(auto cuda_batch,
                         cuda::ReadRecordBatch(batch->schema(), &memo, buffer));

    for (const auto& column : cuda_batch->columns()) {
      ASSERT_OK(CheckBuffersOnDevice(*column, *device_));
    }

    ASSERT_OK(writer->WriteRecordBatch(*cuda_batch));
  }
  ASSERT_OK(writer->Close());
}

TEST_F(TestCuda, DoExchange) {
  // Check that we can send a record batch containing references to
  // GPU buffers.
  FlightCallOptions options;
  options.memory_manager = device_->default_memory_manager();

  RecordBatchVector batches;
  ASSERT_OK(ExampleIntBatches(&batches));

  std::unique_ptr<FlightStreamWriter> writer;
  std::unique_ptr<FlightStreamReader> reader;
  auto descriptor = FlightDescriptor::Path({""});
  ASSERT_OK(client_->DoExchange(options, descriptor, &writer, &reader));
  ASSERT_OK(writer->Begin(batches[0]->schema()));

  ipc::DictionaryMemo write_memo;
  ipc::DictionaryMemo read_memo;
  for (const auto& batch : batches) {
    ASSERT_OK_AND_ASSIGN(auto buffer, cuda::SerializeRecordBatch(*batch, context_.get()));
    ASSERT_OK_AND_ASSIGN(auto cuda_batch,
                         cuda::ReadRecordBatch(batch->schema(), &write_memo, buffer));

    for (const auto& column : cuda_batch->columns()) {
      ASSERT_OK(CheckBuffersOnDevice(*column, *device_));
    }

    ASSERT_OK(writer->WriteRecordBatch(*cuda_batch));

    FlightStreamChunk chunk;
    ASSERT_OK(reader->Next(&chunk));
    for (const auto& column : chunk.data->columns()) {
      ASSERT_OK(CheckBuffersOnDevice(*column, *device_));
    }

    // Bounce record batch back to host memory
    ASSERT_OK_AND_ASSIGN(auto host_batch, CopyBatchToHost(*chunk.data));
    AssertBatchesEqual(*batch, *host_batch);
  }
  ASSERT_OK(writer->Close());
}

}  // namespace flight
}  // namespace arrow

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

#include "arrow/flight/test_util.h"

#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <sstream>

// We need Windows fixes before including Boost
#include "arrow/util/windows_compatibility.h"

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/ipc/test_common.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/process.h"
#include "arrow/testing/util.h"
#include "arrow/util/logging.h"

#include "arrow/flight/api.h"
#include "arrow/flight/serialization_internal.h"

namespace arrow::flight {

Status TestServer::Start(const std::vector<std::string>& extra_args) {
  server_process_ = std::make_unique<util::Process>();
  ARROW_RETURN_NOT_OK(server_process_->SetExecutable(executable_name_));
  std::vector<std::string> args = {};
  if (unix_sock_.empty()) {
    args.push_back("-port");
    args.push_back(std::to_string(port_));
  } else {
    args.push_back("-server_unix");
    args.push_back(unix_sock_);
  }
  args.insert(args.end(), extra_args.begin(), extra_args.end());
  server_process_->SetArgs(args);
  ARROW_RETURN_NOT_OK(server_process_->Execute());
  std::cout << "Server running with pid " << server_process_->pid() << std::endl;
  return Status::OK();
}

void TestServer::Stop() { server_process_ = nullptr; }

bool TestServer::IsRunning() { return server_process_->IsRunning(); }

int TestServer::port() const { return port_; }

const std::string& TestServer::unix_sock() const { return unix_sock_; }

FlightInfo MakeFlightInfo(const Schema& schema, const FlightDescriptor& descriptor,
                          const std::vector<FlightEndpoint>& endpoints,
                          int64_t total_records, int64_t total_bytes, bool ordered,
                          std::string app_metadata) {
  EXPECT_OK_AND_ASSIGN(auto info,
                       FlightInfo::Make(schema, descriptor, endpoints, total_records,
                                        total_bytes, ordered, std::move(app_metadata)));
  return info;
}

NumberingStream::NumberingStream(std::unique_ptr<FlightDataStream> stream)
    : counter_(0), stream_(std::move(stream)) {}

std::shared_ptr<Schema> NumberingStream::schema() { return stream_->schema(); }

arrow::Result<FlightPayload> NumberingStream::GetSchemaPayload() {
  return stream_->GetSchemaPayload();
}

arrow::Result<FlightPayload> NumberingStream::Next() {
  ARROW_ASSIGN_OR_RAISE(FlightPayload payload, stream_->Next());
  if (payload.ipc_message.type == ipc::MessageType::RECORD_BATCH) {
    payload.app_metadata = Buffer::FromString(std::to_string(counter_));
    counter_++;
  }
  return payload;
}

std::shared_ptr<Schema> ExampleIntSchema() {
  auto f0 = field("f0", int8());
  auto f1 = field("f1", uint8());
  auto f2 = field("f2", int16());
  auto f3 = field("f3", uint16());
  auto f4 = field("f4", int32());
  auto f5 = field("f5", uint32());
  auto f6 = field("f6", int64());
  auto f7 = field("f7", uint64());
  return ::arrow::schema({f0, f1, f2, f3, f4, f5, f6, f7});
}

std::shared_ptr<Schema> ExampleFloatSchema() {
  auto f0 = field("f0", float16());
  auto f1 = field("f1", float32());
  auto f2 = field("f2", float64());
  return ::arrow::schema({f0, f1, f2});
}

std::shared_ptr<Schema> ExampleStringSchema() {
  auto f0 = field("f0", utf8());
  auto f1 = field("f1", binary());
  return ::arrow::schema({f0, f1});
}

std::shared_ptr<Schema> ExampleDictSchema() {
  std::shared_ptr<RecordBatch> batch;
  ABORT_NOT_OK(ipc::test::MakeDictionary(&batch));
  return batch->schema();
}

std::shared_ptr<Schema> ExampleLargeSchema() {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  for (int i = 0; i < 128; i++) {
    const auto field_name = "f" + std::to_string(i);
    fields.push_back(arrow::field(field_name, arrow::float64()));
  }
  return arrow::schema(fields);
}

std::vector<FlightInfo> ExampleFlightInfo() {
  Location location1 = *Location::ForGrpcTcp("foo1.bar.com", 12345);
  Location location2 = *Location::ForGrpcTcp("foo2.bar.com", 12345);
  Location location3 = *Location::ForGrpcTcp("foo3.bar.com", 12345);
  Location location4 = *Location::ForGrpcTcp("foo4.bar.com", 12345);
  Location location5 = *Location::ForGrpcTcp("foo5.bar.com", 12345);

  FlightEndpoint endpoint1({Ticket{"ticket-ints-1"}, {location1}, std::nullopt, {}});
  FlightEndpoint endpoint2({Ticket{"ticket-ints-2"}, {location2}, std::nullopt, {}});
  FlightEndpoint endpoint3({Ticket{"ticket-cmd"}, {location3}, std::nullopt, {}});
  FlightEndpoint endpoint4({Ticket{"ticket-dicts-1"}, {location4}, std::nullopt, {}});
  FlightEndpoint endpoint5({Ticket{"ticket-floats-1"}, {location5}, std::nullopt, {}});

  FlightDescriptor descr1{FlightDescriptor::PATH, "", {"examples", "ints"}};
  FlightDescriptor descr2{FlightDescriptor::CMD, "my_command", {}};
  FlightDescriptor descr3{FlightDescriptor::PATH, "", {"examples", "dicts"}};
  FlightDescriptor descr4{FlightDescriptor::PATH, "", {"examples", "floats"}};

  auto schema1 = ExampleIntSchema();
  auto schema2 = ExampleStringSchema();
  auto schema3 = ExampleDictSchema();
  auto schema4 = ExampleFloatSchema();

  return {
      MakeFlightInfo(*schema1, descr1, {endpoint1, endpoint2}, 1000, 100000, false, ""),
      MakeFlightInfo(*schema2, descr2, {endpoint3}, 1000, 100000, false, ""),
      MakeFlightInfo(*schema3, descr3, {endpoint4}, -1, -1, false, ""),
      MakeFlightInfo(*schema4, descr4, {endpoint5}, 1000, 100000, false, ""),
  };
}

Status ExampleIntBatches(RecordBatchVector* out) {
  std::shared_ptr<RecordBatch> batch;
  for (int i = 0; i < 5; ++i) {
    // Make all different sizes, use different random seed
    RETURN_NOT_OK(ipc::test::MakeIntBatchSized(10 + i, &batch, i));
    out->push_back(batch);
  }
  return Status::OK();
}

Status ExampleFloatBatches(RecordBatchVector* out) {
  std::shared_ptr<RecordBatch> batch;
  for (int i = 0; i < 5; ++i) {
    // Make all different sizes, use different random seed
    RETURN_NOT_OK(ipc::test::MakeFloatBatchSized(10 + i, &batch, i));
    out->push_back(batch);
  }
  return Status::OK();
}

Status ExampleDictBatches(RecordBatchVector* out) {
  // Just the same batch, repeated a few times
  std::shared_ptr<RecordBatch> batch;
  for (int i = 0; i < 3; ++i) {
    RETURN_NOT_OK(ipc::test::MakeDictionary(&batch));
    out->push_back(batch);
  }
  return Status::OK();
}

Status ExampleNestedBatches(RecordBatchVector* out) {
  std::shared_ptr<RecordBatch> batch;
  for (int i = 0; i < 3; ++i) {
    RETURN_NOT_OK(ipc::test::MakeListRecordBatch(&batch));
    out->push_back(batch);
  }
  return Status::OK();
}

Status ExampleLargeBatches(RecordBatchVector* out) {
  const auto array_length = 32768;
  std::shared_ptr<RecordBatch> batch;
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  const auto arr = arrow::ConstantArrayGenerator::Float64(array_length, 1.0);
  for (int i = 0; i < 128; i++) {
    arrays.push_back(arr);
  }
  auto schema = ExampleLargeSchema();
  out->push_back(RecordBatch::Make(schema, array_length, arrays));
  out->push_back(RecordBatch::Make(schema, array_length, arrays));
  return Status::OK();
}

arrow::Result<std::shared_ptr<RecordBatch>> VeryLargeBatch() {
  // In CI, some platforms don't let us allocate one very large
  // buffer, so allocate a smaller buffer and repeat it a few times
  constexpr int64_t nbytes = (1ul << 27ul) + 8ul;
  constexpr int64_t nrows = nbytes / 8;
  constexpr int64_t ncols = 16;
  ARROW_ASSIGN_OR_RAISE(auto values, AllocateBuffer(nbytes));
  std::memset(values->mutable_data(), 0x00, values->capacity());
  std::vector<std::shared_ptr<Buffer>> buffers = {nullptr, std::move(values)};
  auto array = std::make_shared<ArrayData>(int64(), nrows, buffers,
                                           /*null_count=*/0);
  std::vector<std::shared_ptr<ArrayData>> arrays(ncols, array);
  std::vector<std::shared_ptr<Field>> fields(ncols, field("a", int64()));
  return RecordBatch::Make(schema(std::move(fields)), nrows, std::move(arrays));
}

std::vector<ActionType> ExampleActionTypes() {
  return {{"drop", "drop a dataset"}, {"cache", "cache a dataset"}};
}

Status ExampleTlsCertificates(std::vector<CertKeyPair>* out) {
  std::string root;
  RETURN_NOT_OK(GetTestResourceRoot(&root));

  *out = std::vector<CertKeyPair>();
  for (int i = 0; i < 2; i++) {
    try {
      std::stringstream cert_path;
      cert_path << root << "/flight/cert" << i << ".pem";
      std::stringstream key_path;
      key_path << root << "/flight/cert" << i << ".key";

      std::ifstream cert_file(cert_path.str());
      if (!cert_file) {
        return Status::IOError("Could not open certificate: " + cert_path.str());
      }
      std::stringstream cert;
      cert << cert_file.rdbuf();

      std::ifstream key_file(key_path.str());
      if (!key_file) {
        return Status::IOError("Could not open key: " + key_path.str());
      }
      std::stringstream key;
      key << key_file.rdbuf();

      out->push_back(CertKeyPair{cert.str(), key.str()});
    } catch (const std::ifstream::failure& e) {
      return Status::IOError(e.what());
    }
  }
  return Status::OK();
}

Status ExampleTlsCertificateRoot(CertKeyPair* out) {
  std::string root;
  RETURN_NOT_OK(GetTestResourceRoot(&root));

  std::stringstream path;
  path << root << "/flight/root-ca.pem";

  try {
    std::ifstream cert_file(path.str());
    if (!cert_file) {
      return Status::IOError("Could not open certificate: " + path.str());
    }
    std::stringstream cert;
    cert << cert_file.rdbuf();
    out->pem_cert = cert.str();
    out->pem_key = "";
    return Status::OK();
  } catch (const std::ifstream::failure& e) {
    return Status::IOError(e.what());
  }
}

}  // namespace arrow::flight

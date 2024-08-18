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

#ifdef __APPLE__
#include <limits.h>
#include <mach-o/dyld.h>
#endif

#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <sstream>

// We need Windows fixes before including Boost
#include "arrow/util/windows_compatibility.h"

#include <gtest/gtest.h>
#include <boost/filesystem.hpp>
#define BOOST_NO_CXX98_FUNCTION_BASE  // ARROW-17805
// We need BOOST_USE_WINDOWS_H definition with MinGW when we use
// boost/process.hpp. See BOOST_USE_WINDOWS_H=1 in
// cpp/cmake_modules/ThirdpartyToolchain.cmake for details.
#include <boost/process.hpp>

#include "arrow/array.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/ipc/test_common.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/util/logging.h"

#include "arrow/flight/api.h"
#include "arrow/flight/serialization_internal.h"

namespace arrow::flight {

namespace bp = boost::process;
namespace fs = boost::filesystem;

namespace {

Status ResolveCurrentExecutable(fs::path* out) {
  // See https://stackoverflow.com/a/1024937/10194 for various
  // platform-specific recipes.

  boost::system::error_code ec;

#if defined(__linux__)
  *out = fs::canonical("/proc/self/exe", ec);
#elif defined(__APPLE__)
  char buf[PATH_MAX + 1];
  uint32_t bufsize = sizeof(buf);
  if (_NSGetExecutablePath(buf, &bufsize) < 0) {
    return Status::Invalid("Can't resolve current exe: path too large");
  }
  *out = fs::canonical(buf, ec);
#elif defined(_WIN32)
  char buf[MAX_PATH + 1];
  if (!GetModuleFileNameA(NULL, buf, sizeof(buf))) {
    return Status::Invalid("Can't get executable file path");
  }
  *out = fs::canonical(buf, ec);
#else
  ARROW_UNUSED(ec);
  return Status::NotImplemented("Not available on this system");
#endif
  if (ec) {
    // XXX fold this into the Status class?
    return Status::IOError("Can't resolve current exe: ", ec.message());
  } else {
    return Status::OK();
  }
}

}  // namespace

void TestServer::Start(const std::vector<std::string>& extra_args) {
  namespace fs = boost::filesystem;

  std::string str_port = std::to_string(port_);
  std::vector<fs::path> search_path = ::boost::this_process::path();
  // If possible, prepend current executable directory to search path,
  // since it's likely that the test server executable is located in
  // the same directory as the running unit test.
  fs::path current_exe;
  Status st = ResolveCurrentExecutable(&current_exe);
  if (st.ok()) {
    search_path.insert(search_path.begin(), current_exe.parent_path());
  } else if (st.IsNotImplemented()) {
    ARROW_CHECK(st.IsNotImplemented()) << st.ToString();
  }

  try {
    if (unix_sock_.empty()) {
      server_process_ =
          std::make_shared<bp::child>(bp::search_path(executable_name_, search_path),
                                      "-port", str_port, bp::args(extra_args));
    } else {
      server_process_ =
          std::make_shared<bp::child>(bp::search_path(executable_name_, search_path),
                                      "-server_unix", unix_sock_, bp::args(extra_args));
    }
  } catch (...) {
    std::stringstream ss;
    ss << "Failed to launch test server '" << executable_name_ << "', looked in ";
    for (const auto& path : search_path) {
      ss << path << " : ";
    }
    ARROW_LOG(FATAL) << ss.str();
    throw;
  }
  std::cout << "Server running with pid " << server_process_->id() << std::endl;
}

int TestServer::Stop() {
  if (server_process_ && server_process_->valid()) {
#ifndef _WIN32
    kill(server_process_->id(), SIGTERM);
#else
    // This would use SIGKILL on POSIX, which is more brutal than SIGTERM
    server_process_->terminate();
#endif
    server_process_->wait();
    return server_process_->exit_code();
  } else {
    // Presumably the server wasn't able to start
    return -1;
  }
}

bool TestServer::IsRunning() { return server_process_->running(); }

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

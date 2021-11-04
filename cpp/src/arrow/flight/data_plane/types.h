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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "arrow/result.h"

namespace grpc {

class ClientContext;
class ServerContext;

};  // namespace grpc

namespace arrow {
namespace flight {

struct FlightPayload;
struct Location;

namespace internal {

struct FlightData;

// Data plane stream simulates grpc::Client|ServerReader[Writer]
// - DataClientStream is created before DataServerStream
// - all interfaces should return IOError on data plane related errors
// - WritesDone closes the writer side, the operation should be idempotent and
//   after that, the writer should return IOError on "Write", and the reader
//   should return IOError on "Read" (pending reads on peer is not discarded)
// - Finish closes the client, no more read/write can be performed, it should
//   be idempotent and after that, both the client and server should return
//   IOError on "Read" or "Write" (pending reads on server is not discarded)
// - TryCancel tells server to stop writing, should be idempotent
// - data race
//   * a single data plane may create several client/server stream pairs which
//     run in parallel
//   * for a single stream, Read may run in parallel with Write (DoExchange),
//     Read/Read and Write/Write normally run in sequence (DoGet/DoPut), but
//     should behave corretly if multiple Reads or Writes run in parallel

// NOTE: grpc defines separated classes for Reader/Writer/ReaderWriter,
//       data plane implements all the read/write interfaces in one class

struct DataClientStream {
  virtual ~DataClientStream() = default;

  virtual Status Read(FlightData* data) = 0;
  virtual Status Write(const FlightPayload& payload) = 0;
  virtual Status WritesDone() = 0;
  virtual Status Finish() = 0;
  virtual void TryCancel() = 0;
};

struct DataServerStream {
  virtual ~DataServerStream() = default;

  virtual Status Read(FlightData* data) = 0;
  virtual Status Write(const FlightPayload& payload) = 0;
  // grpc doesn't implement server writes done
  virtual Status WritesDone() = 0;
};

// Data plane is initialized at client/server startup, it creates data streams
// to replace grpc streams for flight payload transmission (get/put/exchange).

class ClientDataPlane {
 public:
  // client can send a {str:str} map to server together with grpc metadata
  // this is useful to match client data stream with server data stream
  using StreamMap = std::map<std::string, std::string>;
  using ResultClientStream = arrow::Result<std::unique_ptr<DataClientStream>>;

  virtual ~ClientDataPlane() = default;

  static arrow::Result<std::unique_ptr<ClientDataPlane>> Make(const Location& location);

  ResultClientStream DoGet(grpc::ClientContext* context) {
    StreamMap stream_map;
    ARROW_ASSIGN_OR_RAISE(auto data_stream, DoGetImpl(&stream_map));
    AppendStreamMap(context, stream_map);
    return data_stream;
  }

  ResultClientStream DoPut(grpc::ClientContext* context) {
    StreamMap stream_map;
    ARROW_ASSIGN_OR_RAISE(auto data_stream, DoPutImpl(&stream_map));
    AppendStreamMap(context, stream_map);
    return data_stream;
  }

  ResultClientStream DoExchange(grpc::ClientContext* context) {
    StreamMap stream_map;
    ARROW_ASSIGN_OR_RAISE(auto data_stream, DoExchangeImpl(&stream_map));
    AppendStreamMap(context, stream_map);
    return data_stream;
  }

 private:
  // implement empty data plane in base class
  virtual ResultClientStream DoGetImpl(StreamMap* stream_map) { return NULLPTR; }
  virtual ResultClientStream DoPutImpl(StreamMap* stream_map) { return NULLPTR; }
  virtual ResultClientStream DoExchangeImpl(StreamMap* stream_map) { return NULLPTR; }

  void AppendStreamMap(grpc::ClientContext* context, const StreamMap& stream_map);
};

class ServerDataPlane {
 public:
  using StreamMap = std::map<std::string, std::string>;
  using ResultServerStream = arrow::Result<std::unique_ptr<DataServerStream>>;

  virtual ~ServerDataPlane() = default;

  static arrow::Result<std::unique_ptr<ServerDataPlane>> Make(const Location& location);

  ResultServerStream DoGet(const grpc::ServerContext& context) {
    return DoGetImpl(ExtractStreamMap(context));
  }

  ResultServerStream DoPut(const grpc::ServerContext& context) {
    return DoPutImpl(ExtractStreamMap(context));
  }

  ResultServerStream DoExchange(const grpc::ServerContext& context) {
    return DoExchangeImpl(ExtractStreamMap(context));
  }

 private:
  virtual ResultServerStream DoGetImpl(const StreamMap& stream_map) { return NULLPTR; }
  virtual ResultServerStream DoPutImpl(const StreamMap& stream_map) { return NULLPTR; }
  virtual ResultServerStream DoExchangeImpl(const StreamMap& stream_map) {
    return NULLPTR;
  }
  virtual std::vector<std::string> stream_keys() { return {}; }

  StreamMap ExtractStreamMap(const grpc::ServerContext& context);
};

}  // namespace internal
}  // namespace flight
}  // namespace arrow

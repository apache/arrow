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

#ifndef PYARROW_FLIGHT_H
#define PYARROW_FLIGHT_H

#include <memory>
#include <string>
#include <vector>

#include "arrow/flight/api.h"
#include "arrow/ipc/dictionary.h"
#include "arrow/python/common.h"
#include "arrow/python/config.h"

namespace arrow {

namespace py {

namespace flight {

/// \brief A table of function pointers for calling from C++ into
/// Python.
class ARROW_PYTHON_EXPORT PyFlightServerVtable {
 public:
  std::function<void(PyObject*, const arrow::flight::ServerCallContext&,
                     const arrow::flight::Criteria*,
                     std::unique_ptr<arrow::flight::FlightListing>*)>
      list_flights;
  std::function<void(PyObject*, const arrow::flight::ServerCallContext&,
                     const arrow::flight::FlightDescriptor&,
                     std::unique_ptr<arrow::flight::FlightInfo>*)>
      get_flight_info;
  std::function<void(PyObject*, const arrow::flight::ServerCallContext&,
                     const arrow::flight::Ticket&,
                     std::unique_ptr<arrow::flight::FlightDataStream>*)>
      do_get;
  std::function<void(PyObject*, const arrow::flight::ServerCallContext&,
                     std::unique_ptr<arrow::flight::FlightMessageReader>)>
      do_put;
  std::function<void(PyObject*, const arrow::flight::ServerCallContext&,
                     const arrow::flight::Action&,
                     std::unique_ptr<arrow::flight::ResultStream>*)>
      do_action;
  std::function<void(PyObject*, const arrow::flight::ServerCallContext&,
                     std::vector<arrow::flight::ActionType>*)>
      list_actions;
};

class ARROW_PYTHON_EXPORT PyServerAuthHandlerVtable {
 public:
  std::function<void(PyObject*, arrow::flight::ServerAuthSender*,
                     arrow::flight::ServerAuthReader*)>
      authenticate;
  std::function<void(PyObject*, const std::string&, std::string*)> is_valid;
};

class ARROW_PYTHON_EXPORT PyClientAuthHandlerVtable {
 public:
  std::function<void(PyObject*, arrow::flight::ClientAuthSender*,
                     arrow::flight::ClientAuthReader*)>
      authenticate;
  std::function<void(PyObject*, std::string*)> get_token;
};

/// \brief A helper to implement an auth mechanism in Python.
class ARROW_PYTHON_EXPORT PyServerAuthHandler : public arrow::flight::ServerAuthHandler {
 public:
  explicit PyServerAuthHandler(PyObject* handler, PyServerAuthHandlerVtable vtable);
  Status Authenticate(arrow::flight::ServerAuthSender* outgoing,
                      arrow::flight::ServerAuthReader* incoming) override;
  Status IsValid(const std::string& token, std::string* peer_identity) override;

 private:
  OwnedRefNoGIL handler_;
  PyServerAuthHandlerVtable vtable_;
};

/// \brief A helper to implement an auth mechanism in Python.
class ARROW_PYTHON_EXPORT PyClientAuthHandler : public arrow::flight::ClientAuthHandler {
 public:
  explicit PyClientAuthHandler(PyObject* handler, PyClientAuthHandlerVtable vtable);
  Status Authenticate(arrow::flight::ClientAuthSender* outgoing,
                      arrow::flight::ClientAuthReader* incoming) override;
  Status GetToken(std::string* token) override;

 private:
  OwnedRefNoGIL handler_;
  PyClientAuthHandlerVtable vtable_;
};

class ARROW_PYTHON_EXPORT PyFlightServer : public arrow::flight::FlightServerBase {
 public:
  explicit PyFlightServer(PyObject* server, PyFlightServerVtable vtable);

  // Like Serve(), but set up signals and invoke Python signal handlers
  // if necessary.  This function may return with a Python exception set.
  Status ServeWithSignals();

  Status ListFlights(const arrow::flight::ServerCallContext& context,
                     const arrow::flight::Criteria* criteria,
                     std::unique_ptr<arrow::flight::FlightListing>* listings) override;
  Status GetFlightInfo(const arrow::flight::ServerCallContext& context,
                       const arrow::flight::FlightDescriptor& request,
                       std::unique_ptr<arrow::flight::FlightInfo>* info) override;
  Status DoGet(const arrow::flight::ServerCallContext& context,
               const arrow::flight::Ticket& request,
               std::unique_ptr<arrow::flight::FlightDataStream>* stream) override;
  Status DoPut(const arrow::flight::ServerCallContext& context,
               std::unique_ptr<arrow::flight::FlightMessageReader> reader) override;
  Status DoAction(const arrow::flight::ServerCallContext& context,
                  const arrow::flight::Action& action,
                  std::unique_ptr<arrow::flight::ResultStream>* result) override;
  Status ListActions(const arrow::flight::ServerCallContext& context,
                     std::vector<arrow::flight::ActionType>* actions) override;

 private:
  OwnedRefNoGIL server_;
  PyFlightServerVtable vtable_;
};

/// \brief A callback that obtains the next result from a Flight action.
typedef std::function<void(PyObject*, std::unique_ptr<arrow::flight::Result>*)>
    PyFlightResultStreamCallback;

/// \brief A ResultStream built around a Python callback.
class ARROW_PYTHON_EXPORT PyFlightResultStream : public arrow::flight::ResultStream {
 public:
  /// \brief Construct a FlightResultStream from a Python object and callback.
  /// Must only be called while holding the GIL.
  explicit PyFlightResultStream(PyObject* generator,
                                PyFlightResultStreamCallback callback);
  Status Next(std::unique_ptr<arrow::flight::Result>* result) override;

 private:
  OwnedRefNoGIL generator_;
  PyFlightResultStreamCallback callback_;
};

/// \brief A wrapper around a FlightDataStream that keeps alive a
/// Python object backing it.
class ARROW_PYTHON_EXPORT PyFlightDataStream : public arrow::flight::FlightDataStream {
 public:
  /// \brief Construct a FlightDataStream from a Python object and underlying stream.
  /// Must only be called while holding the GIL.
  explicit PyFlightDataStream(PyObject* data_source,
                              std::unique_ptr<arrow::flight::FlightDataStream> stream);

  std::shared_ptr<Schema> schema() override;
  Status GetSchemaPayload(arrow::flight::FlightPayload* payload) override;
  Status Next(arrow::flight::FlightPayload* payload) override;

 private:
  OwnedRefNoGIL data_source_;
  std::unique_ptr<arrow::flight::FlightDataStream> stream_;
};

/// \brief A callback that obtains the next payload from a Flight result stream.
typedef std::function<void(PyObject*, arrow::flight::FlightPayload*)>
    PyGeneratorFlightDataStreamCallback;

/// \brief A FlightDataStream built around a Python callback.
class ARROW_PYTHON_EXPORT PyGeneratorFlightDataStream
    : public arrow::flight::FlightDataStream {
 public:
  /// \brief Construct a FlightDataStream from a Python object and underlying stream.
  /// Must only be called while holding the GIL.
  explicit PyGeneratorFlightDataStream(PyObject* generator,
                                       std::shared_ptr<arrow::Schema> schema,
                                       PyGeneratorFlightDataStreamCallback callback);
  std::shared_ptr<Schema> schema() override;
  Status GetSchemaPayload(arrow::flight::FlightPayload* payload) override;
  Status Next(arrow::flight::FlightPayload* payload) override;

 private:
  OwnedRefNoGIL generator_;
  std::shared_ptr<arrow::Schema> schema_;
  ipc::DictionaryMemo dictionary_memo_;
  PyGeneratorFlightDataStreamCallback callback_;
};

ARROW_PYTHON_EXPORT
Status CreateFlightInfo(const std::shared_ptr<arrow::Schema>& schema,
                        const arrow::flight::FlightDescriptor& descriptor,
                        const std::vector<arrow::flight::FlightEndpoint>& endpoints,
                        uint64_t total_records, uint64_t total_bytes,
                        std::unique_ptr<arrow::flight::FlightInfo>* out);

}  // namespace flight
}  // namespace py
}  // namespace arrow

#endif  // PYARROW_FLIGHT_H

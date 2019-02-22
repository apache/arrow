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

#include <utility>

#include "arrow/flight/internal.h"
#include "arrow/python/flight.h"

namespace arrow {
namespace py {
namespace flight {

PyFlightServer::PyFlightServer(PyObject* server, PyFlightServerVtable vtable)
    : vtable_(vtable) {
  Py_INCREF(server);
  server_.reset(server);
}

Status PyFlightServer::ListFlights(
    const arrow::flight::Criteria* criteria,
    std::unique_ptr<arrow::flight::FlightListing>* listings) {
  return Status::NotImplemented("NYI");
}

Status PyFlightServer::GetFlightInfo(const arrow::flight::FlightDescriptor& request,
                                     std::unique_ptr<arrow::flight::FlightInfo>* info) {
  PyAcquireGIL lock;
  vtable_.get_flight_info(server_.obj(), request, info);
  return CheckPyError();
}

Status PyFlightServer::DoGet(const arrow::flight::Ticket& request,
                             std::unique_ptr<arrow::flight::FlightDataStream>* stream) {
  PyAcquireGIL lock;
  vtable_.do_get(server_.obj(), request, stream);
  return CheckPyError();
}

Status PyFlightServer::DoPut(std::unique_ptr<arrow::flight::FlightMessageReader> reader) {
  PyAcquireGIL lock;
  vtable_.do_put(server_.obj(), std::move(reader));
  return CheckPyError();
}

Status PyFlightServer::DoAction(const arrow::flight::Action& action,
                                std::unique_ptr<arrow::flight::ResultStream>* result) {
  return Status::NotImplemented("NYI");
}

Status PyFlightServer::ListActions(std::vector<arrow::flight::ActionType>* actions) {
  return Status::NotImplemented("NYI");
}

Status CreateFlightInfo(const std::shared_ptr<arrow::Schema>& schema,
                        const arrow::flight::FlightDescriptor& descriptor,
                        const std::vector<arrow::flight::FlightEndpoint>& endpoints,
                        uint64_t total_records, uint64_t total_bytes,
                        std::unique_ptr<arrow::flight::FlightInfo>* out) {
  arrow::flight::FlightInfo::Data flight_data;
  RETURN_NOT_OK(arrow::flight::internal::SchemaToString(*schema, &flight_data.schema));
  flight_data.descriptor = descriptor;
  flight_data.endpoints = endpoints;
  flight_data.total_records = total_records;
  flight_data.total_bytes = total_bytes;
  arrow::flight::FlightInfo value(flight_data);
  *out = std::unique_ptr<arrow::flight::FlightInfo>(new arrow::flight::FlightInfo(value));
  return Status::OK();
}

}  // namespace flight
}  // namespace py
}  // namespace arrow

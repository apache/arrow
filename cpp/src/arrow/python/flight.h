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
#include <vector>

#include "arrow/flight/api.h"
#include "arrow/python/common.h"
#include "arrow/python/config.h"

namespace arrow {

namespace py {

namespace flight {

/// \brief A table of function pointers for calling from C++ into
/// Python.
class ARROW_PYTHON_EXPORT PyFlightServerVtable {
 public:
  std::function<void(PyObject*, const arrow::flight::Criteria*,
                     std::unique_ptr<arrow::flight::FlightListing>*)>
      list_flights;
  std::function<void(PyObject*, const arrow::flight::FlightDescriptor&,
                     std::unique_ptr<arrow::flight::FlightInfo>*)>
      get_flight_info;
  std::function<void(PyObject*, const arrow::flight::Ticket&,
                     std::unique_ptr<arrow::flight::FlightDataStream>*)>
      do_get;
  std::function<void(PyObject*, std::unique_ptr<arrow::flight::FlightMessageReader>)>
      do_put;
  std::function<void(PyObject*, const arrow::flight::Action&,
                     std::unique_ptr<arrow::flight::ResultStream>*)>
      do_action;
  std::function<void(PyObject*, std::vector<arrow::flight::ActionType>*)> list_actions;
};

class ARROW_PYTHON_EXPORT PyFlightServer : public arrow::flight::FlightServerBase {
 public:
  explicit PyFlightServer(PyObject* server, PyFlightServerVtable vtable);

  // Like Serve(), but set up signals and invoke Python signal handlers
  // if necessary.  This function may return with a Python exception set.
  Status ServeWithSignals();

  Status ListFlights(const arrow::flight::Criteria* criteria,
                     std::unique_ptr<arrow::flight::FlightListing>* listings) override;
  Status GetFlightInfo(const arrow::flight::FlightDescriptor& request,
                       std::unique_ptr<arrow::flight::FlightInfo>* info) override;
  Status DoGet(const arrow::flight::Ticket& request,
               std::unique_ptr<arrow::flight::FlightDataStream>* stream) override;
  Status DoPut(std::unique_ptr<arrow::flight::FlightMessageReader> reader) override;
  Status DoAction(const arrow::flight::Action& action,
                  std::unique_ptr<arrow::flight::ResultStream>* result) override;
  Status ListActions(std::vector<arrow::flight::ActionType>* actions) override;

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
  explicit PyFlightResultStream(PyObject* generator,
                                PyFlightResultStreamCallback callback);
  Status Next(std::unique_ptr<arrow::flight::Result>* result) override;

 private:
  OwnedRefNoGIL generator_;
  PyFlightResultStreamCallback callback_;
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

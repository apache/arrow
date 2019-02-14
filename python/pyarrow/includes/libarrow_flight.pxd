# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# distutils: language = c++

from libcpp.functional cimport function

from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *


cdef extern from "arrow/flight/api.h" namespace "arrow" nogil:
    cdef cppclass CActionType" arrow::flight::ActionType":
        c_string type
        c_string description

    cdef cppclass CAction" arrow::flight::Action":
        c_string type
        shared_ptr[CBuffer] body

    cdef cppclass CResult" arrow::flight::Result":
        shared_ptr[CBuffer] body

    cdef cppclass CResultStream" arrow::flight::ResultStream":
        CStatus Next(unique_ptr[CResult]* result)

    cdef cppclass CDescriptorType \
            " arrow::flight::FlightDescriptor::DescriptorType":
        bint operator==(CDescriptorType)

    CDescriptorType CDescriptorTypeUnknown\
        " arrow::flight::FlightDescriptor::UNKNOWN"
    CDescriptorType CDescriptorTypePath\
        " arrow::flight::FlightDescriptor::PATH"
    CDescriptorType CDescriptorTypeCmd\
        " arrow::flight::FlightDescriptor::CMD"

    cdef cppclass CFlightDescriptor" arrow::flight::FlightDescriptor":
        CDescriptorType type
        c_string cmd
        vector[c_string] path

    cdef cppclass CTicket" arrow::flight::Ticket":
        CTicket()
        c_string ticket

    cdef cppclass CLocation" arrow::flight::Location":
        CLocation()

        c_string host
        int32_t port

    cdef cppclass CFlightEndpoint" arrow::flight::FlightEndpoint":
        CFlightEndpoint()

        CTicket ticket
        vector[CLocation] locations

    cdef cppclass CFlightInfo" arrow::flight::FlightInfo":
        uint64_t total_records()
        uint64_t total_bytes()
        CStatus GetSchema(shared_ptr[CSchema]* out)
        CFlightDescriptor& descriptor()
        const vector[CFlightEndpoint]& endpoints()

    cdef cppclass CFlightListing" arrow::flight::FlightListing":
        CStatus Next(unique_ptr[CFlightInfo]* info)

    cdef cppclass CFlightMessageReader \
            " arrow::flight::FlightMessageReader"(CRecordBatchReader):
        CFlightDescriptor& descriptor()

    cdef cppclass CFlightDataStream" arrow::flight::FlightDataStream":
        pass

    cdef cppclass CRecordBatchStream \
            " arrow::flight::RecordBatchStream"(CFlightDataStream):
        CRecordBatchStream(shared_ptr[CRecordBatchReader]& reader)

    cdef cppclass CFlightClient" arrow::flight::FlightClient":
        @staticmethod
        CStatus Connect(const c_string& host, int port,
                        unique_ptr[CFlightClient]* client)

        CStatus DoAction(CAction& action, unique_ptr[CResultStream]* results)
        CStatus ListActions(vector[CActionType]* actions)

        CStatus ListFlights(unique_ptr[CFlightListing]* listing)
        CStatus GetFlightInfo(CFlightDescriptor& descriptor,
                              unique_ptr[CFlightInfo]* info)

        CStatus DoGet(CTicket& ticket, shared_ptr[CSchema]& schema,
                      unique_ptr[CRecordBatchReader]* stream)
        CStatus DoPut(CFlightDescriptor& descriptor,
                      shared_ptr[CSchema]& schema,
                      unique_ptr[CRecordBatchWriter]* stream)


# Callbacks for implementing Flight servers
# Use typedef to emulate syntax for std::function<void(...)>
ctypedef void cb_get_flight_info(object, const CFlightDescriptor&,
                                 unique_ptr[CFlightInfo]*)
ctypedef void cb_do_put(object, unique_ptr[CFlightMessageReader])
ctypedef void cb_do_get(object, const CTicket&,
                        unique_ptr[CFlightDataStream]*)

cdef extern from "arrow/python/flight.h" namespace "arrow::py::flight" nogil:
    cdef cppclass PyFlightServerVtable:
        PyFlightServerVtable()
        function[cb_get_flight_info] get_flight_info
        function[cb_do_put] do_put
        function[cb_do_get] do_get

    cdef cppclass PyFlightServer:
        PyFlightServer(object server, PyFlightServerVtable vtable)
        void Run(int port)
        void Shutdown()

    cdef CStatus CreateFlightInfo" arrow::py::flight::CreateFlightInfo"(
        shared_ptr[CSchema] schema,
        CFlightDescriptor& descriptor,
        vector[CFlightEndpoint] endpoints,
        uint64_t total_records,
        uint64_t total_bytes,
        unique_ptr[CFlightInfo]* out)

cdef extern from "<utility>" namespace "std":
    unique_ptr[CFlightDataStream] move(unique_ptr[CFlightDataStream])

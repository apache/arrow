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


cdef extern from "arrow/ipc/api.h" namespace "arrow" nogil:
    cdef cppclass CIpcPayload" arrow::ipc::internal::IpcPayload":
        MessageType type
        shared_ptr[CBuffer] metadata
        vector[shared_ptr[CBuffer]] body_buffers
        int64_t body_length

    cdef CStatus _GetRecordBatchPayload\
        " arrow::ipc::internal::GetRecordBatchPayload"(
            const CRecordBatch& batch,
            CMemoryPool* pool,
            CIpcPayload* out)


cdef extern from "arrow/flight/api.h" namespace "arrow" nogil:
    cdef cppclass CActionType" arrow::flight::ActionType":
        c_string type
        c_string description

    cdef cppclass CAction" arrow::flight::Action":
        c_string type
        shared_ptr[CBuffer] body

    cdef cppclass CResult" arrow::flight::Result":
        CResult()
        CResult(CResult)
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

    cdef cppclass CCriteria" arrow::flight::Criteria":
        CCriteria()
        c_string expression

    cdef cppclass CLocation" arrow::flight::Location":
        CLocation()

        c_string host
        int32_t port

    cdef cppclass CFlightEndpoint" arrow::flight::FlightEndpoint":
        CFlightEndpoint()

        CTicket ticket
        vector[CLocation] locations

    cdef cppclass CFlightInfo" arrow::flight::FlightInfo":
        CFlightInfo(CFlightInfo info)
        uint64_t total_records()
        uint64_t total_bytes()
        CStatus GetSchema(shared_ptr[CSchema]* out)
        CFlightDescriptor& descriptor()
        const vector[CFlightEndpoint]& endpoints()

    cdef cppclass CFlightListing" arrow::flight::FlightListing":
        CStatus Next(unique_ptr[CFlightInfo]* info)

    cdef cppclass CSimpleFlightListing" arrow::flight::SimpleFlightListing":
        CSimpleFlightListing(vector[CFlightInfo]&& info)

    cdef cppclass CFlightMessageReader \
            " arrow::flight::FlightMessageReader"(CRecordBatchReader):
        CFlightDescriptor& descriptor()

    cdef cppclass CFlightPayload" arrow::flight::FlightPayload":
        shared_ptr[CBuffer] descriptor
        CIpcPayload ipc_message

    cdef cppclass CFlightDataStream" arrow::flight::FlightDataStream":
        shared_ptr[CSchema] schema()
        CStatus Next(CFlightPayload*)

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
ctypedef void cb_list_flights(object, const CCriteria*,
                              unique_ptr[CFlightListing]*)
ctypedef void cb_get_flight_info(object, const CFlightDescriptor&,
                                 unique_ptr[CFlightInfo]*)
ctypedef void cb_do_put(object, unique_ptr[CFlightMessageReader])
ctypedef void cb_do_get(object, const CTicket&,
                        unique_ptr[CFlightDataStream]*)
ctypedef void cb_do_action(object, const CAction&,
                           unique_ptr[CResultStream]*)
ctypedef void cb_list_actions(object, vector[CActionType]*)
ctypedef void cb_result_next(object, unique_ptr[CResult]*)
ctypedef void cb_data_stream_next(object, CFlightPayload*)

cdef extern from "arrow/python/flight.h" namespace "arrow::py::flight" nogil:
    cdef cppclass PyFlightServerVtable:
        PyFlightServerVtable()
        function[cb_list_flights] list_flights
        function[cb_get_flight_info] get_flight_info
        function[cb_do_put] do_put
        function[cb_do_get] do_get
        function[cb_do_action] do_action
        function[cb_list_actions] list_actions

    cdef cppclass PyFlightServer:
        PyFlightServer(object server, PyFlightServerVtable vtable)

        CStatus Init(int port)
        CStatus ServeWithSignals() except *
        void Shutdown()

    cdef cppclass CPyFlightResultStream\
            " arrow::py::flight::PyFlightResultStream"(CResultStream):
        CPyFlightResultStream(object generator,
                              function[cb_result_next] callback)

    cdef cppclass CPyFlightDataStream\
            " arrow::py::flight::PyFlightDataStream"(CFlightDataStream):
        CPyFlightDataStream(object data_source,
                            unique_ptr[CFlightDataStream] stream)

    cdef cppclass CPyGeneratorFlightDataStream\
            " arrow::py::flight::PyGeneratorFlightDataStream"\
            (CFlightDataStream):
        CPyGeneratorFlightDataStream(object generator,
                                     shared_ptr[CSchema] schema,
                                     function[cb_data_stream_next] callback)

    cdef CStatus CreateFlightInfo" arrow::py::flight::CreateFlightInfo"(
        shared_ptr[CSchema] schema,
        CFlightDescriptor& descriptor,
        vector[CFlightEndpoint] endpoints,
        uint64_t total_records,
        uint64_t total_bytes,
        unique_ptr[CFlightInfo]* out)

cdef extern from "<utility>" namespace "std":
    unique_ptr[CFlightDataStream] move(unique_ptr[CFlightDataStream])

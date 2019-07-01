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

    cdef cppclass CFlightResult" arrow::flight::Result":
        CFlightResult()
        CFlightResult(CFlightResult)
        shared_ptr[CBuffer] body

    cdef cppclass CResultStream" arrow::flight::ResultStream":
        CStatus Next(unique_ptr[CFlightResult]* result)

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
        c_string ToString()
        c_bool Equals(const CLocation& other)

        @staticmethod
        CStatus Parse(c_string& uri_string, CLocation* location)
        @staticmethod
        CStatus ForGrpcTcp(c_string& host, int port, CLocation* location)
        @staticmethod
        CStatus ForGrpcTls(c_string& host, int port, CLocation* location)
        @staticmethod
        CStatus ForGrpcUnix(c_string& path, CLocation* location)

    cdef cppclass CFlightEndpoint" arrow::flight::FlightEndpoint":
        CFlightEndpoint()

        CTicket ticket
        vector[CLocation] locations

    cdef cppclass CFlightInfo" arrow::flight::FlightInfo":
        CFlightInfo(CFlightInfo info)
        int64_t total_records()
        int64_t total_bytes()
        CStatus GetSchema(CDictionaryMemo* memo, shared_ptr[CSchema]* out)
        CFlightDescriptor& descriptor()
        const vector[CFlightEndpoint]& endpoints()

    cdef cppclass CFlightListing" arrow::flight::FlightListing":
        CStatus Next(unique_ptr[CFlightInfo]* info)

    cdef cppclass CSimpleFlightListing" arrow::flight::SimpleFlightListing":
        CSimpleFlightListing(vector[CFlightInfo]&& info)

    cdef cppclass CFlightPayload" arrow::flight::FlightPayload":
        shared_ptr[CBuffer] descriptor
        shared_ptr[CBuffer] app_metadata
        CIpcPayload ipc_message

    cdef cppclass CFlightDataStream" arrow::flight::FlightDataStream":
        shared_ptr[CSchema] schema()
        CStatus Next(CFlightPayload*)

    cdef cppclass CFlightStreamChunk" arrow::flight::FlightStreamChunk":
        CFlightStreamChunk()
        shared_ptr[CRecordBatch] data
        shared_ptr[CBuffer] app_metadata

    cdef cppclass CMetadataRecordBatchReader \
            " arrow::flight::MetadataRecordBatchReader":
        shared_ptr[CSchema] schema()
        CStatus Next(CFlightStreamChunk* out)
        CStatus ReadAll(shared_ptr[CTable]* table)

    cdef cppclass CFlightStreamReader \
            " arrow::flight::FlightStreamReader"(CMetadataRecordBatchReader):
        void Cancel()

    cdef cppclass CFlightMessageReader \
            " arrow::flight::FlightMessageReader"(CMetadataRecordBatchReader):
        CFlightDescriptor& descriptor()

    cdef cppclass CFlightStreamWriter \
            " arrow::flight::FlightStreamWriter"(CRecordBatchWriter):
        CStatus WriteWithMetadata(const CRecordBatch& batch,
                                  shared_ptr[CBuffer] app_metadata,
                                  c_bool allow_64bit)

    cdef cppclass CRecordBatchStream \
            " arrow::flight::RecordBatchStream"(CFlightDataStream):
        CRecordBatchStream(shared_ptr[CRecordBatchReader]& reader)

    cdef cppclass CFlightMetadataReader" arrow::flight::FlightMetadataReader":
        CStatus ReadMetadata(shared_ptr[CBuffer]* out)

    cdef cppclass CFlightMetadataWriter" arrow::flight::FlightMetadataWriter":
        CStatus WriteMetadata(const CBuffer& message)

    cdef cppclass CServerAuthReader" arrow::flight::ServerAuthReader":
        CStatus Read(c_string* token)

    cdef cppclass CServerAuthSender" arrow::flight::ServerAuthSender":
        CStatus Write(c_string& token)

    cdef cppclass CClientAuthReader" arrow::flight::ClientAuthReader":
        CStatus Read(c_string* token)

    cdef cppclass CClientAuthSender" arrow::flight::ClientAuthSender":
        CStatus Write(c_string& token)

    cdef cppclass CServerAuthHandler" arrow::flight::ServerAuthHandler":
        pass

    cdef cppclass CClientAuthHandler" arrow::flight::ClientAuthHandler":
        pass

    cdef cppclass CServerCallContext" arrow::flight::ServerCallContext":
        c_string& peer_identity()

    cdef cppclass CTimeoutDuration" arrow::flight::TimeoutDuration":
        CTimeoutDuration(double)

    cdef cppclass CFlightCallOptions" arrow::flight::FlightCallOptions":
        CFlightCallOptions()
        CTimeoutDuration timeout

    cdef cppclass CCertKeyPair" arrow::flight::CertKeyPair":
        CCertKeyPair()
        c_string pem_cert
        c_string pem_key

    cdef cppclass CFlightServerOptions" arrow::flight::FlightServerOptions":
        CFlightServerOptions(const CLocation& location)
        CLocation location
        unique_ptr[CServerAuthHandler] auth_handler
        vector[CCertKeyPair] tls_certificates

    cdef cppclass CFlightClientOptions" arrow::flight::FlightClientOptions":
        CFlightClientOptions()
        c_string tls_root_certs
        c_string override_hostname

    cdef cppclass CFlightClient" arrow::flight::FlightClient":
        @staticmethod
        CStatus Connect(const CLocation& location,
                        const CFlightClientOptions& options,
                        unique_ptr[CFlightClient]* client)

        CStatus Authenticate(CFlightCallOptions& options,
                             unique_ptr[CClientAuthHandler] auth_handler)

        CStatus DoAction(CFlightCallOptions& options, CAction& action,
                         unique_ptr[CResultStream]* results)
        CStatus ListActions(CFlightCallOptions& options,
                            vector[CActionType]* actions)

        CStatus ListFlights(CFlightCallOptions& options, CCriteria criteria,
                            unique_ptr[CFlightListing]* listing)
        CStatus GetFlightInfo(CFlightCallOptions& options,
                              CFlightDescriptor& descriptor,
                              unique_ptr[CFlightInfo]* info)

        CStatus DoGet(CFlightCallOptions& options, CTicket& ticket,
                      unique_ptr[CFlightStreamReader]* stream)
        CStatus DoPut(CFlightCallOptions& options,
                      CFlightDescriptor& descriptor,
                      shared_ptr[CSchema]& schema,
                      unique_ptr[CFlightStreamWriter]* stream,
                      unique_ptr[CFlightMetadataReader]* reader)


# Callbacks for implementing Flight servers
# Use typedef to emulate syntax for std::function<void(...)>
ctypedef void cb_list_flights(object, const CServerCallContext&,
                              const CCriteria*,
                              unique_ptr[CFlightListing]*)
ctypedef void cb_get_flight_info(object, const CServerCallContext&,
                                 const CFlightDescriptor&,
                                 unique_ptr[CFlightInfo]*)
ctypedef void cb_do_put(object, const CServerCallContext&,
                        unique_ptr[CFlightMessageReader],
                        unique_ptr[CFlightMetadataWriter])
ctypedef void cb_do_get(object, const CServerCallContext&,
                        const CTicket&,
                        unique_ptr[CFlightDataStream]*)
ctypedef void cb_do_action(object, const CServerCallContext&, const CAction&,
                           unique_ptr[CResultStream]*)
ctypedef void cb_list_actions(object, const CServerCallContext&,
                              vector[CActionType]*)
ctypedef void cb_result_next(object, unique_ptr[CFlightResult]*)
ctypedef void cb_data_stream_next(object, CFlightPayload*)
ctypedef void cb_server_authenticate(object, CServerAuthSender*,
                                     CServerAuthReader*)
ctypedef void cb_is_valid(object, const c_string&, c_string*)
ctypedef void cb_client_authenticate(object, CClientAuthSender*,
                                     CClientAuthReader*)
ctypedef void cb_get_token(object, c_string*)

cdef extern from "arrow/python/flight.h" namespace "arrow::py::flight" nogil:
    cdef cppclass PyFlightServerVtable:
        PyFlightServerVtable()
        function[cb_list_flights] list_flights
        function[cb_get_flight_info] get_flight_info
        function[cb_do_put] do_put
        function[cb_do_get] do_get
        function[cb_do_action] do_action
        function[cb_list_actions] list_actions

    cdef cppclass PyServerAuthHandlerVtable:
        PyServerAuthHandlerVtable()
        function[cb_server_authenticate] authenticate
        function[cb_is_valid] is_valid

    cdef cppclass PyClientAuthHandlerVtable:
        PyClientAuthHandlerVtable()
        function[cb_client_authenticate] authenticate
        function[cb_get_token] get_token

    cdef cppclass PyFlightServer:
        PyFlightServer(object server, PyFlightServerVtable vtable)

        CStatus Init(CFlightServerOptions& options)
        CStatus ServeWithSignals() except *
        void Shutdown()

    cdef cppclass PyServerAuthHandler\
            " arrow::py::flight::PyServerAuthHandler"(CServerAuthHandler):
        PyServerAuthHandler(object handler, PyServerAuthHandlerVtable vtable)

    cdef cppclass PyClientAuthHandler\
            " arrow::py::flight::PyClientAuthHandler"(CClientAuthHandler):
        PyClientAuthHandler(object handler, PyClientAuthHandlerVtable vtable)

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
        int64_t total_records,
        int64_t total_bytes,
        unique_ptr[CFlightInfo]* out)


cdef extern from "<utility>" namespace "std":
    unique_ptr[CFlightDataStream] move(unique_ptr[CFlightDataStream]) nogil
    unique_ptr[CServerAuthHandler] move(unique_ptr[CServerAuthHandler]) nogil
    unique_ptr[CClientAuthHandler] move(unique_ptr[CClientAuthHandler]) nogil

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

from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *


cdef extern from "arrow/flight/api.h" namespace "arrow" nogil:
    cdef char* CPyServerMiddlewareName\
        " arrow::py::flight::kPyServerMiddlewareName"

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

    cdef cppclass CBasicAuth" arrow::flight::BasicAuth":
        CBasicAuth()
        CBasicAuth(CBuffer)
        CBasicAuth(CBasicAuth)
        c_string username
        c_string password

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
        CStatus SerializeToString(c_string* out)

        @staticmethod
        CStatus Deserialize(const c_string& serialized,
                            CFlightDescriptor* out)
        bint operator==(CFlightDescriptor)

    cdef cppclass CTicket" arrow::flight::Ticket":
        CTicket()
        c_string ticket
        bint operator==(CTicket)
        CStatus SerializeToString(c_string* out)

        @staticmethod
        CStatus Deserialize(const c_string& serialized, CTicket* out)

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

        bint operator==(CFlightEndpoint)

    cdef cppclass CFlightInfo" arrow::flight::FlightInfo":
        CFlightInfo(CFlightInfo info)
        int64_t total_records()
        int64_t total_bytes()
        CStatus GetSchema(CDictionaryMemo* memo, shared_ptr[CSchema]* out)
        CFlightDescriptor& descriptor()
        const vector[CFlightEndpoint]& endpoints()
        CStatus SerializeToString(c_string* out)

        @staticmethod
        CStatus Deserialize(const c_string& serialized,
                            unique_ptr[CFlightInfo]* out)

    cdef cppclass CSchemaResult" arrow::flight::SchemaResult":
        CSchemaResult(CSchemaResult result)
        CStatus GetSchema(CDictionaryMemo* memo, shared_ptr[CSchema]* out)

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
        CResult[shared_ptr[CSchema]] GetSchema()
        CStatus Next(CFlightStreamChunk* out)
        CStatus ReadAll(shared_ptr[CTable]* table)

    cdef cppclass CMetadataRecordBatchWriter \
            " arrow::flight::MetadataRecordBatchWriter"(CRecordBatchWriter):
        CStatus Begin(shared_ptr[CSchema] schema,
                      const CIpcWriteOptions& options)
        CStatus WriteMetadata(shared_ptr[CBuffer] app_metadata)
        CStatus WriteWithMetadata(const CRecordBatch& batch,
                                  shared_ptr[CBuffer] app_metadata)

    cdef cppclass CFlightStreamReader \
            " arrow::flight::FlightStreamReader"(CMetadataRecordBatchReader):
        void Cancel()

    cdef cppclass CFlightMessageReader \
            " arrow::flight::FlightMessageReader"(CMetadataRecordBatchReader):
        CFlightDescriptor& descriptor()

    cdef cppclass CFlightMessageWriter \
            " arrow::flight::FlightMessageWriter"(CMetadataRecordBatchWriter):
        pass

    cdef cppclass CFlightStreamWriter \
            " arrow::flight::FlightStreamWriter"(CMetadataRecordBatchWriter):
        CStatus DoneWriting()

    cdef cppclass CRecordBatchStream \
            " arrow::flight::RecordBatchStream"(CFlightDataStream):
        CRecordBatchStream(shared_ptr[CRecordBatchReader]& reader,
                           const CIpcWriteOptions& options)

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
        c_string& peer()
        CServerMiddleware* GetMiddleware(const c_string& key)

    cdef cppclass CTimeoutDuration" arrow::flight::TimeoutDuration":
        CTimeoutDuration(double)

    cdef cppclass CFlightCallOptions" arrow::flight::FlightCallOptions":
        CFlightCallOptions()
        CTimeoutDuration timeout
        CIpcWriteOptions write_options

    cdef cppclass CCertKeyPair" arrow::flight::CertKeyPair":
        CCertKeyPair()
        c_string pem_cert
        c_string pem_key

    cdef cppclass CFlightMethod" arrow::flight::FlightMethod":
        bint operator==(CFlightMethod)

    CFlightMethod CFlightMethodInvalid\
        " arrow::flight::FlightMethod::Invalid"
    CFlightMethod CFlightMethodHandshake\
        " arrow::flight::FlightMethod::Handshake"
    CFlightMethod CFlightMethodListFlights\
        " arrow::flight::FlightMethod::ListFlights"
    CFlightMethod CFlightMethodGetFlightInfo\
        " arrow::flight::FlightMethod::GetFlightInfo"
    CFlightMethod CFlightMethodGetSchema\
        " arrow::flight::FlightMethod::GetSchema"
    CFlightMethod CFlightMethodDoGet\
        " arrow::flight::FlightMethod::DoGet"
    CFlightMethod CFlightMethodDoPut\
        " arrow::flight::FlightMethod::DoPut"
    CFlightMethod CFlightMethodDoAction\
        " arrow::flight::FlightMethod::DoAction"
    CFlightMethod CFlightMethodListActions\
        " arrow::flight::FlightMethod::ListActions"
    CFlightMethod CFlightMethodDoExchange\
        " arrow::flight::FlightMethod::DoExchange"

    cdef cppclass CCallInfo" arrow::flight::CallInfo":
        CFlightMethod method

    # This is really std::unordered_multimap, but Cython has no
    # bindings for it, so treat it as an opaque class and bind the
    # methods we need
    cdef cppclass CCallHeaders" arrow::flight::CallHeaders":
        cppclass const_iterator:
            pair[c_string, c_string] operator*()
            const_iterator operator++()
            bint operator==(const_iterator)
            bint operator!=(const_iterator)
        const_iterator cbegin()
        const_iterator cend()

    cdef cppclass CAddCallHeaders" arrow::flight::AddCallHeaders":
        void AddHeader(const c_string& key, const c_string& value)

    cdef cppclass CServerMiddleware" arrow::flight::ServerMiddleware":
        c_string name()

    cdef cppclass CServerMiddlewareFactory\
            " arrow::flight::ServerMiddlewareFactory":
        pass

    cdef cppclass CClientMiddleware" arrow::flight::ClientMiddleware":
        pass

    cdef cppclass CClientMiddlewareFactory\
            " arrow::flight::ClientMiddlewareFactory":
        pass

    cdef cppclass CFlightServerOptions" arrow::flight::FlightServerOptions":
        CFlightServerOptions(const CLocation& location)
        CLocation location
        unique_ptr[CServerAuthHandler] auth_handler
        vector[CCertKeyPair] tls_certificates
        c_bool verify_client
        c_string root_certificates
        vector[pair[c_string, shared_ptr[CServerMiddlewareFactory]]] middleware

    cdef cppclass CFlightClientOptions" arrow::flight::FlightClientOptions":
        CFlightClientOptions()
        c_string tls_root_certs
        c_string cert_chain
        c_string private_key
        c_string override_hostname
        vector[shared_ptr[CClientMiddlewareFactory]] middleware
        int64_t write_size_limit_bytes
        vector[pair[c_string, CIntStringVariant]] generic_options
        c_bool disable_server_verification

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
        CStatus GetSchema(CFlightCallOptions& options,
                          CFlightDescriptor& descriptor,
                          unique_ptr[CSchemaResult]* result)
        CStatus DoGet(CFlightCallOptions& options, CTicket& ticket,
                      unique_ptr[CFlightStreamReader]* stream)
        CStatus DoPut(CFlightCallOptions& options,
                      CFlightDescriptor& descriptor,
                      shared_ptr[CSchema]& schema,
                      unique_ptr[CFlightStreamWriter]* stream,
                      unique_ptr[CFlightMetadataReader]* reader)
        CStatus DoExchange(CFlightCallOptions& options,
                           CFlightDescriptor& descriptor,
                           unique_ptr[CFlightStreamWriter]* writer,
                           unique_ptr[CFlightStreamReader]* reader)

    cdef cppclass CFlightStatusCode" arrow::flight::FlightStatusCode":
        bint operator==(CFlightStatusCode)

    CFlightStatusCode CFlightStatusInternal \
        " arrow::flight::FlightStatusCode::Internal"
    CFlightStatusCode CFlightStatusTimedOut \
        " arrow::flight::FlightStatusCode::TimedOut"
    CFlightStatusCode CFlightStatusCancelled \
        " arrow::flight::FlightStatusCode::Cancelled"
    CFlightStatusCode CFlightStatusUnauthenticated \
        " arrow::flight::FlightStatusCode::Unauthenticated"
    CFlightStatusCode CFlightStatusUnauthorized \
        " arrow::flight::FlightStatusCode::Unauthorized"
    CFlightStatusCode CFlightStatusUnavailable \
        " arrow::flight::FlightStatusCode::Unavailable"
    CFlightStatusCode CFlightStatusFailed \
        " arrow::flight::FlightStatusCode::Failed"

    cdef cppclass FlightStatusDetail" arrow::flight::FlightStatusDetail":
        CFlightStatusCode code()
        c_string extra_info()

        @staticmethod
        shared_ptr[FlightStatusDetail] UnwrapStatus(const CStatus& status)

    cdef cppclass FlightWriteSizeStatusDetail\
            " arrow::flight::FlightWriteSizeStatusDetail":
        int64_t limit()
        int64_t actual()

        @staticmethod
        shared_ptr[FlightWriteSizeStatusDetail] UnwrapStatus(
            const CStatus& status)

    cdef CStatus MakeFlightError" arrow::flight::MakeFlightError" \
        (CFlightStatusCode code, const c_string& message)

    cdef CStatus MakeFlightError" arrow::flight::MakeFlightError" \
        (CFlightStatusCode code,
         const c_string& message,
         const c_string& extra_info)

# Callbacks for implementing Flight servers
# Use typedef to emulate syntax for std::function<void(..)>
ctypedef CStatus cb_list_flights(object, const CServerCallContext&,
                                 const CCriteria*,
                                 unique_ptr[CFlightListing]*)
ctypedef CStatus cb_get_flight_info(object, const CServerCallContext&,
                                    const CFlightDescriptor&,
                                    unique_ptr[CFlightInfo]*)
ctypedef CStatus cb_get_schema(object, const CServerCallContext&,
                               const CFlightDescriptor&,
                               unique_ptr[CSchemaResult]*)
ctypedef CStatus cb_do_put(object, const CServerCallContext&,
                           unique_ptr[CFlightMessageReader],
                           unique_ptr[CFlightMetadataWriter])
ctypedef CStatus cb_do_get(object, const CServerCallContext&,
                           const CTicket&,
                           unique_ptr[CFlightDataStream]*)
ctypedef CStatus cb_do_exchange(object, const CServerCallContext&,
                                unique_ptr[CFlightMessageReader],
                                unique_ptr[CFlightMessageWriter])
ctypedef CStatus cb_do_action(object, const CServerCallContext&,
                              const CAction&,
                              unique_ptr[CResultStream]*)
ctypedef CStatus cb_list_actions(object, const CServerCallContext&,
                                 vector[CActionType]*)
ctypedef CStatus cb_result_next(object, unique_ptr[CFlightResult]*)
ctypedef CStatus cb_data_stream_next(object, CFlightPayload*)
ctypedef CStatus cb_server_authenticate(object, CServerAuthSender*,
                                        CServerAuthReader*)
ctypedef CStatus cb_is_valid(object, const c_string&, c_string*)
ctypedef CStatus cb_client_authenticate(object, CClientAuthSender*,
                                        CClientAuthReader*)
ctypedef CStatus cb_get_token(object, c_string*)

ctypedef CStatus cb_middleware_sending_headers(object, CAddCallHeaders*)
ctypedef CStatus cb_middleware_call_completed(object, const CStatus&)
ctypedef CStatus cb_client_middleware_received_headers(
    object, const CCallHeaders&)
ctypedef CStatus cb_server_middleware_start_call(
    object,
    const CCallInfo&,
    const CCallHeaders&,
    shared_ptr[CServerMiddleware]*)
ctypedef CStatus cb_client_middleware_start_call(
    object,
    const CCallInfo&,
    unique_ptr[CClientMiddleware]*)

cdef extern from "arrow/python/flight.h" namespace "arrow::py::flight" nogil:
    cdef cppclass PyFlightServerVtable:
        PyFlightServerVtable()
        function[cb_list_flights] list_flights
        function[cb_get_flight_info] get_flight_info
        function[cb_get_schema] get_schema
        function[cb_do_put] do_put
        function[cb_do_get] do_get
        function[cb_do_exchange] do_exchange
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
        int port()
        CStatus ServeWithSignals() except *
        CStatus Shutdown()
        CStatus Wait()

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
                                     function[cb_data_stream_next] callback,
                                     const CIpcWriteOptions& options)

    cdef cppclass PyServerMiddlewareVtable\
            " arrow::py::flight::PyServerMiddleware::Vtable":
        PyServerMiddlewareVtable()
        function[cb_middleware_sending_headers] sending_headers
        function[cb_middleware_call_completed] call_completed

    cdef cppclass PyClientMiddlewareVtable\
            " arrow::py::flight::PyClientMiddleware::Vtable":
        PyClientMiddlewareVtable()
        function[cb_middleware_sending_headers] sending_headers
        function[cb_client_middleware_received_headers] received_headers
        function[cb_middleware_call_completed] call_completed

    cdef cppclass CPyServerMiddleware\
            " arrow::py::flight::PyServerMiddleware"(CServerMiddleware):
        CPyServerMiddleware(object middleware, PyServerMiddlewareVtable vtable)
        void* py_object()

    cdef cppclass CPyServerMiddlewareFactory\
            " arrow::py::flight::PyServerMiddlewareFactory"\
            (CServerMiddlewareFactory):
        CPyServerMiddlewareFactory(
            object factory,
            function[cb_server_middleware_start_call] start_call)

    cdef cppclass CPyClientMiddleware\
            " arrow::py::flight::PyClientMiddleware"(CClientMiddleware):
        CPyClientMiddleware(object middleware, PyClientMiddlewareVtable vtable)

    cdef cppclass CPyClientMiddlewareFactory\
            " arrow::py::flight::PyClientMiddlewareFactory"\
            (CClientMiddlewareFactory):
        CPyClientMiddlewareFactory(
            object factory,
            function[cb_client_middleware_start_call] start_call)

    cdef CStatus CreateFlightInfo" arrow::py::flight::CreateFlightInfo"(
        shared_ptr[CSchema] schema,
        CFlightDescriptor& descriptor,
        vector[CFlightEndpoint] endpoints,
        int64_t total_records,
        int64_t total_bytes,
        unique_ptr[CFlightInfo]* out)

    cdef CStatus CreateSchemaResult" arrow::py::flight::CreateSchemaResult"(
        shared_ptr[CSchema] schema,
        unique_ptr[CSchemaResult]* out)

    cdef CStatus DeserializeBasicAuth\
        " arrow::py::flight::DeserializeBasicAuth"(
            c_string buf,
            unique_ptr[CBasicAuth]* out)

    cdef CStatus SerializeBasicAuth" arrow::py::flight::SerializeBasicAuth"(
        CBasicAuth basic_auth,
        c_string* out)


cdef extern from "arrow/util/variant.h" namespace "arrow" nogil:
    cdef cppclass CIntStringVariant" arrow::util::variant<int, std::string>":
        CIntStringVariant()
        CIntStringVariant(int)
        CIntStringVariant(c_string)

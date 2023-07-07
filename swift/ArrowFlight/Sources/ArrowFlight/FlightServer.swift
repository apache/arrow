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

import Foundation
import GRPC
import NIO
import NIOConcurrencyHelpers
import SwiftProtobuf
import Arrow

public enum ArrowFlightError: Error {
    case Unknown(String?)
    case NotImplemented(String? = nil)
    case EmptyCollection
    case IOError(String? = nil)
}

public func schemaToArrowStream(_ schema: ArrowSchema) throws -> Data {
    let arrowWriter = ArrowWriter()
    switch arrowWriter.toStream(ArrowWriter.Info(.schema, schema: schema)) {
    case .success(let result):
        return result
    case .failure(let error):
        throw error
    }
}

public func streamToArrowSchema(_ schema: Data) throws -> ArrowSchema {
    let schemaResult = ArrowReader().fromStream(schema)
    switch schemaResult {
    case .success(let result):
        if let retSchema = result.schema {
            return retSchema
        }
        
        throw ArrowFlightError.IOError("Schema not found")
    case .failure(let error):
        throw error
    }
}

public protocol ArrowFlightServer {
    func listFlights(_ criteria: FlightCriteria, writer: FlightInfoStreamWriter) async throws
    func getFlightInfo(_ request: FlightDescriptor) async throws -> FlightInfo
    func getSchema(_ request: FlightDescriptor) async throws -> ArrowFlight.FlightSchemaResult
    func listActions(_ writer: ActionTypeStreamWriter) async throws
    func doAction(_ action: FlightAction, writer: ResultStreamWriter) async throws
    func doGet(_ ticket: FlightTicket, writer: RecordBatchStreamWriter) async throws;
    func doPut(_ reader: RecordBatchStreamReader, writer: PutResultDataStreamWriter) async throws
    func doExchange(_ reader: RecordBatchStreamReader, writer: RecordBatchStreamWriter) async throws
}

public class DefaultFlightServer: ArrowFlightServer {
    public func listFlights(_ criteria: FlightCriteria, writer: FlightInfoStreamWriter) async throws {
        throw ArrowFlightError.NotImplemented()
    }
    
    public func getFlightInfo(_ request: FlightDescriptor) async throws -> FlightInfo {
        throw ArrowFlightError.NotImplemented()
    }
    
    public func getSchema(_ request: FlightDescriptor) async throws -> ArrowFlight.FlightSchemaResult {
        throw ArrowFlightError.NotImplemented()
    }
    
    public func listActions(_ writer: ActionTypeStreamWriter) async throws {
        throw ArrowFlightError.NotImplemented()
    }
    
    public func doAction(_ action: FlightAction, writer: ResultStreamWriter) async throws {
        throw ArrowFlightError.NotImplemented()
    }
    
    public func doGet(_ ticket: FlightTicket, writer: RecordBatchStreamWriter) async throws {
        throw ArrowFlightError.NotImplemented()
    }
    
    public func doPut(_ reader: RecordBatchStreamReader, writer: PutResultDataStreamWriter) async throws {
        throw ArrowFlightError.NotImplemented()
    }
    
    public func doExchange(_ reader: RecordBatchStreamReader, writer: RecordBatchStreamWriter) async throws {
        throw ArrowFlightError.NotImplemented()
    }
}

public func MakeFlightServer(_ handler: ArrowFlightServer) -> CallHandlerProvider {
  return InternalFlightServer(handler)
}

internal final class InternalFlightServer : Arrow_Flight_Protocol_FlightServiceAsyncProvider {
    let arrowFlightServer: ArrowFlightServer?
    
    init(_ arrowFlightServer: ArrowFlightServer?) {
        self.arrowFlightServer = arrowFlightServer
    }
    
    func handshake(requestStream: GRPC.GRPCAsyncRequestStream<Arrow_Flight_Protocol_HandshakeRequest>, responseStream: GRPC.GRPCAsyncResponseStreamWriter<Arrow_Flight_Protocol_HandshakeResponse>, context: GRPC.GRPCAsyncServerCallContext) async throws {
        throw ArrowFlightError.NotImplemented()
    }
    
    func listFlights(request: Arrow_Flight_Protocol_Criteria, responseStream: GRPC.GRPCAsyncResponseStreamWriter<Arrow_Flight_Protocol_FlightInfo>, context: GRPC.GRPCAsyncServerCallContext) async throws {
        if let server = arrowFlightServer {
            let writer = FlightInfoStreamWriter(responseStream)
            try await server.listFlights(FlightCriteria(request), writer: writer)
            return
        }
        
        throw ArrowFlightError.NotImplemented()
    }
    
    func getFlightInfo(request: Arrow_Flight_Protocol_FlightDescriptor, context: GRPC.GRPCAsyncServerCallContext) async throws -> Arrow_Flight_Protocol_FlightInfo {
        if let server = arrowFlightServer {
            return try await server.getFlightInfo(FlightDescriptor(request)).toProtocol()
        }
        
        throw ArrowFlightError.NotImplemented()
    }
    
    func getSchema(request: Arrow_Flight_Protocol_FlightDescriptor, context: GRPC.GRPCAsyncServerCallContext) async throws -> Arrow_Flight_Protocol_SchemaResult {
        if let server = arrowFlightServer {
            return try await server.getSchema(FlightDescriptor(request)).toProtocol()
        }
        
        throw ArrowFlightError.NotImplemented()
    }
    
    func doGet(request: Arrow_Flight_Protocol_Ticket, responseStream: GRPC.GRPCAsyncResponseStreamWriter<Arrow_Flight_Protocol_FlightData>, context: GRPC.GRPCAsyncServerCallContext) async throws {
        if let server = arrowFlightServer {
            let writer = RecordBatchStreamWriter(responseStream)
            let ticket = FlightTicket(request)
            try await server.doGet(ticket, writer: writer)
            return
        }
        
        throw ArrowFlightError.NotImplemented()        
    }
    
    func doPut(requestStream: GRPC.GRPCAsyncRequestStream<Arrow_Flight_Protocol_FlightData>, responseStream: GRPC.GRPCAsyncResponseStreamWriter<Arrow_Flight_Protocol_PutResult>, context: GRPC.GRPCAsyncServerCallContext) async throws {
        if let server = arrowFlightServer {
            let reader = RecordBatchStreamReader(requestStream)
            let writer = PutResultDataStreamWriter(responseStream)
            try await server.doPut(reader, writer: writer)
            return
        }
        
        throw ArrowFlightError.NotImplemented()
    }
    
    func doExchange(requestStream: GRPC.GRPCAsyncRequestStream<Arrow_Flight_Protocol_FlightData>, responseStream: GRPC.GRPCAsyncResponseStreamWriter<Arrow_Flight_Protocol_FlightData>, context: GRPC.GRPCAsyncServerCallContext) async throws {
        if let server = arrowFlightServer {
            let reader = RecordBatchStreamReader(requestStream)
            let writer = RecordBatchStreamWriter(responseStream)
            try await server.doExchange(reader, writer: writer)
            return
        }
        
        throw ArrowFlightError.NotImplemented()
    }
    
    func doAction(request: Arrow_Flight_Protocol_Action, responseStream: GRPC.GRPCAsyncResponseStreamWriter<Arrow_Flight_Protocol_Result>, context: GRPC.GRPCAsyncServerCallContext) async throws {
        if let server = arrowFlightServer {
            try await server.doAction(FlightAction(request), writer: ResultStreamWriter(responseStream))
            return
        }
        
        throw ArrowFlightError.NotImplemented()
    }
    
    func listActions(request: Arrow_Flight_Protocol_Empty, responseStream: GRPC.GRPCAsyncResponseStreamWriter<Arrow_Flight_Protocol_ActionType>, context: GRPC.GRPCAsyncServerCallContext) async throws {
        if let server = arrowFlightServer {
            let writer = ActionTypeStreamWriter(responseStream)
            try await server.listActions(writer)
            return
        }
        
        throw ArrowFlightError.NotImplemented()
    }
    
  internal var interceptors: Arrow_Flight_Protocol_FlightServiceServerInterceptorFactoryProtocol? { get { return nil } }

}

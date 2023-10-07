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

import struct Foundation.Data
import struct Foundation.URL
import GRPC
import NIOCore
import NIOPosix
import Arrow

public class FlightClient {
    let client: Arrow_Flight_Protocol_FlightServiceAsyncClient
    public init(channel: GRPCChannel) {
        client = Arrow_Flight_Protocol_FlightServiceAsyncClient(channel: channel)
    }

    public func listActions(_ closure: (FlightActionType) -> Void) async throws {
        let listActions = client.makeListActionsCall(Arrow_Flight_Protocol_Empty())
        for try await data in listActions.responseStream {
            closure(FlightActionType(data))
        }
    }

    public func listFlights(_ criteria: FlightCriteria, closure: (FlightInfo) throws -> Void) async throws {
        let listFlights = client.makeListFlightsCall(criteria.toProtocol())
        for try await data in listFlights.responseStream {
            try closure(FlightInfo(data))
        }
    }

    public func doAction(_ action: FlightAction, closure: (FlightResult) throws -> Void) async throws {
        let actionResponse = client.makeDoActionCall(action.toProtocol())
        for try await data in actionResponse.responseStream {
            try closure(FlightResult(data))
        }
    }

    public func getSchema(_ descriptor: FlightDescriptor) async throws -> FlightSchemaResult {
        let schemaResultResponse = client.makeGetSchemaCall(descriptor.toProtocol())
        return FlightSchemaResult(try await schemaResultResponse.response)
    }

    public func doGet(
        _ ticket: FlightTicket,
        readerResultClosure: (ArrowReader.ArrowReaderResult) throws -> Void) async throws {
        let getResult = client.makeDoGetCall(ticket.toProtocol())
        let reader = ArrowReader()
        for try await data in getResult.responseStream {
            switch reader.fromStream(data.dataBody) {
            case .success(let rb):
                try readerResultClosure(rb)
            case .failure(let error):
                throw error
            }
        }
    }

    public func doGet(_ ticket: FlightTicket, flightDataClosure: (FlightData) throws -> Void) async throws {
        let getResult = client.makeDoGetCall(ticket.toProtocol())
        for try await data in getResult.responseStream {
            try flightDataClosure(FlightData(data))
        }
    }

    public func doPut(_ recordBatchs: [RecordBatch], closure: (FlightPutResult) throws -> Void) async throws {
        if recordBatchs.isEmpty {
            throw ArrowFlightError.emptyCollection
        }

        let putCall = client.makeDoPutCall()
        let writer = ArrowWriter()
        let writerInfo = ArrowWriter.Info(.recordbatch, schema: recordBatchs[0].schema, batches: recordBatchs)
        switch writer.toStream(writerInfo) {
        case .success(let data):
            try await putCall.requestStream.send(FlightData(data).toProtocol())
            putCall.requestStream.finish()
            for try await response in putCall.responseStream {
                try closure(FlightPutResult(response))
            }
        case .failure(let error):
            throw error
        }
    }

    public func doPut(flightData: FlightData, closure: (FlightPutResult) throws -> Void) async throws {
        let putCall = client.makeDoPutCall()
        try await putCall.requestStream.send(flightData.toProtocol())
        putCall.requestStream.finish()
        for try await response in putCall.responseStream {
            try closure(FlightPutResult(response))
        }
    }

    public func doExchange(
        _ recordBatchs: [RecordBatch],
        closure: (ArrowReader.ArrowReaderResult) throws -> Void) async throws {
        if recordBatchs.isEmpty {
            throw ArrowFlightError.emptyCollection
        }

        let exchangeCall = client.makeDoExchangeCall()
        let writer = ArrowWriter()
        let info = ArrowWriter.Info(.recordbatch, schema: recordBatchs[0].schema, batches: recordBatchs)
        switch writer.toStream(info) {
        case .success(let data):
            let request = Arrow_Flight_Protocol_FlightData.with {
                $0.dataBody = data
            }
            try await exchangeCall.requestStream.send(request)
            exchangeCall.requestStream.finish()
            let reader = ArrowReader()
            for try await response in exchangeCall.responseStream {
                switch reader.fromStream(response.dataBody) {
                case .success(let rbResult):
                    try closure(rbResult)
                case .failure(let error):
                    throw error
                }
            }
        case .failure(let error):
            throw error
        }
    }

    public func doExchange(fligthData: FlightData, closure: (FlightData) throws -> Void) async throws {
        let exchangeCall = client.makeDoExchangeCall()
        try await exchangeCall.requestStream.send(fligthData.toProtocol())
        exchangeCall.requestStream.finish()
        for try await response in exchangeCall.responseStream {
            try closure(FlightData(response))
        }
    }
}

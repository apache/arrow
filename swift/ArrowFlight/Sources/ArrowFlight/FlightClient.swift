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
    let allowReadingUnalignedBuffers: Bool

    public init(channel: GRPCChannel, allowReadingUnalignedBuffers: Bool = false ) {
        client = Arrow_Flight_Protocol_FlightServiceAsyncClient(channel: channel)
        self.allowReadingUnalignedBuffers = allowReadingUnalignedBuffers
    }

    private func readMessages(
        _ responseStream: GRPCAsyncResponseStream<Arrow_Flight_Protocol_FlightData>
    ) async throws -> ArrowReader.ArrowReaderResult {
        let reader = ArrowReader()
        let arrowResult = ArrowReader.makeArrowReaderResult()
        for try await data in responseStream {
            switch reader.fromMessage(
                data.dataHeader,
                dataBody: data.dataBody,
                result: arrowResult,
                useUnalignedBuffers: allowReadingUnalignedBuffers) {
            case .success:
                continue
            case .failure(let error):
                throw error
            }
        }

        return arrowResult
    }

    private func writeBatches(
        _ requestStream: GRPCAsyncRequestStreamWriter<Arrow_Flight_Protocol_FlightData>,
        descriptor: FlightDescriptor,
        recordBatches: [RecordBatch]
    ) async throws {
        let writer = ArrowWriter()
        switch writer.toMessage(recordBatches[0].schema) {
        case .success(let schemaData):
            try await requestStream.send(
                FlightData(
                    schemaData,
                    dataBody: Data(),
                    flightDescriptor: descriptor).toProtocol())
            for recordBatch in recordBatches {
                switch writer.toMessage(recordBatch) {
                case .success(let data):
                    try await requestStream.send(
                        FlightData(
                            data[0],
                            dataBody: data[1],
                            flightDescriptor: descriptor).toProtocol())
                case .failure(let error):
                    throw error
                }
            }
            requestStream.finish()
        case .failure(let error):
            throw error
        }
    }

    public func listActions(_ closure: (FlightActionType) -> Void) async throws {
        let listActions = client.makeListActionsCall(Arrow_Flight_Protocol_Empty())
        for try await data in listActions.responseStream {
            closure(FlightActionType(data))
        }
    }

    public func listFlights(
        _ criteria: FlightCriteria,
        closure: (FlightInfo) throws -> Void) async throws {
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
        try readerResultClosure(try await readMessages(getResult.responseStream))
    }

    public func doGet(
        _ ticket: FlightTicket,
        flightDataClosure: (FlightData) throws -> Void) async throws {
        let getResult = client.makeDoGetCall(ticket.toProtocol())
        for try await data in getResult.responseStream {
            try flightDataClosure(FlightData(data))
        }
    }

    public func doPut(
        _ descriptor: FlightDescriptor,
        recordBatches: [RecordBatch],
        closure: (FlightPutResult) throws -> Void) async throws {
        if recordBatches.isEmpty {
            throw ArrowFlightError.emptyCollection
        }

        let putCall = client.makeDoPutCall()
        try await writeBatches(putCall.requestStream, descriptor: descriptor, recordBatches: recordBatches)
        var closureCalled = false
        for try await response in putCall.responseStream {
            try closure(FlightPutResult(response))
            closureCalled = true
        }

        if !closureCalled {
            try closure(FlightPutResult())
        }
    }

    public func doPut(_ flightData: FlightData, closure: (FlightPutResult) throws -> Void) async throws {
        let putCall = client.makeDoPutCall()
        try await putCall.requestStream.send(flightData.toProtocol())
        putCall.requestStream.finish()
        var closureCalled = false
        for try await response in putCall.responseStream {
            try closure(FlightPutResult(response))
            closureCalled = true
        }

        if !closureCalled {
            try closure(FlightPutResult())
        }
    }

    public func doExchange(
        _ descriptor: FlightDescriptor,
        recordBatches: [RecordBatch],
        closure: (ArrowReader.ArrowReaderResult) throws -> Void) async throws {
        if recordBatches.isEmpty {
            throw ArrowFlightError.emptyCollection
        }

        let exchangeCall = client.makeDoExchangeCall()
        try await writeBatches(exchangeCall.requestStream, descriptor: descriptor, recordBatches: recordBatches)
        try closure(try await readMessages(exchangeCall.responseStream))
    }

    public func doExchange(flightData: FlightData, closure: (FlightData) throws -> Void) async throws {
        let exchangeCall = client.makeDoExchangeCall()
        try await exchangeCall.requestStream.send(flightData.toProtocol())
        exchangeCall.requestStream.finish()
        for try await response in exchangeCall.responseStream {
            try closure(FlightData(response))
        }
    }
}

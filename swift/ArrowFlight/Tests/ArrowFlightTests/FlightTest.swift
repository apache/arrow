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

import XCTest
import struct Foundation.Data
import struct Foundation.URL
import GRPC
import NIOCore
import NIOPosix
import Arrow

@testable import ArrowFlight

func makeSchema() -> ArrowSchema {
    let schemaBuilder = ArrowSchema.Builder()
    return schemaBuilder.addField("col1", type: ArrowType(ArrowType.ArrowDouble), isNullable: true)
        .addField("col2", type: ArrowType(ArrowType.ArrowString), isNullable: false)
        .addField("col3", type: ArrowType(ArrowType.ArrowDate32), isNullable: false)
        .finish()
}

func makeRecordBatch() throws -> RecordBatch {
    let doubleBuilder: NumberArrayBuilder<Double> = try ArrowArrayBuilders.loadNumberArrayBuilder()
    doubleBuilder.append(11.11)
    doubleBuilder.append(22.22)
    doubleBuilder.append(33.33)
    doubleBuilder.append(44.44)
    let stringBuilder = try ArrowArrayBuilders.loadStringArrayBuilder()
    stringBuilder.append("test10")
    stringBuilder.append("test22")
    stringBuilder.append("test33")
    stringBuilder.append("test44")
    let date32Builder = try ArrowArrayBuilders.loadDate32ArrayBuilder()
    let date2 = Date(timeIntervalSinceReferenceDate: 86400 * 1)
    let date1 = Date(timeIntervalSinceReferenceDate: 86400 * 5000 + 352)
    date32Builder.append(date1)
    date32Builder.append(date2)
    date32Builder.append(date1)
    date32Builder.append(date2)
    let doubleHolder = ArrowArrayHolder(try doubleBuilder.finish())
    let stringHolder = ArrowArrayHolder(try stringBuilder.finish())
    let date32Holder = ArrowArrayHolder(try date32Builder.finish())
    let result = RecordBatch.Builder()
        .addColumn("col1", arrowArray: doubleHolder)
        .addColumn("col2", arrowArray: stringHolder)
        .addColumn("col3", arrowArray: date32Holder)
        .finish()
    switch result {
    case .success(let recordBatch):
        return recordBatch
    case .failure(let error):
        throw error
    }
}

var flights = [String: FlightInfo]()
final class MyFlightServer: ArrowFlightServer {
    func doExchange(
        _ reader: ArrowFlight.RecordBatchStreamReader,
        writer: ArrowFlight.RecordBatchStreamWriter) async throws {
        do {
            for try await rbData in reader {
                let rb = rbData.0!
                XCTAssertEqual(rb.schema.fields.count, 3)
                XCTAssertEqual(rb.length, 4)
            }

            let rb = try makeRecordBatch()
            try await writer.write(rb)
        } catch {
            print("Unknown error: \(error)")
        }
    }

    func doPut(
        _ reader: ArrowFlight.RecordBatchStreamReader,
        writer: ArrowFlight.PutResultDataStreamWriter) async throws {
        for try await rbData in reader {
            let rb = rbData.0!
            let key = String(decoding: rbData.1!.cmd, as: UTF8.self)
            flights[key] = try FlightInfo(schemaToMessage(rb.schema), endpoints: [], descriptor: rbData.1)
            XCTAssertEqual(rb.schema.fields.count, 3)
            XCTAssertEqual(rb.length, 4)
            try await writer.write(FlightPutResult())
        }
    }

    func doGet(_ ticket: ArrowFlight.FlightTicket, writer: ArrowFlight.RecordBatchStreamWriter) async throws {
        try await writer.write(try makeRecordBatch())
    }

    func getSchema(_ request: ArrowFlight.FlightDescriptor) async throws -> ArrowFlight.FlightSchemaResult {
        XCTAssertEqual(String(bytes: request.cmd, encoding: .utf8)!, "schema info")
        XCTAssertEqual(request.type, .cmd)
        return try ArrowFlight.FlightSchemaResult(schemaToMessage(makeSchema()))
    }

    func getFlightInfo(_ request: ArrowFlight.FlightDescriptor) async throws -> ArrowFlight.FlightInfo {
        let key = String(decoding: request.cmd, as: UTF8.self)
        if flights[key] != nil {
            return ArrowFlight.FlightInfo(flights[key]!.toProtocol())
        }

        throw ArrowFlightError.ioError("Flight not found")
    }

    func listFlights(_ criteria: ArrowFlight.FlightCriteria, writer: ArrowFlight.FlightInfoStreamWriter) async throws {
        XCTAssertEqual(String(bytes: criteria.expression, encoding: .utf8), "flight criteria expression")
        for flightData in flights {
            try await writer.write(flightData.value)
        }
    }

    func listActions(_ writer: ArrowFlight.ActionTypeStreamWriter) async throws {
        try await writer.write(FlightActionType("clear", description: "Clear the stored flights."))
        try await writer.write(FlightActionType("shutdown", description: "Shut down this server."))
    }

    func doAction(_ action: FlightAction, writer: ResultStreamWriter) async throws {
        XCTAssertEqual(action.type, "healthcheck")
        XCTAssertEqual(String(bytes: action.body, encoding: .utf8)!, "healthcheck body")
        try await writer.write(FlightResult("test_action result".data(using: .utf8)!))
    }
}

struct FlightServerImpl {
    var port = 1234
    static var server: Server?
    static var group: MultiThreadedEventLoopGroup?
    static func run() async throws {
        do {
            // Create an event loop group for the server to run on.
            let group = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
            // Create a provider using the features we read.
            let provider = ArrowFlight.makeFlightServer(MyFlightServer())

            // Start the server and print its address once it has started.
            FlightServerImpl.server = try await Server.insecure(group: group)
                .withServiceProviders([provider])
                .bind(host: "localhost", port: 8088)
                .get()

            print("server started on port \(server!.channel.localAddress!.port!)")
            // Wait on the server's `onClose` future to stop the program from exiting.
        } catch {
            print("Unknown server error: \(error)")
        }
    }
}

public class FlightClientTester {
    var client: FlightClient?
    var group: MultiThreadedEventLoopGroup?
    var channel: GRPCChannel?

    init() async throws {
        // Load the features.
        let group = PlatformSupport.makeEventLoopGroup(loopCount: 1)
        let channel = try GRPCChannelPool.with(
            target: .host("localhost", port: 8088),
            transportSecurity: .plaintext,
            eventLoopGroup: group
        )

        client = FlightClient(channel: channel)
    }

    deinit {
        try? group?.syncShutdownGracefully()
        try? channel?.close().wait()
    }

    func listActionTest() async throws {
        var actionTypes = [FlightActionType]()
        try await client?.listActions({ action in
            actionTypes.append(action)
        })

        XCTAssertEqual(actionTypes.count, 2)

        XCTAssertEqual(actionTypes[0].type, "clear")
        XCTAssertEqual(actionTypes[0].description, "Clear the stored flights.")
        XCTAssertEqual(actionTypes[1].type, "shutdown")
        XCTAssertEqual(actionTypes[1].description, "Shut down this server.")
    }

    func listFlightsTest() async throws {
        let flightCriteria = FlightCriteria("flight criteria expression".data(using: .utf8)!)
        var numCalls = 0
        try await client?.listFlights(flightCriteria, closure: { data in
            if let schema = data.schema {
                XCTAssertGreaterThanOrEqual(schema.fields.count, 0)
                numCalls += 1
            }
        })

        XCTAssertEqual(numCalls, 2)
    }

    func doActionTest(_ type: String, actionBody: Data) async throws {
        let action = FlightAction(type, body: actionBody)
        var actionResults = [FlightResult]()
        try await client?.doAction(action, closure: { result in
            actionResults.append(result)
        })

        XCTAssertEqual(actionResults.count, 1)
        XCTAssertEqual(String(bytes: actionResults[0].body, encoding: .utf8), "test_action result")
    }

    func getSchemaTest(_ cmd: Data) async throws {
        let descriptor = FlightDescriptor(cmd: cmd)
        let schemaResult = try await client?.getSchema(descriptor)
        let schema = schemaResult!.schema!
        XCTAssertEqual(schema.fields.count, 3)
    }

    func doGetTest(_ flightData: Data) async throws {
        let ticket = FlightTicket(flightData)
        var numCall = 0
        try await client?.doGet(ticket, readerResultClosure: { rb in
            numCall += 1
            XCTAssertEqual(rb.schema!.fields.count, 3)
            XCTAssertEqual(rb.batches[0].length, 4)
            switch ArrowTable.from(recordBatches: rb.batches) {
            case .success(let table):
                    for column in table.columns {
                    switch column.type.id {
                    case .double:
                        let doubleArray = column.data() as? ChunkedArray<Double>
                        XCTAssertNotNil(doubleArray)
                        XCTAssertEqual(doubleArray?[0], 11.11)
                        XCTAssertEqual(doubleArray?.asString(0), "11.11")
                    default:
                        continue
                    }
                }
            case .failure(let error):
                throw error
            }
        })

        XCTAssertEqual(numCall, 1)
    }

    func doGetTestFlightData(_ flightData: Data) async throws {
        let ticket = FlightTicket(flightData)
        var numCall = 0
        let reader = ArrowReader()
        let arrowResult = ArrowReader.makeArrowReaderResult()
        try await client?.doGet(ticket, flightDataClosure: { flightData in
            switch reader.fromMessage(flightData.dataHeader, dataBody: flightData.dataBody, result: arrowResult) {
            case .success:
                numCall += 1
            case .failure(let error):
                throw error
            }
        })

        XCTAssertEqual(numCall, 2)
    }

    func doPutTest(_ cmd: String) async throws {
        let descriptor = FlightDescriptor(cmd: cmd.data(using: .utf8)!)
        let rb = try makeRecordBatch()
        var numCall = 0
        try await client?.doPut(descriptor, recordBatches: [rb], closure: { _ in
            numCall += 1
        })

        XCTAssertEqual(numCall, 1)
    }

    func doExchangeTest() async throws {
        let descriptor = FlightDescriptor(cmd: "flight_ticket".data(using: .utf8)!)
        let rb = try makeRecordBatch()
        var numCall = 0
        try await client?.doExchange(descriptor, recordBatches: [rb], closure: { result in
            numCall += 1
            XCTAssertEqual(result.schema?.fields.count, 3)
            XCTAssertEqual(result.batches[0].length, 4)
        })

        XCTAssertEqual(numCall, 1)
    }
}

actor FlightServerData {
    public var serverup = false
    func setServerUp(_ serverUp: Bool) {
        self.serverup = serverUp
    }

    func isServerUp() -> Bool {
        return serverup
    }
}

final class FlightTest: XCTestCase {
    let serverData = FlightServerData()

    func testFlightServer() async throws {
        let basicTask = Task {
            try await FlightServerImpl.run()
            defer {
                print("server shutting down")
                do {
                    try FlightServerImpl.group?.syncShutdownGracefully()
                } catch {
                }
            }

            await serverData.setServerUp(true)
            try await FlightServerImpl.server?.onClose.get()
            return "done"
        }

        let secondTask = Task {
            defer {
                _ = FlightServerImpl.server?.close()
            }

            while await !serverData.isServerUp() {
                try await Task.sleep(nanoseconds: 1_000_000)
            }

            let clientImpl = try await FlightClientTester()
            try await clientImpl.listActionTest()
            try await clientImpl.doPutTest("flight_ticket")
            try await clientImpl.doPutTest("flight_another")
            try await clientImpl.listFlightsTest()
            try await clientImpl.doActionTest("healthcheck", actionBody: Data("healthcheck body".utf8))
            try await clientImpl.getSchemaTest(Data("schema info".utf8))
            try await clientImpl.doGetTest(Data("'flight_ticket'".utf8))
            try await clientImpl.doGetTestFlightData(Data("'flight_another'".utf8))
            try await clientImpl.doExchangeTest()
            return "done"
        }

        _ = try await [basicTask.value, secondTask.value]
        print("done running")
    }
}

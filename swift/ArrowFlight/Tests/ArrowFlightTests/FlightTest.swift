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
    return schemaBuilder.addField("col1", type: ArrowType(ArrowType.ArrowUInt8), isNullable: true)
        .addField("col2", type: ArrowType(ArrowType.ArrowString), isNullable: false)
        .addField("col3", type: ArrowType(ArrowType.ArrowDate32), isNullable: false)
        .finish()
}

func makeRecordBatch() throws -> RecordBatch {
    let uint8Builder: NumberArrayBuilder<UInt8> = try ArrowArrayBuilders.loadNumberArrayBuilder()
    uint8Builder.append(10)
    uint8Builder.append(22)
    uint8Builder.append(33)
    uint8Builder.append(44)
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
    let intHolder = ArrowArrayHolder(try uint8Builder.finish())
    let stringHolder = ArrowArrayHolder(try stringBuilder.finish())
    let date32Holder = ArrowArrayHolder(try date32Builder.finish())
    let result = RecordBatch.Builder()
        .addColumn("col1", arrowArray: intHolder)
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

final class MyFlightServer : ArrowFlightServer {
    func doExchange(_ reader: ArrowFlight.RecordBatchStreamReader, writer: ArrowFlight.RecordBatchStreamWriter) async throws {
        do {
            for try await rb in reader {
                XCTAssertEqual(rb.schema.fields.count, 3)
                XCTAssertEqual(rb.length, 4)
            }
            
            let rb = try makeRecordBatch()
            try await writer.write(rb)
        } catch {
            print("Unknown error: \(error)")
        }
    }
    
    func doPut(_ reader: ArrowFlight.RecordBatchStreamReader, writer: ArrowFlight.PutResultDataStreamWriter) async throws {
        for try await rb in reader {
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
        return try ArrowFlight.FlightSchemaResult(schemaToArrowStream(makeSchema()))
    }
    
    func getFlightInfo(_ request: ArrowFlight.FlightDescriptor) async throws -> ArrowFlight.FlightInfo {
        return ArrowFlight.FlightInfo(Data())
    }
    
    func listFlights(_ criteria: ArrowFlight.FlightCriteria, writer: ArrowFlight.FlightInfoStreamWriter) async throws {
        XCTAssertEqual(String(bytes: criteria.expression, encoding: .utf8), "flight criteria expression")
        let flight_info = try ArrowFlight.FlightInfo(schemaToArrowStream(makeSchema()))
        try await writer.write(flight_info)
    }
    
    func listActions(_ writer: ArrowFlight.ActionTypeStreamWriter) async throws {
        try await writer.write(FlightActionType("type1", description: "desc1"))
        try await writer.write(FlightActionType("type2", description: "desc2"))
    }
    
    func doAction(_ action: FlightAction, writer: ResultStreamWriter) async throws {
        XCTAssertEqual(action.type, "test_action")
        XCTAssertEqual(String(bytes: action.body, encoding: .utf8)!, "test_action body")
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
            let provider = ArrowFlight.MakeFlightServer(MyFlightServer())
            
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
        try await client?.listActions( { action in
            actionTypes.append(action)
        })
        
        XCTAssertEqual(actionTypes.count, 2)
        XCTAssertEqual(actionTypes[0].type, "type1")
        XCTAssertEqual(actionTypes[0].description, "desc1")
        XCTAssertEqual(actionTypes[1].type, "type2")
        XCTAssertEqual(actionTypes[1].description, "desc2")
    }
    
    func listFlightsTest() async throws {
        let flightCriteria = FlightCriteria("flight criteria expression".data(using: .utf8)!)
        var num_calls = 0
        try await client?.listFlights(flightCriteria, closure: { data in
            num_calls += 1
            let schema = try streamToArrowSchema(data.schema)
            XCTAssertEqual(schema.fields.count, 3)
        })
        
        XCTAssertEqual(num_calls, 1)
    }
    
    func doActionTest() async throws {
        let action = FlightAction("test_action", body: "test_action body".data(using: .utf8)!)
        var actionResults = [FlightResult]()
        try await client?.doAction(action, closure: { result in
            actionResults.append(result)
        })
        
        XCTAssertEqual(actionResults.count, 1)
        XCTAssertEqual(String(bytes:actionResults[0].body, encoding: .utf8), "test_action result")
    }
    
    func getSchemaTest() async throws {
        let descriptor = FlightDescriptor(cmd: "schema info".data(using: .utf8)!)
        let schemaResult = try await client?.getSchema(descriptor)
        let schema = try streamToArrowSchema(schemaResult!.schema)
        XCTAssertEqual(schema.fields.count, 3)
    }
    
    func doGetTest() async throws {
        let ticket = FlightTicket("flight_ticket test".data(using: .utf8)!)
        var num_call = 0
        try await client?.doGet(ticket, readerResultClosure: { rb in
            num_call += 1
            XCTAssertEqual(rb.schema!.fields.count, 3)
            XCTAssertEqual(rb.batches[0].length, 4)
        })
        
        XCTAssertEqual(num_call, 1)
    }
    
    func doGetTestFlightData() async throws {
        let ticket = FlightTicket("flight_ticket test".data(using: .utf8)!)
        var num_call = 0
        try await client?.doGet(ticket, flightDataClosure: { flightData in
            let reader = ArrowReader();
            let result = reader.fromStream(flightData.dataBody)
            switch result {
            case .success(let rb):
                XCTAssertEqual(rb.schema?.fields.count, 3)
                XCTAssertEqual(rb.batches[0].length, 4)
                num_call += 1
            case .failure(let error):
                throw error
            }
        })
        
        XCTAssertEqual(num_call, 1)
    }
    
    func doPutTest() async throws {
        let rb = try makeRecordBatch()
        var num_call = 0
        try await client?.doPut([rb], closure: { result in
            num_call += 1
        })
        
        XCTAssertEqual(num_call, 1)
    }

    func doExchangeTest() async throws {
        let rb = try makeRecordBatch()
        var num_call = 0
        try await client?.doExchange([rb], closure: { result in
            num_call += 1
            XCTAssertEqual(result.schema?.fields.count, 3)
            XCTAssertEqual(result.batches[0].length, 4)
        })
        
        XCTAssertEqual(num_call, 1)
    }
}

actor FlightServerData {
    public var serverup = false
    func SetServerUp(_ serverUp: Bool) {
        self.serverup = serverUp
    }
    
    func IsServerUp() -> Bool {
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
                try! FlightServerImpl.group?.syncShutdownGracefully()
            }
            
            await serverData.SetServerUp(true)
            try await FlightServerImpl.server?.onClose.get()
            return "done"
        }

        let secondTask = Task {
            defer {
                _ = FlightServerImpl.server?.close()
            }
            
            while await !serverData.IsServerUp() {
                try await Task.sleep(nanoseconds: 1_000_000)
            }
            
            let clientImpl = try await FlightClientTester()
            try await clientImpl.listActionTest()
            try await clientImpl.listFlightsTest()
            try await clientImpl.doActionTest()
            try await clientImpl.getSchemaTest()
            try await clientImpl.doGetTest()
            try await clientImpl.doGetTestFlightData()
            try await clientImpl.doPutTest()
            try await clientImpl.doExchangeTest()
            
            return "done"
        }
                
        let _ = try await [basicTask.value, secondTask.value]
        print("done running")
    }
}

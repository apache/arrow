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
import Arrow
import GRPC

public class RecordBatchStreamReader: AsyncSequence, AsyncIteratorProtocol {
    public typealias AsyncIterator = RecordBatchStreamReader
    public typealias Element = (Arrow.RecordBatch?, FlightDescriptor?)
    let reader = ArrowReader()
    var batches = [RecordBatch]()
    var descriptor: FlightDescriptor?
    var batchIndex = 0
    var streamIterator: any AsyncIteratorProtocol
    var useUnalignedBuffers: Bool
    let stream: GRPC.GRPCAsyncRequestStream<Arrow_Flight_Protocol_FlightData>
    init(_ stream: GRPC.GRPCAsyncRequestStream<Arrow_Flight_Protocol_FlightData>,
         useUnalignedBuffers: Bool = false) {
        self.stream = stream
        self.streamIterator = self.stream.makeAsyncIterator()
        self.useUnalignedBuffers = useUnalignedBuffers
    }

    public func next() async throws -> (Arrow.RecordBatch?, FlightDescriptor?)? {
        guard !Task.isCancelled else {
            return nil
        }

        if batchIndex < batches.count {
            let batch = batches[batchIndex]
            batchIndex += 1
            return (batch, descriptor)
        }

        let result = ArrowReader.makeArrowReaderResult()
        while true {
            let streamData = try await self.streamIterator.next()
            if streamData == nil {
                return nil
            }

            let flightData = (streamData as? Arrow_Flight_Protocol_FlightData)!
            let dataBody = flightData.dataBody
            let dataHeader = flightData.dataHeader
            descriptor = FlightDescriptor(flightData.flightDescriptor)
            switch reader.fromMessage(
                dataHeader,
                dataBody: dataBody,
                result: result,
                useUnalignedBuffers: useUnalignedBuffers) {
            case .success(()):
                if result.batches.count > 0 {
                    batches = result.batches
                    batchIndex = 1
                    return (batches[0], descriptor)
                }
            case .failure(let error):
                throw error
            }
        }
    }

    public func makeAsyncIterator() -> RecordBatchStreamReader {
        self
    }
}

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

import FlatBuffers
import Foundation

public enum ArrowError: Error {
    case runtimeError(String)
}

let FILEMARKER = "ARROW1"
let CONTINUATIONMARKER = -1

public class ArrowReader {
    private struct DataLoadInfo {
        let recordBatch: org_apache_arrow_flatbuf_RecordBatch
        let field: org_apache_arrow_flatbuf_Field
        let nodeIndex: Int32
        let bufferIndex: Int32
        let fileData: Data
        let messageOffset: Int64
    }

    private func loadSchema(_ schema: org_apache_arrow_flatbuf_Schema) throws -> ArrowSchema {
        let builder = ArrowSchema.Builder()
        for index in 0 ..< schema.fieldsCount {
            let field = schema.fields(at: index)!
            let arrowField = ArrowField(field.name!, type: findArrowType(field), isNullable: field.nullable)
            builder.addField(arrowField)
            if field.typeType == .struct_ {
                throw ValidationError.unknownType
            }
        }

        return builder.finish()
    }

    private func loadPrimitiveData(_ loadInfo: DataLoadInfo) throws -> ChunkedArrayHolder {
        let node = loadInfo.recordBatch.nodes(at: loadInfo.nodeIndex)!
        try validateBufferIndex(loadInfo.recordBatch, index: loadInfo.bufferIndex)
        let nullBuffer = loadInfo.recordBatch.buffers(at: loadInfo.bufferIndex)!
        let arrowNullBuffer = makeBuffer(nullBuffer, fileData: loadInfo.fileData,
                                         length: UInt(node.nullCount), messageOffset: loadInfo.messageOffset)
        try validateBufferIndex(loadInfo.recordBatch, index: loadInfo.bufferIndex + 1)
        let valueBuffer = loadInfo.recordBatch.buffers(at: loadInfo.bufferIndex + 1)!
        let arrowValueBuffer = makeBuffer(valueBuffer, fileData: loadInfo.fileData,
                                          length: UInt(node.length), messageOffset: loadInfo.messageOffset)
        return try makeArrayHolder(loadInfo.field, buffers: [arrowNullBuffer, arrowValueBuffer])
    }

    private func loadVariableData(_ loadInfo: DataLoadInfo) throws -> ChunkedArrayHolder {
        let node = loadInfo.recordBatch.nodes(at: loadInfo.nodeIndex)!
        try validateBufferIndex(loadInfo.recordBatch, index: loadInfo.bufferIndex)
        let nullBuffer = loadInfo.recordBatch.buffers(at: loadInfo.bufferIndex)!
        let arrowNullBuffer = makeBuffer(nullBuffer, fileData: loadInfo.fileData,
                                         length: UInt(node.nullCount), messageOffset: loadInfo.messageOffset)
        try validateBufferIndex(loadInfo.recordBatch, index: loadInfo.bufferIndex + 1)
        let offsetBuffer = loadInfo.recordBatch.buffers(at: loadInfo.bufferIndex + 1)!
        let arrowOffsetBuffer = makeBuffer(offsetBuffer, fileData: loadInfo.fileData,
                                           length: UInt(node.length), messageOffset: loadInfo.messageOffset)
        try validateBufferIndex(loadInfo.recordBatch, index: loadInfo.bufferIndex + 2)
        let valueBuffer = loadInfo.recordBatch.buffers(at: loadInfo.bufferIndex + 2)!
        let arrowValueBuffer = makeBuffer(valueBuffer, fileData: loadInfo.fileData,
                                          length: UInt(node.length), messageOffset: loadInfo.messageOffset)
        return try makeArrayHolder(loadInfo.field, buffers: [arrowNullBuffer, arrowOffsetBuffer, arrowValueBuffer])
    }

    private func loadRecordBatch(_ message: org_apache_arrow_flatbuf_Message, schema: org_apache_arrow_flatbuf_Schema,
                                 data: Data, messageEndOffset: Int64) throws -> RecordBatch {
        let recordBatch = message.header(type: org_apache_arrow_flatbuf_RecordBatch.self)
        let nodesCount = recordBatch?.nodesCount ?? 0
        var bufferIndex: Int32 = 0
        var columns: [ChunkedArrayHolder] = []
        let arrowSchema = try loadSchema(schema)
        for nodeIndex in 0 ..< nodesCount {
            let field = schema.fields(at: nodeIndex)!
            let loadInfo = DataLoadInfo(recordBatch: recordBatch!, field: field,
                                        nodeIndex: nodeIndex, bufferIndex: bufferIndex,
                                        fileData: data, messageOffset: messageEndOffset)
            if isFixedPrimitive(field.typeType) {
                let holder = try loadPrimitiveData(loadInfo)
                columns.append(holder)
                bufferIndex += 2
            } else {
                let holder = try loadVariableData(loadInfo)
                columns.append(holder)
                bufferIndex += 3
            }
        }

        return RecordBatch(arrowSchema, columns: columns)
    }

    public func fromStream(_ fileData: Data) throws -> [RecordBatch] {
        let footerLength = fileData.withUnsafeBytes { rawBuffer in
            rawBuffer.loadUnaligned(fromByteOffset: fileData.count - 4, as: Int32.self)
        }

        var recordBatchs: [RecordBatch] = []
        let footerStartOffset = fileData.count - Int(footerLength + 4)
        let footerData = fileData[footerStartOffset...]
        let footerBuffer = ByteBuffer(data: footerData)
        let footer = org_apache_arrow_flatbuf_Footer.getRootAsFooter(bb: footerBuffer)
        for index in 0 ..< footer.recordBatchesCount {
            let recordBatch = footer.recordBatches(at: index)!
            var messageLength = fileData.withUnsafeBytes { rawBuffer in
                rawBuffer.loadUnaligned(fromByteOffset: Int(recordBatch.offset), as: Int32.self)
            }

            var messageOffset: Int64 = 1
            if messageLength == CONTINUATIONMARKER {
                messageOffset += 1
                messageLength = fileData.withUnsafeBytes { rawBuffer in
                    rawBuffer.loadUnaligned(
                        fromByteOffset: Int(recordBatch.offset + Int64(MemoryLayout<Int32>.size)),
                        as: Int32.self)
                }
            }

            let messageStartOffset = recordBatch.offset + (Int64(MemoryLayout<Int32>.size) * messageOffset)
            let messageEndOffset = messageStartOffset + Int64(messageLength)
            let recordBatchData = fileData[messageStartOffset ... messageEndOffset]
            let mbb = ByteBuffer(data: recordBatchData)
            let message = org_apache_arrow_flatbuf_Message.getRootAsMessage(bb: mbb)
            switch message.headerType {
            case .recordbatch:
                let recordBatch = try loadRecordBatch(message, schema: footer.schema!,
                                                      data: fileData, messageEndOffset: messageEndOffset)
                recordBatchs.append(recordBatch)
            default:
                print("Unhandled header type: \(message.headerType)")
            }
        }

        return recordBatchs
    }

    public func fromFile(_ fileURL: URL) throws -> [RecordBatch] {
        let fileData = try Data(contentsOf: fileURL)
        if !validateFileData(fileData) {
            throw ArrowError.runtimeError("Not a valid arrow file.")
        }

        let markerLength = FILEMARKER.utf8.count
        let footerLengthEnd = Int(fileData.count - markerLength)
        let data = fileData[..<(footerLengthEnd)]
        return try fromStream(data)
    }
}

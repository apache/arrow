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

    public class ArrowReaderResult {
        fileprivate var messageSchema: org_apache_arrow_flatbuf_Schema?
        public var schema: ArrowSchema?
        public var batches = [RecordBatch]()
    }

    public init() {}

    private func loadSchema(_ schema: org_apache_arrow_flatbuf_Schema) -> Result<ArrowSchema, ArrowError> {
        let builder = ArrowSchema.Builder()
        for index in 0 ..< schema.fieldsCount {
            let field = schema.fields(at: index)!
            let fieldType = findArrowType(field)
            if fieldType.info == ArrowType.ArrowUnknown {
                return .failure(.unknownType("Unsupported field type found: \(field.typeType)"))
            }
            let arrowField = ArrowField(field.name!, type: fieldType, isNullable: field.nullable)
            builder.addField(arrowField)
        }

        return .success(builder.finish())
    }

    private func loadPrimitiveData(_ loadInfo: DataLoadInfo) -> Result<ArrowArrayHolder, ArrowError> {
        do {
            let node = loadInfo.recordBatch.nodes(at: loadInfo.nodeIndex)!
            let nullLength = UInt(ceil(Double(node.length) / 8))
            try validateBufferIndex(loadInfo.recordBatch, index: loadInfo.bufferIndex)
            let nullBuffer = loadInfo.recordBatch.buffers(at: loadInfo.bufferIndex)!
            let arrowNullBuffer = makeBuffer(nullBuffer, fileData: loadInfo.fileData,
                                             length: nullLength, messageOffset: loadInfo.messageOffset)
            try validateBufferIndex(loadInfo.recordBatch, index: loadInfo.bufferIndex + 1)
            let valueBuffer = loadInfo.recordBatch.buffers(at: loadInfo.bufferIndex + 1)!
            let arrowValueBuffer = makeBuffer(valueBuffer, fileData: loadInfo.fileData,
                                              length: UInt(node.length), messageOffset: loadInfo.messageOffset)
            return makeArrayHolder(loadInfo.field, buffers: [arrowNullBuffer, arrowValueBuffer],
                                   nullCount: UInt(node.nullCount))
        } catch let error as ArrowError {
            return .failure(error)
        } catch {
            return .failure(.unknownError("\(error)"))
        }
    }

    private func loadVariableData(_ loadInfo: DataLoadInfo) -> Result<ArrowArrayHolder, ArrowError> {
        let node = loadInfo.recordBatch.nodes(at: loadInfo.nodeIndex)!
        do {
            let nullLength = UInt(ceil(Double(node.length) / 8))
            try validateBufferIndex(loadInfo.recordBatch, index: loadInfo.bufferIndex)
            let nullBuffer = loadInfo.recordBatch.buffers(at: loadInfo.bufferIndex)!
            let arrowNullBuffer = makeBuffer(nullBuffer, fileData: loadInfo.fileData,
                                             length: nullLength, messageOffset: loadInfo.messageOffset)
            try validateBufferIndex(loadInfo.recordBatch, index: loadInfo.bufferIndex + 1)
            let offsetBuffer = loadInfo.recordBatch.buffers(at: loadInfo.bufferIndex + 1)!
            let arrowOffsetBuffer = makeBuffer(offsetBuffer, fileData: loadInfo.fileData,
                                               length: UInt(node.length), messageOffset: loadInfo.messageOffset)
            try validateBufferIndex(loadInfo.recordBatch, index: loadInfo.bufferIndex + 2)
            let valueBuffer = loadInfo.recordBatch.buffers(at: loadInfo.bufferIndex + 2)!
            let arrowValueBuffer = makeBuffer(valueBuffer, fileData: loadInfo.fileData,
                                              length: UInt(node.length), messageOffset: loadInfo.messageOffset)
            return makeArrayHolder(loadInfo.field, buffers: [arrowNullBuffer, arrowOffsetBuffer, arrowValueBuffer],
                                   nullCount: UInt(node.nullCount))
        } catch let error as ArrowError {
            return .failure(error)
        } catch {
            return .failure(.unknownError("\(error)"))
        }
    }

    private func loadRecordBatch(
        _ recordBatch: org_apache_arrow_flatbuf_RecordBatch,
        schema: org_apache_arrow_flatbuf_Schema,
        arrowSchema: ArrowSchema,
        data: Data,
        messageEndOffset: Int64
    ) -> Result<RecordBatch, ArrowError> {
        let nodesCount = recordBatch.nodesCount
        var bufferIndex: Int32 = 0
        var columns: [ArrowArrayHolder] = []
        for nodeIndex in 0 ..< nodesCount {
            let field = schema.fields(at: nodeIndex)!
            let loadInfo = DataLoadInfo(recordBatch: recordBatch, field: field,
                                        nodeIndex: nodeIndex, bufferIndex: bufferIndex,
                                        fileData: data, messageOffset: messageEndOffset)
            var result: Result<ArrowArrayHolder, ArrowError>
            if isFixedPrimitive(field.typeType) {
                result = loadPrimitiveData(loadInfo)
                bufferIndex += 2
            } else {
                result = loadVariableData(loadInfo)
                bufferIndex += 3
            }

            switch result {
            case .success(let holder):
                columns.append(holder)
            case .failure(let error):
                return .failure(error)
            }
        }

        return .success(RecordBatch(arrowSchema, columns: columns))
    }

    public func fromStream( // swiftlint:disable:this function_body_length
        _ fileData: Data,
        useUnalignedBuffers: Bool = false
    ) -> Result<ArrowReaderResult, ArrowError> {
        let footerLength = fileData.withUnsafeBytes { rawBuffer in
            rawBuffer.loadUnaligned(fromByteOffset: fileData.count - 4, as: Int32.self)
        }

        let result = ArrowReaderResult()
        let footerStartOffset = fileData.count - Int(footerLength + 4)
        let footerData = fileData[footerStartOffset...]
        let footerBuffer = ByteBuffer(
            data: footerData,
            allowReadingUnalignedBuffers: useUnalignedBuffers)
        let footer = org_apache_arrow_flatbuf_Footer.getRootAsFooter(bb: footerBuffer)
        let schemaResult = loadSchema(footer.schema!)
        switch schemaResult {
        case .success(let schema):
            result.schema = schema
        case .failure(let error):
            return .failure(error)
        }

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
            let recordBatchData = fileData[messageStartOffset ..< messageEndOffset]
            let mbb = ByteBuffer(
                data: recordBatchData,
                allowReadingUnalignedBuffers: useUnalignedBuffers)
            let message = org_apache_arrow_flatbuf_Message.getRootAsMessage(bb: mbb)
            switch message.headerType {
            case .recordbatch:
                do {
                    let rbMessage = message.header(type: org_apache_arrow_flatbuf_RecordBatch.self)!
                    let recordBatch = try loadRecordBatch(
                        rbMessage,
                        schema: footer.schema!,
                        arrowSchema: result.schema!,
                        data: fileData,
                        messageEndOffset: messageEndOffset).get()
                    result.batches.append(recordBatch)
                } catch let error as ArrowError {
                    return .failure(error)
                } catch {
                    return .failure(.unknownError("Unexpected error: \(error)"))
                }
            default:
                return .failure(.unknownError("Unhandled header type: \(message.headerType)"))
            }
        }

        return .success(result)
    }

    public func fromFile(_ fileURL: URL) -> Result<ArrowReaderResult, ArrowError> {
        do {
            let fileData = try Data(contentsOf: fileURL)
            if !validateFileData(fileData) {
                return .failure(.ioError("Not a valid arrow file."))
            }

            let markerLength = FILEMARKER.utf8.count
            let footerLengthEnd = Int(fileData.count - markerLength)
            let data = fileData[..<(footerLengthEnd)]
            return fromStream(data)
        } catch {
            return .failure(.unknownError("Error loading file: \(error)"))
        }
    }

    static public func makeArrowReaderResult() -> ArrowReaderResult {
        return ArrowReaderResult()
    }

    public func fromMessage(
        _ dataHeader: Data,
        dataBody: Data,
        result: ArrowReaderResult,
        useUnalignedBuffers: Bool = false
    ) -> Result<Void, ArrowError> {
        let mbb = ByteBuffer(
            data: dataHeader,
            allowReadingUnalignedBuffers: useUnalignedBuffers)
        let message = org_apache_arrow_flatbuf_Message.getRootAsMessage(bb: mbb)
        switch message.headerType {
        case .schema:
            let sMessage = message.header(type: org_apache_arrow_flatbuf_Schema.self)!
            switch loadSchema(sMessage) {
            case .success(let schema):
                result.schema = schema
                result.messageSchema = sMessage
                return .success(())
            case .failure(let error):
                return .failure(error)
            }
        case .recordbatch:
            let rbMessage = message.header(type: org_apache_arrow_flatbuf_RecordBatch.self)!
            do {
                let recordBatch = try loadRecordBatch(
                    rbMessage, schema: result.messageSchema!, arrowSchema: result.schema!,
                    data: dataBody, messageEndOffset: 0).get()
                result.batches.append(recordBatch)
                return .success(())
            } catch let error as ArrowError {
                return .failure(error)
            } catch {
                return .failure(.unknownError("Unexpected error: \(error)"))
            }

        default:
            return .failure(.unknownError("Unhandled header type: \(message.headerType)"))
        }
    }

}

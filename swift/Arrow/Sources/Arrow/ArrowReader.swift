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

public class ArrowReader { // swiftlint:disable:this type_body_length
    private class RecordBatchData {
        let schema: org_apache_arrow_flatbuf_Schema
        let recordBatch: org_apache_arrow_flatbuf_RecordBatch
        private var fieldIndex: Int32 = 0
        private var nodeIndex: Int32 = 0
        private var bufferIndex: Int32 = 0
        init(_ recordBatch: org_apache_arrow_flatbuf_RecordBatch,
             schema: org_apache_arrow_flatbuf_Schema) {
            self.recordBatch = recordBatch
            self.schema = schema
        }

        func nextNode() -> org_apache_arrow_flatbuf_FieldNode? {
            if nodeIndex >= self.recordBatch.nodesCount {return nil}
            defer {nodeIndex += 1}
            return self.recordBatch.nodes(at: nodeIndex)
        }

        func nextBuffer() -> org_apache_arrow_flatbuf_Buffer? {
            if bufferIndex >= self.recordBatch.buffersCount {return nil}
            defer {bufferIndex += 1}
            return self.recordBatch.buffers(at: bufferIndex)
        }

        func nextField() -> org_apache_arrow_flatbuf_Field? {
            if fieldIndex >= self.schema.fieldsCount {return nil}
            defer {fieldIndex += 1}
            return self.schema.fields(at: fieldIndex)
        }

        func isDone() -> Bool {
            return nodeIndex >= self.recordBatch.nodesCount
        }
    }

    private struct DataLoadInfo {
        let fileData: Data
        let messageOffset: Int64
        var batchData: RecordBatchData
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

    private func loadStructData(_ loadInfo: DataLoadInfo,
                                field: org_apache_arrow_flatbuf_Field)
    -> Result<ArrowArrayHolder, ArrowError> {
        guard let node = loadInfo.batchData.nextNode() else {
            return .failure(.invalid("Node not found"))
        }

        guard let nullBuffer = loadInfo.batchData.nextBuffer() else {
            return .failure(.invalid("Null buffer not found"))
        }

        let nullLength = UInt(ceil(Double(node.length) / 8))
        let arrowNullBuffer = makeBuffer(nullBuffer, fileData: loadInfo.fileData,
                                         length: nullLength, messageOffset: loadInfo.messageOffset)
        var children = [ArrowData]()
        for index in 0..<field.childrenCount {
            let childField = field.children(at: index)!
            switch loadField(loadInfo, field: childField) {
            case .success(let holder):
                children.append(holder.array.arrowData)
            case .failure(let error):
                return .failure(error)
            }
        }

        return makeArrayHolder(field, buffers: [arrowNullBuffer],
                               nullCount: UInt(node.nullCount), children: children,
                               rbLength: UInt(loadInfo.batchData.recordBatch.length))
    }

    private func loadPrimitiveData(
        _ loadInfo: DataLoadInfo,
        field: org_apache_arrow_flatbuf_Field)
    -> Result<ArrowArrayHolder, ArrowError> {
        guard let node = loadInfo.batchData.nextNode() else {
            return .failure(.invalid("Node not found"))
        }

        guard let nullBuffer = loadInfo.batchData.nextBuffer() else {
            return .failure(.invalid("Null buffer not found"))
        }

        guard let valueBuffer = loadInfo.batchData.nextBuffer() else {
            return .failure(.invalid("Value buffer not found"))
        }

        let nullLength = UInt(ceil(Double(node.length) / 8))
        let arrowNullBuffer = makeBuffer(nullBuffer, fileData: loadInfo.fileData,
                                         length: nullLength, messageOffset: loadInfo.messageOffset)
        let arrowValueBuffer = makeBuffer(valueBuffer, fileData: loadInfo.fileData,
                                          length: UInt(node.length), messageOffset: loadInfo.messageOffset)
        return makeArrayHolder(field, buffers: [arrowNullBuffer, arrowValueBuffer],
                               nullCount: UInt(node.nullCount), children: nil,
                               rbLength: UInt(loadInfo.batchData.recordBatch.length))
    }

    private func loadVariableData(
        _ loadInfo: DataLoadInfo,
        field: org_apache_arrow_flatbuf_Field)
    -> Result<ArrowArrayHolder, ArrowError> {
        guard let node = loadInfo.batchData.nextNode() else {
            return .failure(.invalid("Node not found"))
        }

        guard let nullBuffer = loadInfo.batchData.nextBuffer() else {
            return .failure(.invalid("Null buffer not found"))
        }

        guard let offsetBuffer = loadInfo.batchData.nextBuffer() else {
            return .failure(.invalid("Offset buffer not found"))
        }

        guard let valueBuffer = loadInfo.batchData.nextBuffer() else {
            return .failure(.invalid("Value buffer not found"))
        }

        let nullLength = UInt(ceil(Double(node.length) / 8))
        let arrowNullBuffer = makeBuffer(nullBuffer, fileData: loadInfo.fileData,
                                         length: nullLength, messageOffset: loadInfo.messageOffset)
        let arrowOffsetBuffer = makeBuffer(offsetBuffer, fileData: loadInfo.fileData,
                                           length: UInt(node.length), messageOffset: loadInfo.messageOffset)
        let arrowValueBuffer = makeBuffer(valueBuffer, fileData: loadInfo.fileData,
                                          length: UInt(node.length), messageOffset: loadInfo.messageOffset)
        return makeArrayHolder(field, buffers: [arrowNullBuffer, arrowOffsetBuffer, arrowValueBuffer],
                               nullCount: UInt(node.nullCount), children: nil,
                               rbLength: UInt(loadInfo.batchData.recordBatch.length))
    }

    private func loadField(
        _ loadInfo: DataLoadInfo,
        field: org_apache_arrow_flatbuf_Field)
    -> Result<ArrowArrayHolder, ArrowError> {
        if isNestedType(field.typeType) {
            return loadStructData(loadInfo, field: field)
        } else if isFixedPrimitive(field.typeType) {
            return loadPrimitiveData(loadInfo, field: field)
        } else {
            return loadVariableData(loadInfo, field: field)
        }
    }

    private func loadRecordBatch(
        _ recordBatch: org_apache_arrow_flatbuf_RecordBatch,
        schema: org_apache_arrow_flatbuf_Schema,
        arrowSchema: ArrowSchema,
        data: Data,
        messageEndOffset: Int64
    ) -> Result<RecordBatch, ArrowError> {
        var columns: [ArrowArrayHolder] = []
        let batchData = RecordBatchData(recordBatch, schema: schema)
        let loadInfo = DataLoadInfo(fileData: data,
                                    messageOffset: messageEndOffset,
                                    batchData: batchData)
        while !batchData.isDone() {
            guard let field = batchData.nextField() else {
                return .failure(.invalid("Field not found"))
            }

            let result = loadField(loadInfo, field: field)
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

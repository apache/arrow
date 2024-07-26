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
import FlatBuffers

public protocol DataWriter {
    var count: Int {get}
    func append(_ data: Data)
}

public class ArrowWriter { // swiftlint:disable:this type_body_length
    public class InMemDataWriter: DataWriter {
        public private(set) var data: Data
        public var count: Int { return data.count }
        public init(_ data: Data) {
            self.data = data
        }
        convenience init() {
            self.init(Data())
        }

        public func append(_ data: Data) {
            self.data.append(data)
        }
    }

    public class FileDataWriter: DataWriter {
        private var handle: FileHandle
        private var currentSize: Int = 0
        public var count: Int { return currentSize }
        public init(_ handle: FileHandle) {
            self.handle = handle
        }

        public func append(_ data: Data) {
            self.handle.write(data)
            self.currentSize += data.count
        }
    }

    public class Info {
        public let type: org_apache_arrow_flatbuf_MessageHeader
        public let schema: ArrowSchema
        public let batches: [RecordBatch]
        public init(_ type: org_apache_arrow_flatbuf_MessageHeader, schema: ArrowSchema, batches: [RecordBatch]) {
            self.type = type
            self.schema = schema
            self.batches = batches
        }

        public convenience init(_ type: org_apache_arrow_flatbuf_MessageHeader, schema: ArrowSchema) {
            self.init(type, schema: schema, batches: [RecordBatch]())
        }
    }

    public init() {}

    private func writeField(_ fbb: inout FlatBufferBuilder, field: ArrowField) -> Result<Offset, ArrowError> {
        var fieldsOffset: Offset?
        if let nestedField = field.type as? ArrowNestedType {
            var offsets = [Offset]()
            for field in nestedField.fields {
                switch writeField(&fbb, field: field) {
                case .success(let offset):
                    offsets.append(offset)
                case .failure(let error):
                    return .failure(error)
                }
            }

            fieldsOffset = fbb.createVector(ofOffsets: offsets)
        }

        let nameOffset = fbb.create(string: field.name)
        let fieldTypeOffsetResult = toFBType(&fbb, arrowType: field.type)
        let startOffset = org_apache_arrow_flatbuf_Field.startField(&fbb)
        org_apache_arrow_flatbuf_Field.add(name: nameOffset, &fbb)
        org_apache_arrow_flatbuf_Field.add(nullable: field.isNullable, &fbb)
        if let childrenOffset = fieldsOffset {
            org_apache_arrow_flatbuf_Field.addVectorOf(children: childrenOffset, &fbb)
        }

        switch toFBTypeEnum(field.type) {
        case .success(let type):
            org_apache_arrow_flatbuf_Field.add(typeType: type, &fbb)
        case .failure(let error):
            return .failure(error)
        }

        switch fieldTypeOffsetResult {
        case .success(let offset):
            org_apache_arrow_flatbuf_Field.add(type: offset, &fbb)
            return .success(org_apache_arrow_flatbuf_Field.endField(&fbb, start: startOffset))
        case .failure(let error):
            return .failure(error)
        }
    }

    private func writeSchema(_ fbb: inout FlatBufferBuilder, schema: ArrowSchema) -> Result<Offset, ArrowError> {
        var fieldOffsets = [Offset]()
        for field in schema.fields {
            switch writeField(&fbb, field: field) {
            case .success(let offset):
                fieldOffsets.append(offset)
            case .failure(let error):
                return .failure(error)
            }
        }

        let fieldsOffset: Offset = fbb.createVector(ofOffsets: fieldOffsets)
        let schemaOffset =
            org_apache_arrow_flatbuf_Schema.createSchema(&fbb,
                                                         endianness: .little,
                                                         fieldsVectorOffset: fieldsOffset)
        return .success(schemaOffset)

    }

    private func writeRecordBatches(
        _ writer: inout DataWriter,
        batches: [RecordBatch]
    ) -> Result<[org_apache_arrow_flatbuf_Block], ArrowError> {
        var rbBlocks = [org_apache_arrow_flatbuf_Block]()

        for batch in batches {
            let startIndex = writer.count
            switch writeRecordBatch(batch: batch) {
            case .success(let rbResult):
                withUnsafeBytes(of: CONTINUATIONMARKER.littleEndian) {writer.append(Data($0))}
                withUnsafeBytes(of: rbResult.1.o.littleEndian) {writer.append(Data($0))}
                writer.append(rbResult.0)
                switch writeRecordBatchData(&writer, fields: batch.schema.fields, columns: batch.columns) {
                case .success:
                    rbBlocks.append(
                        org_apache_arrow_flatbuf_Block(offset: Int64(startIndex),
                                                       metaDataLength: Int32(0),
                                                       bodyLength: Int64(rbResult.1.o)))
                case .failure(let error):
                    return .failure(error)
                }
            case .failure(let error):
                return .failure(error)
            }
        }

        return .success(rbBlocks)
    }

    private func writeFieldNodes(_ fields: [ArrowField], columns: [ArrowArrayHolder], offsets: inout [Offset],
                                 fbb: inout FlatBufferBuilder) {
        for index in (0 ..< fields.count).reversed() {
            let column = columns[index]
            let fieldNode =
                org_apache_arrow_flatbuf_FieldNode(length: Int64(column.length),
                                                   nullCount: Int64(column.nullCount))
            offsets.append(fbb.create(struct: fieldNode))
            if let nestedType = column.type as? ArrowNestedType {
                let structArray = column.array as? StructArray
                writeFieldNodes(nestedType.fields, columns: structArray!.arrowFields!, offsets: &offsets, fbb: &fbb)
            }
        }
    }

    private func writeBufferInfo(_ fields: [ArrowField],
                                 columns: [ArrowArrayHolder],
                                 bufferOffset: inout Int,
                                 buffers: inout [org_apache_arrow_flatbuf_Buffer],
                                 fbb: inout FlatBufferBuilder) {
        for index in 0 ..< fields.count {
            let column = columns[index]
            let colBufferDataSizes = column.getBufferDataSizes()
            for var bufferDataSize in colBufferDataSizes {
                bufferDataSize = getPadForAlignment(bufferDataSize)
                let buffer = org_apache_arrow_flatbuf_Buffer(offset: Int64(bufferOffset), length: Int64(bufferDataSize))
                buffers.append(buffer)
                bufferOffset += bufferDataSize
                if let nestedType = column.type as? ArrowNestedType {
                    let structArray = column.array as? StructArray
                    writeBufferInfo(nestedType.fields, columns: structArray!.arrowFields!,
                                    bufferOffset: &bufferOffset, buffers: &buffers, fbb: &fbb)
                }
            }
        }
    }

    private func writeRecordBatch(batch: RecordBatch) -> Result<(Data, Offset), ArrowError> {
        let schema = batch.schema
        var fbb = FlatBufferBuilder()

        // write out field nodes
        var fieldNodeOffsets = [Offset]()
        fbb.startVector(schema.fields.count, elementSize: MemoryLayout<org_apache_arrow_flatbuf_FieldNode>.size)
        writeFieldNodes(schema.fields, columns: batch.columns, offsets: &fieldNodeOffsets, fbb: &fbb)
        let nodeOffset = fbb.endVector(len: fieldNodeOffsets.count)

        // write out buffers
        var buffers = [org_apache_arrow_flatbuf_Buffer]()
        var bufferOffset = Int(0)
        writeBufferInfo(schema.fields, columns: batch.columns,
                        bufferOffset: &bufferOffset, buffers: &buffers,
                        fbb: &fbb)
        org_apache_arrow_flatbuf_RecordBatch.startVectorOfBuffers(batch.schema.fields.count, in: &fbb)
        for buffer in buffers.reversed() {
            fbb.create(struct: buffer)
        }

        let batchBuffersOffset = fbb.endVector(len: buffers.count)
        let startRb = org_apache_arrow_flatbuf_RecordBatch.startRecordBatch(&fbb)
        org_apache_arrow_flatbuf_RecordBatch.addVectorOf(nodes: nodeOffset, &fbb)
        org_apache_arrow_flatbuf_RecordBatch.addVectorOf(buffers: batchBuffersOffset, &fbb)
        org_apache_arrow_flatbuf_RecordBatch.add(length: Int64(batch.length), &fbb)
        let recordBatchOffset = org_apache_arrow_flatbuf_RecordBatch.endRecordBatch(&fbb, start: startRb)
        let bodySize = Int64(bufferOffset)
        let startMessage = org_apache_arrow_flatbuf_Message.startMessage(&fbb)
        org_apache_arrow_flatbuf_Message.add(version: .max, &fbb)
        org_apache_arrow_flatbuf_Message.add(bodyLength: Int64(bodySize), &fbb)
        org_apache_arrow_flatbuf_Message.add(headerType: .recordbatch, &fbb)
        org_apache_arrow_flatbuf_Message.add(header: recordBatchOffset, &fbb)
        let messageOffset = org_apache_arrow_flatbuf_Message.endMessage(&fbb, start: startMessage)
        fbb.finish(offset: messageOffset)
        return .success((fbb.data, Offset(offset: UInt32(fbb.data.count))))
    }

    private func writeRecordBatchData(
        _ writer: inout DataWriter, fields: [ArrowField],
        columns: [ArrowArrayHolder])
    -> Result<Bool, ArrowError> {
        for index in 0 ..< fields.count {
            let column = columns[index]
            let colBufferData = column.getBufferData()
            for var bufferData in colBufferData {
                addPadForAlignment(&bufferData)
                writer.append(bufferData)
                if let nestedType = column.type as? ArrowNestedType {
                    guard let structArray = column.array as? StructArray else {
                        return .failure(.invalid("Struct type array expected for nested type"))
                    }

                    switch writeRecordBatchData(&writer, fields: nestedType.fields, columns: structArray.arrowFields!) {
                    case .success:
                        continue
                    case .failure(let error):
                        return .failure(error)
                    }
                }
            }
        }

        return .success(true)
    }

    private func writeFooter(schema: ArrowSchema,
                             rbBlocks: [org_apache_arrow_flatbuf_Block]
    ) -> Result<Data, ArrowError> {
        var fbb: FlatBufferBuilder = FlatBufferBuilder()
        switch writeSchema(&fbb, schema: schema) {
        case .success(let schemaOffset):
            fbb.startVector(rbBlocks.count, elementSize: MemoryLayout<org_apache_arrow_flatbuf_Block>.size)
            for blkInfo in rbBlocks.reversed() {
                fbb.create(struct: blkInfo)
            }

            let rbBlkEnd = fbb.endVector(len: rbBlocks.count)
            let footerStartOffset = org_apache_arrow_flatbuf_Footer.startFooter(&fbb)
            org_apache_arrow_flatbuf_Footer.add(schema: schemaOffset, &fbb)
            org_apache_arrow_flatbuf_Footer.addVectorOf(recordBatches: rbBlkEnd, &fbb)
            let footerOffset = org_apache_arrow_flatbuf_Footer.endFooter(&fbb, start: footerStartOffset)
            fbb.finish(offset: footerOffset)
            return .success(fbb.data)
        case .failure(let error):
            return .failure(error)
        }
    }

    private func writeFile(_ writer: inout DataWriter, info: ArrowWriter.Info) -> Result<Bool, ArrowError> {
        var fbb: FlatBufferBuilder = FlatBufferBuilder()
        switch writeSchema(&fbb, schema: info.schema) {
        case .success(let schemaOffset):
            fbb.finish(offset: schemaOffset)
            writer.append(fbb.data)
        case .failure(let error):
            return .failure(error)
        }

        switch writeRecordBatches(&writer, batches: info.batches) {
        case .success(let rbBlocks):
            switch writeFooter(schema: info.schema, rbBlocks: rbBlocks) {
            case .success(let footerData):
                fbb.finish(offset: Offset(offset: fbb.buffer.size))
                let footerOffset = writer.count
                writer.append(footerData)
                addPadForAlignment(&writer)

                withUnsafeBytes(of: Int32(0).littleEndian) { writer.append(Data($0)) }
                let footerDiff = (UInt32(writer.count) - UInt32(footerOffset))
                withUnsafeBytes(of: footerDiff.littleEndian) { writer.append(Data($0)) }
            case .failure(let error):
                return .failure(error)
            }
        case .failure(let error):
            return .failure(error)
        }

        return .success(true)
    }

    public func writeStreaming(_ info: ArrowWriter.Info) -> Result<Data, ArrowError> {
        let writer: any DataWriter = InMemDataWriter()
        switch toMessage(info.schema) {
        case .success(let schemaData):
            withUnsafeBytes(of: CONTINUATIONMARKER.littleEndian) {writer.append(Data($0))}
            withUnsafeBytes(of: UInt32(schemaData.count).littleEndian) {writer.append(Data($0))}
            writer.append(schemaData)
        case .failure(let error):
            return .failure(error)
        }

        for batch in info.batches {
            switch toMessage(batch) {
            case .success(let batchData):
                withUnsafeBytes(of: CONTINUATIONMARKER.littleEndian) {writer.append(Data($0))}
                withUnsafeBytes(of: UInt32(batchData[0].count).littleEndian) {writer.append(Data($0))}
                writer.append(batchData[0])
                writer.append(batchData[1])
            case .failure(let error):
                return .failure(error)
            }
        }

        withUnsafeBytes(of: CONTINUATIONMARKER.littleEndian) {writer.append(Data($0))}
        withUnsafeBytes(of: UInt32(0).littleEndian) {writer.append(Data($0))}
        if let memWriter = writer as? InMemDataWriter {
            return .success(memWriter.data)
        } else {
            return .failure(.invalid("Unable to cast writer"))
        }
    }

    public func writeFile(_ info: ArrowWriter.Info) -> Result<Data, ArrowError> {
        var writer: any DataWriter = InMemDataWriter()
        switch writeFile(&writer, info: info) {
        case .success:
            if let memWriter = writer as? InMemDataWriter {
                return .success(memWriter.data)
            } else {
                return .failure(.invalid("Unable to cast writer"))
            }
        case .failure(let error):
            return .failure(error)
        }
    }

    public func toFile(_ fileName: URL, info: ArrowWriter.Info) -> Result<Bool, ArrowError> {
        do {
            try Data().write(to: fileName)
        } catch {
            return .failure(.ioError("\(error)"))
        }

        let fileHandle = FileHandle(forUpdatingAtPath: fileName.path)!
        defer { fileHandle.closeFile() }

        var markerData = FILEMARKER.data(using: .utf8)!
        addPadForAlignment(&markerData)

        var writer: any DataWriter = FileDataWriter(fileHandle)
        writer.append(FILEMARKER.data(using: .utf8)!)
        switch writeFile(&writer, info: info) {
        case .success:
            writer.append(FILEMARKER.data(using: .utf8)!)
        case .failure(let error):
            return .failure(error)
        }

        return .success(true)
    }

    public func toMessage(_ batch: RecordBatch) -> Result<[Data], ArrowError> {
        var writer: any DataWriter = InMemDataWriter()
        switch writeRecordBatch(batch: batch) {
        case .success(let message):
            writer.append(message.0)
            addPadForAlignment(&writer)
            var dataWriter: any DataWriter = InMemDataWriter()
            switch writeRecordBatchData(&dataWriter, fields: batch.schema.fields, columns: batch.columns) {
            case .success:
                return .success([
                    (writer as! InMemDataWriter).data, // swiftlint:disable:this force_cast
                    (dataWriter as! InMemDataWriter).data // swiftlint:disable:this force_cast
                ])
            case .failure(let error):
                return .failure(error)
            }
        case .failure(let error):
            return .failure(error)
        }
    }

    public func toMessage(_ schema: ArrowSchema) -> Result<Data, ArrowError> {
        var schemaSize: Int32 = 0
        var fbb = FlatBufferBuilder()
        switch writeSchema(&fbb, schema: schema) {
        case .success(let schemaOffset):
            schemaSize = Int32(schemaOffset.o)
        case .failure(let error):
            return .failure(error)
        }

        let startMessage = org_apache_arrow_flatbuf_Message.startMessage(&fbb)
        org_apache_arrow_flatbuf_Message.add(bodyLength: Int64(0), &fbb)
        org_apache_arrow_flatbuf_Message.add(headerType: .schema, &fbb)
        org_apache_arrow_flatbuf_Message.add(header: Offset(offset: UOffset(schemaSize)), &fbb)
        org_apache_arrow_flatbuf_Message.add(version: .max, &fbb)
        let messageOffset = org_apache_arrow_flatbuf_Message.endMessage(&fbb, start: startMessage)
        fbb.finish(offset: messageOffset)
        return .success(fbb.data)
    }
}
// swiftlint:disable:this file_length

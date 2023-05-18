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

public class ArrowWriter {
    public class InMemDataWriter : DataWriter {
        public private(set) var data: Data;
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

    public class FileDataWriter : DataWriter {
        private var handle: FileHandle
        private var current_size: Int = 0
        public var count: Int { return current_size }
        public init(_ handle: FileHandle) {
            self.handle = handle
        }
        
        public func append(_ data: Data) {
            self.handle.write(data)
            self.current_size += data.count
        }
    }

    private func writeField(_ fbb: inout FlatBufferBuilder, field: ArrowField) throws -> Offset {
        let nameOffset = fbb.create(string: field.name)
        let fieldTypeOfffset = try toFBType(&fbb, infoType: field.type)
        let startOffset = org_apache_arrow_flatbuf_Field.startField(&fbb)
        org_apache_arrow_flatbuf_Field.add(name: nameOffset, &fbb)
        org_apache_arrow_flatbuf_Field.add(nullable: field.isNullable, &fbb)
        org_apache_arrow_flatbuf_Field.add(typeType: try toFBTypeEnum(field.type), &fbb)
        org_apache_arrow_flatbuf_Field.add(type: fieldTypeOfffset, &fbb)
        return org_apache_arrow_flatbuf_Field.endField(&fbb, start: startOffset)
    }

    private func writeSchema(_ fbb: inout FlatBufferBuilder, schema: ArrowSchema) throws -> Offset {
        var fieldOffsets = [Offset]()
        for field in schema.fields {
            fieldOffsets.append(try writeField(&fbb, field: field))
        }

        let fieldsOffset: Offset = fbb.createVector(ofOffsets: fieldOffsets)
        let schemaOffset = org_apache_arrow_flatbuf_Schema.createSchema(&fbb, endianness: .little, fieldsVectorOffset: fieldsOffset)
        return schemaOffset

    }
    
    private func writeRecordBatches(_ writer: inout DataWriter, batches: [RecordBatch]) throws -> [org_apache_arrow_flatbuf_Block] {
        var rbBlocks = [org_apache_arrow_flatbuf_Block]()

        for batch in batches {
            let startIndex = writer.count
            let rbResult = try writeRecordBatch(batch: batch)
            withUnsafeBytes(of: rbResult.1.o.littleEndian) {writer.append(Data($0))}
            writer.append(rbResult.0)
            try writeRecordBatchData(&writer, batch: batch)
            rbBlocks.append(org_apache_arrow_flatbuf_Block(offset: Int64(startIndex), metaDataLength: Int32(0), bodyLength: Int64(rbResult.1.o)))
        }
                
        return rbBlocks
    }
        
    private func writeRecordBatch(batch: RecordBatch) throws -> (Data, Offset) {
        let schema = batch.schema
        var output = Data()
        var fbb = FlatBufferBuilder()

        // write out field nodes
        var fieldNodeOffsets = [Offset]()
        fbb.startVector(schema.fields.count, elementSize: MemoryLayout<org_apache_arrow_flatbuf_FieldNode>.size)
        for index in (0 ..< schema.fields.count).reversed() {
            let column = batch.column(index)
            let fieldNode = org_apache_arrow_flatbuf_FieldNode(length: Int64(column.length), nullCount: Int64(column.nullCount))
            fieldNodeOffsets.append(fbb.create(struct: fieldNode))
        }
        
        let nodeOffset = fbb.endVector(len: schema.fields.count)

        // write out buffers
        var buffers = [org_apache_arrow_flatbuf_Buffer]()
        var bufferOffset = Int(0)
        for index in 0 ..< batch.schema.fields.count {
            let column = batch.column(index)
            let colBufferDataSizes = try column.getBufferDataSizes()
            for var bufferDataSize in colBufferDataSizes {
                bufferDataSize = getPadForAlignment(bufferDataSize)
                let buffer = org_apache_arrow_flatbuf_Buffer(offset: Int64(bufferOffset), length: Int64(bufferDataSize))
                buffers.append(buffer)
                bufferOffset += bufferDataSize
            }
        }
        
        org_apache_arrow_flatbuf_RecordBatch.startVectorOfBuffers(batch.schema.fields.count, in: &fbb)
        for buffer in buffers.reversed()  {
            fbb.create(struct: buffer)
        }

        let batchBuffersOffset = fbb.endVector(len: buffers.count)
        let startRb = org_apache_arrow_flatbuf_RecordBatch.startRecordBatch(&fbb);
        org_apache_arrow_flatbuf_RecordBatch.addVectorOf(nodes:nodeOffset, &fbb)
        org_apache_arrow_flatbuf_RecordBatch.addVectorOf(buffers:batchBuffersOffset, &fbb)
        let recordBatchOffset = org_apache_arrow_flatbuf_RecordBatch.endRecordBatch(&fbb, start: startRb)
        
        let bodySize = Int64(bufferOffset);
        let startMessage = org_apache_arrow_flatbuf_Message.startMessage(&fbb)
        org_apache_arrow_flatbuf_Message.add(bodyLength: Int64(bodySize), &fbb)
        org_apache_arrow_flatbuf_Message.add(headerType: .recordbatch, &fbb)
        org_apache_arrow_flatbuf_Message.add(header: recordBatchOffset, &fbb)
        let messageOffset = org_apache_arrow_flatbuf_Message.endMessage(&fbb, start: startMessage)
        fbb.finish(offset: messageOffset)
        output.append(fbb.data)
        return (output, Offset(offset: UInt32(output.count)))
    }
    
    private func writeRecordBatchData(_ writer: inout DataWriter, batch: RecordBatch) throws {
        for index in 0 ..< batch.schema.fields.count {
            let column = batch.column(index)
            let colBufferData = try column.getBufferData()
            for var bufferData in colBufferData {
                addPadForAlignment(&bufferData)
                writer.append(bufferData)
            }
        }
    }
    
    private func writeFooter(schema: ArrowSchema, rbBlocks: [org_apache_arrow_flatbuf_Block])  throws -> Data {
        var fbb: FlatBufferBuilder = FlatBufferBuilder()
        let schemaOffset = try writeSchema(&fbb, schema: schema)
        
        let _ = fbb.startVector(rbBlocks.count, elementSize: MemoryLayout<org_apache_arrow_flatbuf_Block>.size)
        for blkInfo in rbBlocks.reversed() {
            fbb.create(struct: blkInfo)
        }
        
        let rbBlkEnd = fbb.endVector(len: rbBlocks.count)
        

        let footerStartOffset = org_apache_arrow_flatbuf_Footer.startFooter(&fbb)
        org_apache_arrow_flatbuf_Footer.add(schema: schemaOffset, &fbb)
        org_apache_arrow_flatbuf_Footer.addVectorOf(recordBatches: rbBlkEnd, &fbb)
        let footerOffset = org_apache_arrow_flatbuf_Footer.endFooter(&fbb, start: footerStartOffset)
        fbb.finish(offset: footerOffset)
        return fbb.data
    }

    private func writeStream(_ writer: inout DataWriter, schema: ArrowSchema, batches: [RecordBatch]) throws {
        var fbb: FlatBufferBuilder = FlatBufferBuilder()
        let schemaOffset = try writeSchema(&fbb, schema: schema)
        fbb.finish(offset: schemaOffset)
        writer.append(fbb.data)
        
        let rbBlocks = try writeRecordBatches(&writer, batches: batches)
        let footerData = try writeFooter(schema: schema, rbBlocks: rbBlocks)
        fbb.finish(offset: Offset(offset: fbb.buffer.size))
        let footerOffset = writer.count
        writer.append(footerData)
        addPadForAlignment(&writer)
        
        withUnsafeBytes(of: Int32(0).littleEndian) { writer.append(Data($0)) }
        let footerDiff = (UInt32(writer.count) - UInt32(footerOffset));
        withUnsafeBytes(of: footerDiff.littleEndian) { writer.append(Data($0)) }
    }

    public func toStream(_ schema: ArrowSchema, batches: [RecordBatch]) throws -> Data {
        var writer: any DataWriter = InMemDataWriter()
        try writeStream(&writer, schema: schema, batches: batches)
        return (writer as! InMemDataWriter).data
    }

    public func toFile(_ fileName: URL, schema: ArrowSchema, batches: [RecordBatch]) throws {
        try Data().write(to: fileName)
        let fileHandle = FileHandle(forUpdatingAtPath: fileName.path)!
        defer { fileHandle.closeFile() }
        
        var markerData = FILEMARKER.data(using: .utf8)!;
        addPadForAlignment(&markerData)

        var writer: any DataWriter = FileDataWriter(fileHandle)
        writer.append(FILEMARKER.data(using: .utf8)!)
        try writeStream(&writer, schema: schema, batches: batches)
        writer.append(FILEMARKER.data(using: .utf8)!)
    }
}

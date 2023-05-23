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

public class ChunkedArrayHolder {
    public let type: ArrowType.Info
    public let length: UInt
    public let nullCount: UInt
    public let holder: Any
    public let getBufferData: () throws -> [Data]
    public let getBufferDataSizes: () throws -> [Int]
    public init<T>(_ chunked: ChunkedArray<T>) {
        self.holder = chunked
        self.length = chunked.length
        self.type = chunked.type
        self.nullCount = chunked.nullCount
        self.getBufferData = {() throws -> [Data] in 
            var bufferData = [Data]()
            var numBuffers = 2;
            if !isFixedPrimitive(try toFBTypeEnum(chunked.type)) {
                numBuffers = 3
            }

            for _ in 0 ..< numBuffers {
                bufferData.append(Data())
            }

            for arrow_data in chunked.arrays {
                for index in 0 ..< numBuffers {
                    arrow_data.arrowData.buffers[index].append(to: &bufferData[index])
                }
            }

            return bufferData;
        }
        
        self.getBufferDataSizes = {() throws -> [Int] in
            var bufferDataSizes = [Int]()
            var numBuffers = 2;
            if !isFixedPrimitive(try toFBTypeEnum(chunked.type)) {
                numBuffers = 3
            }
            for _ in 0 ..< numBuffers {
                bufferDataSizes.append(Int(0))
            }
            
            for arrow_data in chunked.arrays {
                for index in 0 ..< numBuffers {
                    bufferDataSizes[index] += Int(arrow_data.arrowData.buffers[index].capacity);
                }
            }

            return bufferDataSizes;
        }

    }
}

public class ArrowColumn {
    public let field: ArrowField
    fileprivate let dataHolder: ChunkedArrayHolder
    public var type: ArrowType.Info {get{return self.dataHolder.type}}
    public var length: UInt {get{return self.dataHolder.length}}
    public var nullCount: UInt {get{return self.dataHolder.nullCount}}

    public func data<T>() -> ChunkedArray<T> {
        return (self.dataHolder.holder as! ChunkedArray<T>)
    }

    public var name: String {get{return field.name}}
    public init<T>(_ field: ArrowField, chunked: ChunkedArray<T>) {
        self.field = field
        self.dataHolder = ChunkedArrayHolder(chunked)
    }
}

public class ArrowTable {
    public let schema: ArrowSchema
    public var columnCount: UInt {get{return UInt(self.columns.count)}}
    public let rowCount: UInt
    public let columns: [ArrowColumn]
    init(_ schema: ArrowSchema, columns: [ArrowColumn]) {
        self.schema = schema
        self.columns = columns
        self.rowCount = columns[0].length
    }
    
    public func toRecordBatch() -> RecordBatch {
        var rbColumns = [ChunkedArrayHolder]()
        for column in self.columns {
            rbColumns.append(column.dataHolder)
        }
        
        return RecordBatch(schema, columns: rbColumns)
    }
    
    public class Builder {
        let schemaBuilder = ArrowSchema.Builder()
        var columns = [ArrowColumn]()
        
        public func addColumn<T>(_ fieldName: String, arrowArray: ArrowArray<T>) throws -> Builder {
            return self.addColumn(fieldName, chunked: try ChunkedArray([arrowArray]))
        }

        public func addColumn<T>(_ fieldName: String, chunked: ChunkedArray<T>) -> Builder {
            let field = ArrowField(fieldName, type: chunked.type, isNullable: chunked.nullCount != 0)
            let _ = self.schemaBuilder.addField(field)
            self.columns.append(ArrowColumn(field, chunked: chunked))
            return self
        }

        public func addColumn<T>(_ field: ArrowField, arrowArray: ArrowArray<T>) throws -> Builder {
            let _ = self.schemaBuilder.addField(field)
            self.columns.append(ArrowColumn(field, chunked: try ChunkedArray([arrowArray])))
            return self
        }

        public func addColumn<T>(_ field: ArrowField, chunked: ChunkedArray<T>) -> Builder {
            let _ = self.schemaBuilder.addField(field)
            self.columns.append(ArrowColumn(field, chunked: chunked))
            return self
        }

        public func addColumn(_ column: ArrowColumn) -> Builder {
            self.columns.append(column)
            return self
        }
        
        public func finish() -> ArrowTable {
            return ArrowTable(self.schemaBuilder.finish(), columns: self.columns)
        }
    }
}

public class RecordBatch {
    let schema: ArrowSchema
    var columnCount: UInt {get{return UInt(self.columns.count)}}
    let columns: [ChunkedArrayHolder]
    let length: UInt
    public init(_ schema: ArrowSchema, columns: [ChunkedArrayHolder]) {
        self.schema = schema
        self.columns = columns
        self.length = columns[0].length
    }
    
    public class Builder {
        let schemaBuilder = ArrowSchema.Builder()
        var columns = [ChunkedArrayHolder]()
        
        public func addColumn(_ fieldName: String, chunked: ChunkedArrayHolder) -> Builder {
            let field = ArrowField(fieldName, type: chunked.type, isNullable: chunked.nullCount != 0)
            let _ = self.schemaBuilder.addField(field)
            self.columns.append(chunked)
            return self
        }

        public func addColumn(_ field: ArrowField, chunked: ChunkedArrayHolder) -> Builder {
            let _ = self.schemaBuilder.addField(field)
            self.columns.append(chunked)
            return self
        }

        public func finish() -> RecordBatch {
            return RecordBatch(self.schemaBuilder.finish(), columns: self.columns)
        }
    }
    
    public func data<T>(for columnIndex: Int) -> ChunkedArray<T> {
        let arrayHolder = column(columnIndex)
        return (arrayHolder.holder as! ChunkedArray<T>)
    }
    
    public func column(_ index: Int) -> ChunkedArrayHolder {
        return self.columns[index]
    }

    public func column(_ name: String) -> ChunkedArrayHolder? {
        if let index = self.schema.fieldIndex(name) {
            return self.columns[index]
        }
        
        return nil;
    }


}

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
            
    public init<T>(_ chunked: ChunkedArray<T>) {
        self.holder = chunked
        self.length = chunked.length
        self.type = chunked.type
        self.nullCount = chunked.nullCount
    }
}

public class ArrowColumn {
    public let field: ArrowField
    private let dataHolder: ChunkedArrayHolder
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
    
    public class Builder {
        let schemaBuilder = ArrowSchema.Builder()
        var columns = [ArrowColumn]()
        
        public func addColumn<T>(_ fieldName: String, arrowArray: ArrowArray<T>) throws -> Builder {
            return self.addColumn(fieldName, chunked: try ChunkedArray([arrowArray]))
        }

        public func addColumn<T>(_ fieldName: String, chunked: ChunkedArray<T>) -> Builder {
            let field = ArrowField(fieldName, type: chunked.type, isNullable: chunked.nullCount != 0)
            self.schemaBuilder.addField(field)
            self.columns.append(ArrowColumn(field, chunked: chunked))
            return self
        }

        public func addColumn<T>(_ field: ArrowField, arrowArray: ArrowArray<T>) throws -> Builder {
            self.schemaBuilder.addField(field)
            self.columns.append(ArrowColumn(field, chunked: try ChunkedArray([arrowArray])))
            return self
        }

        public func addColumn<T>(_ field: ArrowField, chunked: ChunkedArray<T>) -> Builder {
            self.schemaBuilder.addField(field)
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

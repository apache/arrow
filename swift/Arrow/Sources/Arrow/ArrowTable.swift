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
    let type: ArrowType.Info
    let length: UInt
    let nullCount: UInt
    let holder: Any
            
    init<T>(_ chunked: ChunkedArray<T>) {
        self.holder = chunked
        self.length = chunked.length
        self.type = chunked.type
        self.nullCount = chunked.nullCount
    }
}

public class ArrowColumn {
    let field: ArrowField
    private let dataHolder: ChunkedArrayHolder
    var type: ArrowType.Info {get{return self.dataHolder.type}}
    var length: UInt {get{return self.dataHolder.length}}
    var nullCount: UInt {get{return self.dataHolder.nullCount}}

    func data<T>() -> ChunkedArray<T> {
        return (self.dataHolder.holder as! ChunkedArray<T>)
    }

    var name: String {get{return field.name}}
    init<T>(_ field: ArrowField, chunked: ChunkedArray<T>) {
        self.field = field
        self.dataHolder = ChunkedArrayHolder(chunked)
    }
}

public class ArrowTable {
    let schema: ArrowSchema
    var columnCount: UInt {get{return UInt(self.columns.count)}}
    let rowCount: UInt
    let columns: [ArrowColumn]
    init(_ schema: ArrowSchema, columns: [ArrowColumn]) {
        self.schema = schema
        self.columns = columns
        self.rowCount = columns[0].length
    }
    
    public class Builder {
        let schemaBuilder = ArrowSchema.Builder()
        var columns = [ArrowColumn]()
        
        func addColumn<T>(_ fieldName: String, arrowArray: ArrowArray<T>) throws -> Builder {
            return self.addColumn(fieldName, chunked: try ChunkedArray([arrowArray]))
        }

        func addColumn<T>(_ fieldName: String, chunked: ChunkedArray<T>) -> Builder {
            let field = ArrowField(fieldName, type: chunked.type, isNullable: chunked.nullCount != 0)
            self.schemaBuilder.addField(field)
            self.columns.append(ArrowColumn(field, chunked: chunked))
            return self
        }

        func addColumn<T>(_ field: ArrowField, arrowArray: ArrowArray<T>) throws -> Builder {
            self.schemaBuilder.addField(field)
            self.columns.append(ArrowColumn(field, chunked: try ChunkedArray([arrowArray])))
            return self
        }

        func addColumn<T>(_ field: ArrowField, chunked: ChunkedArray<T>) -> Builder {
            self.schemaBuilder.addField(field)
            self.columns.append(ArrowColumn(field, chunked: chunked))
            return self
        }

        func addColumn(_ column: ArrowColumn) -> Builder {
            self.columns.append(column)
            return self
        }
        
        func finish() -> ArrowTable {
            return ArrowTable(self.schemaBuilder.finish(), columns: self.columns)
        }
    }
}

public class RecordBatch {
    let schema: ArrowSchema
    var columnCount: UInt {get{return UInt(self.columns.count)}}
    let columns: [ChunkedArrayHolder]
    let length: UInt
    init(_ schema: ArrowSchema, columns: [ChunkedArrayHolder]) {
        self.schema = schema
        self.columns = columns
        self.length = columns[0].length
    }
    
    func column(_ index: Int) -> ChunkedArrayHolder {
        return self.columns[index]
    }

    func column(_ name: String) -> ChunkedArrayHolder? {
        if let index = self.schema.fieldIndex(name) {
            return self.columns[index]
        }
        
        return nil;
    }


}

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
    public init(_ field: ArrowField, chunked: ChunkedArrayHolder) {
        self.field = field
        self.dataHolder = chunked
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
    
    public static func from(recordBatches: [RecordBatch]) -> Result<ArrowTable, ArrowError> {
        if(recordBatches.isEmpty) {
            return .failure(.arrayHasNoElements)
        }
        
        var holders = [[ArrowArrayHolder]]()
        let schema = recordBatches[0].schema;
        for recordBatch in recordBatches {
            for index in 0..<schema.fields.count {
                if holders.count <= index {
                    holders.append([ArrowArrayHolder]())
                }
                holders[index].append(recordBatch.columns[index])
            }
        }
        
        let builder = ArrowTable.Builder()
        for index in 0..<schema.fields.count {
            switch ArrowArrayHolder.makeArrowColumn(schema.fields[index], holders: holders[index]) {
            case .success(let column):
                builder.addColumn(column)
            case .failure(let error):
                return .failure(error)
            }
        }
        
        return .success(builder.finish())
    }
    
    public class Builder {
        let schemaBuilder = ArrowSchema.Builder()
        var columns = [ArrowColumn]()
        
        @discardableResult
        public func addColumn<T>(_ fieldName: String, arrowArray: ArrowArray<T>) throws -> Builder {
            return self.addColumn(fieldName, chunked: try ChunkedArray([arrowArray]))
        }

        @discardableResult
        public func addColumn<T>(_ fieldName: String, chunked: ChunkedArray<T>) -> Builder {
            let field = ArrowField(fieldName, type: chunked.type, isNullable: chunked.nullCount != 0)
            self.schemaBuilder.addField(field)
            self.columns.append(ArrowColumn(field, chunked: ChunkedArrayHolder(chunked)))
            return self
        }

        @discardableResult
        public func addColumn<T>(_ field: ArrowField, arrowArray: ArrowArray<T>) throws -> Builder {
            self.schemaBuilder.addField(field)
            let holder = ChunkedArrayHolder(try ChunkedArray([arrowArray]))
            self.columns.append(ArrowColumn(field, chunked: holder))
            return self
        }

        @discardableResult
        public func addColumn<T>(_ field: ArrowField, chunked: ChunkedArray<T>) -> Builder {
            self.schemaBuilder.addField(field)
            self.columns.append(ArrowColumn(field, chunked: ChunkedArrayHolder(chunked)))
            return self
        }

        @discardableResult
        public func addColumn(_ column: ArrowColumn) -> Builder {
            self.schemaBuilder.addField(column.field)
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
    let columns: [ArrowArrayHolder]
    let length: UInt
    public init(_ schema: ArrowSchema, columns: [ArrowArrayHolder]) {
        self.schema = schema
        self.columns = columns
        self.length = columns[0].length
    }
    
    public class Builder {
        let schemaBuilder = ArrowSchema.Builder()
        var columns = [ArrowArrayHolder]()
        
        @discardableResult
        public func addColumn(_ fieldName: String, arrowArray: ArrowArrayHolder) -> Builder {
            let field = ArrowField(fieldName, type: arrowArray.type, isNullable: arrowArray.nullCount != 0)
            self.schemaBuilder.addField(field)
            self.columns.append(arrowArray)
            return self
        }

        @discardableResult
        public func addColumn(_ field: ArrowField, arrowArray: ArrowArrayHolder) -> Builder {
            self.schemaBuilder.addField(field)
            self.columns.append(arrowArray)
            return self
        }

        public func finish() -> Result<RecordBatch, ArrowError> {
            if columns.count > 0 {
                let columnLength = columns[0].length
                for column in columns {
                    if column.length != columnLength {
                        return .failure(.runtimeError("Columns have different sizes"))
                    }
                }
            }
            return .success(RecordBatch(self.schemaBuilder.finish(), columns: self.columns))
        }
    }
    
    public func data<T>(for columnIndex: Int) -> ArrowArray<T> {
        let arrayHolder = column(columnIndex)
        return (arrayHolder.array as! ArrowArray<T>)
    }
    
    public func column(_ index: Int) -> ArrowArrayHolder {
        return self.columns[index]
    }

    public func column(_ name: String) -> ArrowArrayHolder? {
        if let index = self.schema.fieldIndex(name) {
            return self.columns[index]
        }
        
        return nil;
    }


}

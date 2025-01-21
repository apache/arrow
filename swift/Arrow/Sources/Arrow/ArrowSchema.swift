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
public class ArrowField {
    public let type: ArrowType
    public let name: String
    public let isNullable: Bool

    init(_ name: String, type: ArrowType, isNullable: Bool) {
        self.name = name
        self.type = type
        self.isNullable = isNullable
    }
}

public class ArrowSchema {
    public let fields: [ArrowField]
    public let fieldLookup: [String: Int]
    init(_ fields: [ArrowField]) {
        var fieldLookup = [String: Int]()
        for (index, field) in fields.enumerated() {
            fieldLookup[field.name] = index
        }

        self.fields = fields
        self.fieldLookup = fieldLookup
    }

    public func field(_ index: Int) -> ArrowField {
        return self.fields[index]
    }

    public func fieldIndex(_ name: String) -> Int? {
        return self.fieldLookup[name]
    }

    public class Builder {
        private var fields: [ArrowField] = []

        public init() {}

        @discardableResult
        public func addField(_ field: ArrowField) -> Builder {
            fields.append(field)
            return self
        }

        @discardableResult
        public func addField(_ name: String, type: ArrowType, isNullable: Bool) -> Builder {
            fields.append(ArrowField(name, type: type, isNullable: isNullable))
            return self
        }

        public func finish() -> ArrowSchema {
            return ArrowSchema(fields)
        }
    }
}

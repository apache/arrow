/* istanbul ignore file */

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

import { Data } from '../data';
import { Visitor } from '../visitor';
import { Vector } from '../interfaces';
import { Type, TimeUnit } from '../enum';
import { Schema, Field } from '../schema';
import {
    DataType, Dictionary,
    Float, Int, Date_, Interval, Time, Timestamp,
    Bool, Null, Utf8, Binary, Decimal, FixedSizeBinary,
    List, FixedSizeList, Map_, Struct, Union,
} from '../type';

/** @ignore */ const sum = (x: number, y: number) => x + y;
/** @ignore */ const variableWidthColumnErrorMessage = (type: DataType) => `Cannot compute the byte width of variable-width column ${type}`;

export interface ByteWidthVisitor extends Visitor {
    visit<T extends DataType>(node: T): number;
    visitMany<T extends DataType>(nodes: T[]): number[];
    getVisitFn<T extends Type>    (node: T): (type: DataType<T>) => number;
    getVisitFn<T extends DataType>(node: Vector<T> | Data<T> | T): (type: T) => number;
}

export class ByteWidthVisitor extends Visitor {
    public visitNull            (____: Null            ) { return 0; }
    public visitInt             (type: Int             ) { return type.bitWidth / 8; }
    public visitFloat           (type: Float           ) { return type.ArrayType.BYTES_PER_ELEMENT; }
    public visitBinary          (type: Binary          ) { throw new Error(variableWidthColumnErrorMessage(type)); }
    public visitUtf8            (type: Utf8            ) { throw new Error(variableWidthColumnErrorMessage(type)); }
    public visitBool            (____: Bool            ) { return 1 / 8; }
    public visitDecimal         (____: Decimal         ) { return 16; }
    public visitDate            (type: Date_           ) { return (type.unit + 1) * 4; }
    public visitTime            (type: Time            ) { return type.bitWidth / 8; }
    public visitTimestamp       (type: Timestamp       ) { return type.unit === TimeUnit.SECOND ? 4 : 8; }
    public visitInterval        (type: Interval        ) { return (type.unit + 1) * 4; }
    public visitList            (type: List            ) { throw new Error(variableWidthColumnErrorMessage(type)); }
    public visitStruct          (type: Struct          ) { return this.visitFields(type.children).reduce(sum, 0); }
    public visitUnion           (type: Union           ) { return this.visitFields(type.children).reduce(sum, 0); }
    public visitFixedSizeBinary (type: FixedSizeBinary ) { return type.byteWidth; }
    public visitFixedSizeList   (type: FixedSizeList   ) { return type.listSize * this.visitFields(type.children).reduce(sum, 0); }
    public visitMap             (type: Map_            ) { return this.visitFields(type.children).reduce(sum, 0); }
    public visitDictionary      (type: Dictionary      ) { return this.visit(type.indices); }
    public visitFields          (fields: Field[]       ) { return (fields || []).map((field) => this.visit(field.type)); }
    public visitSchema          (schema: Schema        ) { return this.visitFields(schema.fields).reduce(sum, 0); }
}

/** @ignore */
export const instance = new ByteWidthVisitor();

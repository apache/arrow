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

/* eslint-disable unicorn/no-array-callback-reference */

import { Data } from '../data.js';
import { Visitor } from '../visitor.js';
import { TypeToDataType } from '../interfaces.js';
import { Type, TimeUnit, UnionMode } from '../enum.js';
import {
    DataType, Dictionary,
    Float, Int, Date_, Interval, Time, Timestamp,
    Bool, Null, Utf8, Binary, Decimal, FixedSizeBinary,
    List, FixedSizeList, Map_, Struct, Union, DenseUnion, SparseUnion,
} from '../type.js';

/** @ignore */ const sum = (x: number, y: number) => x + y;

/** @ignore */
export interface GetByteLengthVisitor extends Visitor {
    visit<T extends DataType>(node: Data<T>, index: number): number;
    visitMany<T extends DataType>(nodes: Data<T>[], index: number[]): number[];
    getVisitFn<T extends DataType>(node: Data<T> | T): (data: Data<T>, index: number) => number;
    getVisitFn<T extends Type>(node: T): (data: Data<TypeToDataType<T>>, index: number) => number;
    visitBinary<T extends Binary>(data: Data<T>, index: number): number;
    visitUtf8<T extends Utf8>(data: Data<T>, index: number): number;
    visitList<T extends List>(data: Data<T>, index: number): number;
    visitDenseUnion<T extends DenseUnion>(data: Data<T>, index: number): number;
    visitSparseUnion<T extends SparseUnion>(data: Data<T>, index: number): number;
    visitFixedSizeList<T extends FixedSizeList>(data: Data<T>, index: number): number;
}

/** @ignore */
export class GetByteLengthVisitor extends Visitor {
    public visitNull(____: Data<Null>, _: number) {
        return 0;
    }
    public visitInt(data: Data<Int>, _: number) {
        return data.type.bitWidth / 8;
    }
    public visitFloat(data: Data<Float>, _: number) {
        return data.type.ArrayType.BYTES_PER_ELEMENT;
    }
    public visitBool(____: Data<Bool>, _: number) {
        return 1 / 8;
    }
    public visitDecimal(data: Data<Decimal>, _: number) {
        return data.type.bitWidth / 8;
    }
    public visitDate(data: Data<Date_>, _: number) {
        return (data.type.unit + 1) * 4;
    }
    public visitTime(data: Data<Time>, _: number) {
        return data.type.bitWidth / 8;
    }
    public visitTimestamp(data: Data<Timestamp>, _: number) {
        return data.type.unit === TimeUnit.SECOND ? 4 : 8;
    }
    public visitInterval(data: Data<Interval>, _: number) {
        return (data.type.unit + 1) * 4;
    }
    public visitStruct(data: Data<Struct>, i: number) {
        return data.children.reduce((total, child) => total + instance.visit(child, i), 0);
    }
    public visitFixedSizeBinary(data: Data<FixedSizeBinary>, _: number) {
        return data.type.byteWidth;
    }
    public visitMap(data: Data<Map_>, i: number) {
        // 4 + 4 for the indices
        return 8 + data.children.reduce((total, child) => total + instance.visit(child, i), 0);
    }
    public visitDictionary(data: Data<Dictionary>, i: number) {
        return (data.type.indices.bitWidth / 8) + (data.dictionary?.getByteLength(data.values[i]) || 0);
    }
}

/** @ignore */
const getUtf8ByteLength = <T extends Utf8>({ valueOffsets }: Data<T>, index: number): number => {
    // 4 + 4 for the indices, `end - start` for the data bytes
    return 8 + (valueOffsets[index + 1] - valueOffsets[index]);
};

/** @ignore */
const getBinaryByteLength = <T extends Binary>({ valueOffsets }: Data<T>, index: number): number => {
    // 4 + 4 for the indices, `end - start` for the data bytes
    return 8 + (valueOffsets[index + 1] - valueOffsets[index]);
};

/** @ignore */
const getListByteLength = <T extends List>({ valueOffsets, stride, children }: Data<T>, index: number): number => {
    const child: Data<T['valueType']> = children[0];
    const { [index * stride]: start } = valueOffsets;
    const { [index * stride + 1]: end } = valueOffsets;
    const visit = instance.getVisitFn(child.type);
    const slice = child.slice(start, end - start);
    let size = 8; // 4 + 4 for the indices
    for (let idx = -1, len = end - start; ++idx < len;) {
        size += visit(slice, idx);
    }
    return size;
};

/** @ignore */
const getFixedSizeListByteLength = <T extends FixedSizeList>({ stride, children }: Data<T>, index: number): number => {
    const child: Data<T['valueType']> = children[0];
    const slice = child.slice(index * stride, stride);
    const visit = instance.getVisitFn(child.type);
    let size = 0;
    for (let idx = -1, len = slice.length; ++idx < len;) {
        size += visit(slice, idx);
    }
    return size;
};

/* istanbul ignore next */
/** @ignore */
const getUnionByteLength = <
    D extends Data<Union> | Data<DenseUnion> | Data<SparseUnion>
>(data: D, index: number): number => {
    return data.type.mode === UnionMode.Dense ?
        getDenseUnionByteLength(data as Data<DenseUnion>, index) :
        getSparseUnionByteLength(data as Data<SparseUnion>, index);
};

/** @ignore */
const getDenseUnionByteLength = <T extends DenseUnion>({ type, children, typeIds, valueOffsets }: Data<T>, index: number): number => {
    const childIndex = type.typeIdToChildIndex[typeIds[index]];
    // 4 for the typeId, 4 for the valueOffsets, then the child at the offset
    return 8 + instance.visit(children[childIndex], valueOffsets[index]);
};

/** @ignore */
const getSparseUnionByteLength = <T extends SparseUnion>({ children }: Data<T>, index: number): number => {
    // 4 for the typeId, then once each for the children at this index
    return 4 + instance.visitMany(children, children.map(() => index)).reduce(sum, 0);
};

GetByteLengthVisitor.prototype.visitUtf8 = getUtf8ByteLength;
GetByteLengthVisitor.prototype.visitBinary = getBinaryByteLength;
GetByteLengthVisitor.prototype.visitList = getListByteLength;
GetByteLengthVisitor.prototype.visitFixedSizeList = getFixedSizeListByteLength;
GetByteLengthVisitor.prototype.visitUnion = getUnionByteLength;
GetByteLengthVisitor.prototype.visitDenseUnion = getDenseUnionByteLength;
GetByteLengthVisitor.prototype.visitSparseUnion = getSparseUnionByteLength;

/** @ignore */
export const instance = new GetByteLengthVisitor();

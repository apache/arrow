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

import {
    Data,
    ValueOffsetsBuffer,
    DataBuffer,
    NullBuffer,
    TypeIdsBuffer,
} from '../data';
import { Type } from '../enum';
import { Vector } from '../vector';
import { Visitor } from '../visitor';
import { TypeToDataType } from '../interfaces';
import { toArrayBufferView, toInt32Array, toUint8Array } from '../util/buffer';
import {
    DataType, Dictionary,
    Bool, Null, Utf8, Binary, Decimal, FixedSizeBinary, List, FixedSizeList, Map_, Struct,
    Float,
    Int,
    Date_,
    Interval,
    Time,
    Timestamp,
    Union, DenseUnion, SparseUnion,
    strideForType,
} from '../type';

/** @ignore */ export type DataProps<T extends DataType> = {
    type: T,
    offset?: number,
    length?: number,
    nullCount?: number,
    valueOffsets?: ValueOffsetsBuffer,
    data?: DataBuffer<T>,
    nullBitmap?: NullBuffer,
    typeIds?: TypeIdsBuffer,
    children?: Data[],
    child?: Data,
    dictionary?: Vector,
};
/** @ignore */ export type NullDataProps<T extends Null> = {type: T, offset?: number, length?: number};
/** @ignore */ export type IntDataProps<T extends Int> = {type: T, offset?: number, length?: number, nullCount?: number, nullBitmap?: NullBuffer, data?: DataBuffer<T>};
/** @ignore */ export type DictionaryDataProps<T extends Dictionary> = {type: T, offset?: number, length?: number, nullCount?: number, nullBitmap?: NullBuffer, data?: DataBuffer<T>, dictionary: Vector<T['dictionary']>};
/** @ignore */ export type FloatDataProps<T extends Float> = {type: T, offset?: number, length?: number, nullCount?: number, nullBitmap?: NullBuffer, data?: DataBuffer<T>};
/** @ignore */ export type BoolDataProps<T extends Bool> = {type: T, offset?: number, length?: number, nullCount?: number, nullBitmap?: NullBuffer, data?: DataBuffer<T>};
/** @ignore */ export type DecimalDataProps<T extends Decimal> = {type: T, offset?: number, length?: number, nullCount?: number, nullBitmap?: NullBuffer, data?: DataBuffer<T>};
/** @ignore */ export type DateDataProps<T extends Date_> = {type: T, offset?: number, length?: number, nullCount?: number, nullBitmap?: NullBuffer, data?: DataBuffer<T>};
/** @ignore */ export type TimeDataProps<T extends Time> = {type: T, offset?: number, length?: number, nullCount?: number, nullBitmap?: NullBuffer, data?: DataBuffer<T>};
/** @ignore */ export type TimestampDataProps<T extends Timestamp> = {type: T, offset?: number, length?: number, nullCount?: number, nullBitmap?: NullBuffer, data?: DataBuffer<T>};
/** @ignore */ export type IntervalDataProps<T extends Interval> = {type: T, offset?: number, length?: number, nullCount?: number, nullBitmap?: NullBuffer, data?: DataBuffer<T>};
/** @ignore */ export type FixedSizeBinaryDataProps<T extends FixedSizeBinary> = {type: T, offset?: number, length?: number, nullCount?: number, nullBitmap?: NullBuffer, data?: DataBuffer<T>};
/** @ignore */ export type BinaryDataProps<T extends Binary> = {type: T, offset?: number, length?: number, nullCount?: number, nullBitmap?: NullBuffer, valueOffsets: ValueOffsetsBuffer, data?: DataBuffer<T>};
/** @ignore */ export type Utf8DataProps<T extends Utf8> = {type: T, offset?: number, length?: number, nullCount?: number, nullBitmap?: NullBuffer, valueOffsets: ValueOffsetsBuffer, data?: DataBuffer<T>};
/** @ignore */ export type ListDataProps<T extends List> = {type: T, offset?: number, length?: number, nullCount?: number, nullBitmap?: NullBuffer, valueOffsets: ValueOffsetsBuffer, child: Data<T['valueType']>};
/** @ignore */ export type FixedSizeListDataProps<T extends FixedSizeList> = {type: T, offset?: number, length?: number, nullCount?: number, nullBitmap?: NullBuffer, child: Data<T['valueType']>};
/** @ignore */ export type StructDataProps<T extends Struct> = {type: T, offset?: number, length?: number, nullCount?: number, nullBitmap?: NullBuffer, children: Data[]};
/** @ignore */ export type MapDataProps<T extends Map_> = {type: T, offset?: number, length?: number, nullCount?: number, nullBitmap?: NullBuffer, valueOffsets: ValueOffsetsBuffer, child: Data};
/** @ignore */ export type SparseUnionDataProps<T extends SparseUnion> = {type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, typeIds: TypeIdsBuffer, children: Data[]};
/** @ignore */ export type DenseUnionDataProps<T extends DenseUnion> = {type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, typeIds: TypeIdsBuffer, children: Data[], valueOffsets: ValueOffsetsBuffer};
/** @ignore */ export type UnionDataProps<T extends Union> = {type: T, offset?: number, length?: number, nullCount?: number, nullBitmap?: NullBuffer, typeIds: TypeIdsBuffer, children: Data[], valueOffsets?: ValueOffsetsBuffer};

//
// Visitor to make Data instances for each of the Arrow types from component properties and buffers
//

/** @ignore */
export interface MakeDataVisitor extends Visitor {
    visit<T extends DataType>(props: any): Data<T>;
    visitMany<T extends DataType>(props: any[]): Data<T>[];
    getVisitFn<T extends DataType>(node: T): (node: any) => Data<T>;
    getVisitFn<T extends Type>(node: TypeToDataType<T>): (node: any) => Data<TypeToDataType<T>>;

    visitNull<T extends Null>(props: NullDataProps<T>): Data<T>;
    visitBool<T extends Bool>(props: BoolDataProps<T>): Data<T>;
    visitInt<T extends Int>(props: IntDataProps<T>): Data<T>;
    visitFloat<T extends Float>(props: FloatDataProps<T>): Data<T>;
    visitUtf8<T extends Utf8>(props: Utf8DataProps<T>): Data<T>;
    visitBinary<T extends Binary>(props: BinaryDataProps<T>): Data<T>;
    visitFixedSizeBinary<T extends FixedSizeBinary>(props: FixedSizeBinaryDataProps<T>): Data<T>;
    visitDate<T extends Date_>(props: DateDataProps<T>): Data<T>;
    visitTimestamp<T extends Timestamp>(props: TimestampDataProps<T>): Data<T>;
    visitTime<T extends Time>(props: TimeDataProps<T>): Data<T>;
    visitDecimal<T extends Decimal>(props: DecimalDataProps<T>): Data<T>;
    visitList<T extends List>(props: ListDataProps<T>): Data<T>;
    visitStruct<T extends Struct>(props: StructDataProps<T>): Data<T>;
    visitUnion<T extends Union>(props: UnionDataProps<T>): Data<T>;
    visitDictionary<T extends Dictionary>(props: DictionaryDataProps<T>): Data<T>;
    visitInterval<T extends Interval>(props: IntervalDataProps<T>): Data<T>;
    visitFixedSizeList<T extends FixedSizeList>(props: FixedSizeListDataProps<T>): Data<T>;
    visitMap<T extends Map_>(props: MapDataProps<T>): Data<T>;
}

/** @ignore */
export class MakeDataVisitor extends Visitor {
    visit<T extends DataType>(props: any): Data<T> {
        return this.getVisitFn(props.type).call(this, props);
    }
}

MakeDataVisitor.prototype.visitBinary = visitBinary;
MakeDataVisitor.prototype.visitBool = visitBool;
MakeDataVisitor.prototype.visitDate = visitDate;
MakeDataVisitor.prototype.visitDecimal = visitDecimal;
MakeDataVisitor.prototype.visitDictionary = visitDictionary;
MakeDataVisitor.prototype.visitFixedSizeBinary = visitFixedSizeBinary;
MakeDataVisitor.prototype.visitFixedSizeList = visitFixedSizeList;
MakeDataVisitor.prototype.visitFloat = visitFloat;
MakeDataVisitor.prototype.visitInterval = visitInterval;
MakeDataVisitor.prototype.visitInt = visitInt;
MakeDataVisitor.prototype.visitList = visitList;
MakeDataVisitor.prototype.visitMap = visitMap;
MakeDataVisitor.prototype.visitNull = visitNull;
MakeDataVisitor.prototype.visitStruct = visitStruct;
MakeDataVisitor.prototype.visitTimestamp = visitTimestamp;
MakeDataVisitor.prototype.visitTime = visitTime;
MakeDataVisitor.prototype.visitUnion = visitUnion;
MakeDataVisitor.prototype.visitUtf8 = visitUtf8;

export const instance = new MakeDataVisitor();

export function visitNull<T extends Null>(props: NullDataProps<T>) {
    const {
        type,
        offset = 0,
        length = 0,
    } = props;
    return new Data(type, offset, length, 0);
}

export function visitBool<T extends Bool>(props: BoolDataProps<T>) {
    const { type, offset = 0 } = props;
    const nullBitmap = toUint8Array(props.nullBitmap);
    const data = toArrayBufferView(type.ArrayType, props.data);
    const { length = data.length >> 3, nullCount = props.nullBitmap ? -1 : 0, } = props;
    return new Data(type, offset, length, nullCount, [undefined, data, nullBitmap]);
}

export function visitInt<T extends Int>(props: IntDataProps<T>) {
    const { type, offset = 0 } = props;
    const nullBitmap = toUint8Array(props.nullBitmap);
    const data = toArrayBufferView(type.ArrayType, props.data);
    const { length = data.length, nullCount = props.nullBitmap ? -1 : 0, } = props;
    return new Data(type, offset, length, nullCount, [undefined, data, nullBitmap]);
}

export function visitFloat<T extends Float>(props: FloatDataProps<T>) {
    const { type, offset = 0 } = props;
    const nullBitmap = toUint8Array(props.nullBitmap);
    const data = toArrayBufferView(type.ArrayType, props.data);
    const { length = data.length, nullCount = props.nullBitmap ? -1 : 0, } = props;
    return new Data(type, offset, length, nullCount, [undefined, data, nullBitmap]);
}

export function visitUtf8<T extends Utf8>(props: Utf8DataProps<T>) {
    const { type, offset = 0 } = props;
    const data = toUint8Array(props.data);
    const nullBitmap = toUint8Array(props.nullBitmap);
    const valueOffsets = toInt32Array(props.valueOffsets);
    const { length = valueOffsets.length - 1, nullCount = props.nullBitmap ? -1 : 0 } = props;
    return new Data(type, offset, length, nullCount, [valueOffsets, data, nullBitmap]);
}

export function visitBinary<T extends Binary>(props: BinaryDataProps<T>) {
    const { type, offset = 0 } = props;
    const data = toUint8Array(props.data);
    const nullBitmap = toUint8Array(props.nullBitmap);
    const valueOffsets = toInt32Array(props.valueOffsets);
    const { length = valueOffsets.length - 1, nullCount = props.nullBitmap ? -1 : 0 } = props;
    return new Data(type, offset, length, nullCount, [valueOffsets, data, nullBitmap]);
}

export function visitFixedSizeBinary<T extends FixedSizeBinary>(props: FixedSizeBinaryDataProps<T>) {
    const { type, offset = 0 } = props;
    const nullBitmap = toUint8Array(props.nullBitmap);
    const data = toArrayBufferView(type.ArrayType, props.data);
    const { length = data.length / strideForType(type), nullCount = props.nullBitmap ? -1 : 0, } = props;
    return new Data(type, offset, length, nullCount, [undefined, data, nullBitmap]);
}

export function visitDate<T extends Date_>(props: DateDataProps<T>) {
    const { type, offset = 0 } = props;
    const nullBitmap = toUint8Array(props.nullBitmap);
    const data = toArrayBufferView(type.ArrayType, props.data);
    const { length = data.length / strideForType(type), nullCount = props.nullBitmap ? -1 : 0, } = props;
    return new Data(type, offset, length, nullCount, [undefined, data, nullBitmap]);
}

export function visitTimestamp<T extends Timestamp>(props: TimestampDataProps<T>) {
    const { type, offset = 0 } = props;
    const nullBitmap = toUint8Array(props.nullBitmap);
    const data = toArrayBufferView(type.ArrayType, props.data);
    const { length = data.length / strideForType(type), nullCount = props.nullBitmap ? -1 : 0, } = props;
    return new Data(type, offset, length, nullCount, [undefined, data, nullBitmap]);
}

export function visitTime<T extends Time>(props: TimeDataProps<T>) {
    const { type, offset = 0 } = props;
    const nullBitmap = toUint8Array(props.nullBitmap);
    const data = toArrayBufferView(type.ArrayType, props.data);
    const { length = data.length / strideForType(type), nullCount = props.nullBitmap ? -1 : 0, } = props;
    return new Data(type, offset, length, nullCount, [undefined, data, nullBitmap]);
}

export function visitDecimal<T extends Decimal>(props: DecimalDataProps<T>) {
    const { type, offset = 0 } = props;
    const nullBitmap = toUint8Array(props.nullBitmap);
    const data = toArrayBufferView(type.ArrayType, props.data);
    const { length = data.length / strideForType(type), nullCount = props.nullBitmap ? -1 : 0, } = props;
    return new Data(type, offset, length, nullCount, [undefined, data, nullBitmap]);
}

export function visitList<T extends List>(props: ListDataProps<T>) {
    const { type, offset = 0, child } = props;
    const nullBitmap = toUint8Array(props.nullBitmap);
    const valueOffsets = toInt32Array(props.valueOffsets);
    const { length = valueOffsets.length - 1, nullCount = props.nullBitmap ? -1 : 0 } = props;
    return new Data(type, offset, length, nullCount, [valueOffsets, undefined, nullBitmap], [child]);
}

export function visitStruct<T extends Struct>(props: StructDataProps<T>) {
    const { type, offset = 0, children } = props;
    const nullBitmap = toUint8Array(props.nullBitmap);
    const {
        length = children.reduce((len, {length}) => Math.max(len, length), Number.MIN_SAFE_INTEGER),
        nullCount = props.nullBitmap ? -1 : 0
    } = props;
    return new Data(type, offset, length, nullCount, [undefined, undefined, nullBitmap], children);
}

export function visitUnion<T extends Union>(props: UnionDataProps<T>) {
    const { type, offset = 0, children } = props;
    const nullBitmap = toUint8Array(props.nullBitmap);
    const typeIds = toArrayBufferView(type.ArrayType, props.typeIds);
    const { length = typeIds.length, nullCount = props.nullBitmap ? -1 : 0, } = props;
    if (DataType.isSparseUnion(type)) {
        return new Data(type, offset, length, nullCount, [undefined, undefined, nullBitmap, typeIds], children);
    }
    const valueOffsets = toInt32Array(props.valueOffsets);
    return new Data(type, offset, length, nullCount, [valueOffsets, undefined, nullBitmap, typeIds], children);
}

export function visitDictionary<T extends Dictionary>(props: DictionaryDataProps<T>) {
    const { type, offset = 0 } = props;
    const nullBitmap = toUint8Array(props.nullBitmap);
    const data = toArrayBufferView(type.indices.ArrayType, props.data);
    const { length = data.length, nullCount = props.nullBitmap ? -1 : 0, dictionary } = props;
    return new Data(type, offset, length, nullCount, [undefined, data, nullBitmap], [], dictionary);
}

export function visitInterval<T extends Interval>(props: IntervalDataProps<T>) {
    const { type, offset = 0 } = props;
    const nullBitmap = toUint8Array(props.nullBitmap);
    const data = toArrayBufferView(type.ArrayType, props.data);
    const { length = data.length / strideForType(type), nullCount = props.nullBitmap ? -1 : 0, } = props;
    return new Data(type, offset, length, nullCount, [undefined, data, nullBitmap]);
}

export function visitFixedSizeList<T extends FixedSizeList>(props: FixedSizeListDataProps<T>) {
    const { type, offset = 0, child } = props;
    const nullBitmap = toUint8Array(props.nullBitmap);
    const { length = child.length / strideForType(type), nullCount = props.nullBitmap ? -1 : 0 } = props;
    return new Data(type, offset, length, nullCount, [undefined, undefined, nullBitmap], [child]);
}

export function visitMap<T extends Map_>(props: MapDataProps<T>) {
    const { type, offset = 0, child } = props;
    const nullBitmap = toUint8Array(props.nullBitmap);
    const valueOffsets = toInt32Array(props.valueOffsets);
    const { length = valueOffsets.length - 1, nullCount = props.nullBitmap ? -1 : 0, } = props;
    return new Data(type, offset, length, nullCount, [valueOffsets, undefined, nullBitmap], [child]);
}

export function makeData<T extends Binary>(props: BinaryDataProps<T>): Data<T>;
export function makeData<T extends Bool>(props: BoolDataProps<T>): Data<T>;
export function makeData<T extends Date_>(props: DateDataProps<T>): Data<T>;
export function makeData<T extends Decimal>(props: DecimalDataProps<T>): Data<T>;
export function makeData<T extends Dictionary>(props: DictionaryDataProps<T>): Data<T>;
export function makeData<T extends FixedSizeBinary>(props: FixedSizeBinaryDataProps<T>): Data<T>;
export function makeData<T extends FixedSizeList>(props: FixedSizeListDataProps<T>): Data<T>;
export function makeData<T extends Float>(props: FloatDataProps<T>): Data<T>;
export function makeData<T extends Interval>(props: IntervalDataProps<T>): Data<T>;
export function makeData<T extends Int>(props: IntDataProps<T>): Data<T>;
export function makeData<T extends List>(props: ListDataProps<T>): Data<T>;
export function makeData<T extends Map_>(props: MapDataProps<T>): Data<T>;
export function makeData<T extends Null>(props: NullDataProps<T>): Data<T>;
export function makeData<T extends Struct>(props: StructDataProps<T>): Data<T>;
export function makeData<T extends Timestamp>(props: TimestampDataProps<T>): Data<T>;
export function makeData<T extends Time>(props: TimeDataProps<T>): Data<T>;
export function makeData<T extends SparseUnion>(props: SparseUnionDataProps<T>): Data<T>;
export function makeData<T extends DenseUnion>(props: DenseUnionDataProps<T>): Data<T>;
export function makeData<T extends Union>(props: UnionDataProps<T>): Data<T>;
export function makeData<T extends Utf8>(props: Utf8DataProps<T>): Data<T>;
export function makeData<T extends DataType>(props: DataProps<T>): Data<T>;
export function makeData(props: any) {
    return instance.visit(props);
}

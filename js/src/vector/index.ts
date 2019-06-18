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

export { Row } from './row';
export { Vector } from '../vector';
export { BaseVector } from './base';
export { BinaryVector } from './binary';
export { BoolVector } from './bool';
export { Chunked } from './chunked';
export { DateVector, DateDayVector, DateMillisecondVector } from './date';
export { DecimalVector } from './decimal';
export { DictionaryVector } from './dictionary';
export { FixedSizeBinaryVector } from './fixedsizebinary';
export { FixedSizeListVector } from './fixedsizelist';
export { FloatVector, Float16Vector, Float32Vector, Float64Vector } from './float';
export { IntervalVector, IntervalDayTimeVector, IntervalYearMonthVector } from './interval';
export { IntVector, Int8Vector, Int16Vector, Int32Vector, Int64Vector, Uint8Vector, Uint16Vector, Uint32Vector, Uint64Vector } from './int';
export { ListVector } from './list';
export { MapVector } from './map';
export { NullVector } from './null';
export { StructVector } from './struct';
export { TimestampVector, TimestampSecondVector, TimestampMillisecondVector, TimestampMicrosecondVector, TimestampNanosecondVector } from './timestamp';
export { TimeVector, TimeSecondVector, TimeMillisecondVector, TimeMicrosecondVector, TimeNanosecondVector } from './time';
export { UnionVector, DenseUnionVector, SparseUnionVector } from './union';
export { Utf8Vector } from './utf8';

import { Data } from '../data';
import { Type } from '../enum';
import { Vector } from '../vector';
import { DataType } from '../type';
import { BaseVector } from './base';
import { setBool } from '../util/bit';
import { Vector as V, VectorCtorArgs } from '../interfaces';
import { instance as getVisitor } from '../visitor/get';
import { instance as setVisitor } from '../visitor/set';
import { instance as indexOfVisitor } from '../visitor/indexof';
import { instance as toArrayVisitor } from '../visitor/toarray';
import { instance as iteratorVisitor } from '../visitor/iterator';
import { instance as byteWidthVisitor } from '../visitor/bytewidth';
import { instance as getVectorConstructor } from '../visitor/vectorctor';

declare module '../vector' {
    namespace Vector {
        export { newVector as new };
    }
}

declare module './base' {
    interface BaseVector<T extends DataType> {
        get(index: number): T['TValue'] | null;
        set(index: number, value: T['TValue'] | null): void;
        indexOf(value: T['TValue'] | null, fromIndex?: number): number;
        toArray(): T['TArray'];
        getByteWidth(): number;
        [Symbol.iterator](): IterableIterator<T['TValue'] | null>;
    }
}

/** @nocollapse */
Vector.new = newVector;

/** @ignore */
function newVector<T extends DataType>(data: Data<T>, ...args: VectorCtorArgs<V<T>>): V<T> {
    return new (getVectorConstructor.getVisitFn(data.type)())(data, ...args) as V<T>;
}

//
// We provide the following method implementations for code navigability purposes only.
// They're overridden at runtime below with the specific Visitor implementation for each type,
// short-circuiting the usual Visitor traversal and reducing intermediate lookups and calls.
// This comment is here to remind you to not set breakpoints in these function bodies, or to inform
// you why the breakpoints you have already set are not being triggered. Have a great day!
//

BaseVector.prototype.get = function baseVectorGet<T extends DataType>(this: BaseVector<T>, index: number): T['TValue'] | null {
    return getVisitor.visit(this, index);
};

BaseVector.prototype.set = function baseVectorSet<T extends DataType>(this: BaseVector<T>, index: number, value: T['TValue'] | null): void {
    return setVisitor.visit(this, index, value);
};

BaseVector.prototype.indexOf = function baseVectorIndexOf<T extends DataType>(this: BaseVector<T>, value: T['TValue'] | null, fromIndex?: number): number {
    return indexOfVisitor.visit(this, value, fromIndex);
};

BaseVector.prototype.toArray = function baseVectorToArray<T extends DataType>(this: BaseVector<T>): T['TArray'] {
    return toArrayVisitor.visit(this);
};

BaseVector.prototype.getByteWidth = function baseVectorGetByteWidth<T extends DataType>(this: BaseVector<T>): number {
    return byteWidthVisitor.visit(this.type);
};

BaseVector.prototype[Symbol.iterator] = function baseVectorSymbolIterator<T extends DataType>(this: BaseVector<T>): IterableIterator<T['TValue'] | null> {
    return iteratorVisitor.visit(this);
};

(BaseVector.prototype as any)._bindDataAccessors = bindBaseVectorDataAccessors;

// Perf: bind and assign the operator Visitor methods to each of the Vector subclasses for each Type
(Object.keys(Type) as any[])
    .filter((typeId) => typeId !== Type.NONE && typeId !== Type[Type.NONE])
    .map((T: any) => Type[T] as any).filter((T: any): T is Type => typeof T === 'number')
    .forEach((typeId) => {
        let typeIds: Type[];
        switch (typeId) {
            case Type.Int:       typeIds = [Type.Int8, Type.Int16, Type.Int32, Type.Int64, Type.Uint8, Type.Uint16, Type.Uint32, Type.Uint64]; break;
            case Type.Float:     typeIds = [Type.Float16, Type.Float32, Type.Float64]; break;
            case Type.Date:      typeIds = [Type.DateDay, Type.DateMillisecond]; break;
            case Type.Time:      typeIds = [Type.TimeSecond, Type.TimeMillisecond, Type.TimeMicrosecond, Type.TimeNanosecond]; break;
            case Type.Timestamp: typeIds = [Type.TimestampSecond, Type.TimestampMillisecond, Type.TimestampMicrosecond, Type.TimestampNanosecond]; break;
            case Type.Interval:  typeIds = [Type.IntervalDayTime, Type.IntervalYearMonth]; break;
            case Type.Union:     typeIds = [Type.DenseUnion, Type.SparseUnion]; break;
            default:                typeIds = [typeId]; break;
        }
        typeIds.forEach((typeId) => {
            const VectorCtor = getVectorConstructor.visit(typeId);
            VectorCtor.prototype['get'] = partial1(getVisitor.getVisitFn(typeId));
            VectorCtor.prototype['set'] = partial2(setVisitor.getVisitFn(typeId));
            VectorCtor.prototype['indexOf'] = partial2(indexOfVisitor.getVisitFn(typeId));
            VectorCtor.prototype['toArray'] = partial0(toArrayVisitor.getVisitFn(typeId));
            VectorCtor.prototype['getByteWidth'] = partialType0(byteWidthVisitor.getVisitFn(typeId));
            VectorCtor.prototype[Symbol.iterator] = partial0(iteratorVisitor.getVisitFn(typeId));
        });
    });

/** @ignore */
function partial0<T>(visit: (node: T) => any) {
    return function(this: T) { return visit(this); };
}

/** @ignore */
function partialType0<T extends Vector>(visit: (node: T['type']) => any) {
    return function(this: T) { return visit(this.type); };
}

/** @ignore */
function partial1<T>(visit: (node: T, a: any) => any) {
    return function(this: T, a: any) { return visit(this, a); };
}

/** @ignore */
function partial2<T>(visit: (node: T, a: any, b: any) => any) {
    return function(this: T, a: any, b: any) { return visit(this, a, b); };
}

/** @ignore */
function wrapNullableGet<T extends DataType, V extends Vector<T>, F extends (i: number) => any>(fn: F): (...args: Parameters<F>) => ReturnType<F> {
    return function(this: V, i: number) { return this.isValid(i) ? fn.call(this, i) : null; };
}

/** @ignore */
function wrapNullableSet<T extends DataType, V extends BaseVector<T>, F extends (i: number, a: any) => void>(fn: F): (...args: Parameters<F>) => void {
    return function(this: V, i: number, a: any) {
        if (setBool(this.nullBitmap, this.offset + i, !(a === null || a === undefined))) {
            fn.call(this, i, a);
        }
    };
}

/** @ignore */
function bindBaseVectorDataAccessors<T extends DataType>(this: BaseVector<T>) {
    const nullBitmap = this.nullBitmap;
    if (nullBitmap && nullBitmap.byteLength > 0) {
        this.get = wrapNullableGet(this.get);
        this.set = wrapNullableSet(this.set);
    }
}

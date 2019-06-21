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

import * as fn from '../util/fn';
import { Data } from '../data';
import { Type } from '../enum';
import { Vector } from '../vector';
import { DataType } from '../type';
import { Chunked } from './chunked';
import { BaseVector } from './base';
import { setBool } from '../util/bit';
import { isIterable, isAsyncIterable } from '../util/compat';
import { Builder, IterableBuilderOptions } from '../builder';
import { VectorType as V, VectorCtorArgs } from '../interfaces';
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
        export { vectorFrom as from };
    }
}

declare module './base' {
    namespace BaseVector {
        export { vectorFrom as from };
    }
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

/** @nocollapse */
Vector.from = vectorFrom;

/** @ignore */
function newVector<T extends DataType>(data: Data<T>, ...args: VectorCtorArgs<V<T>>): V<T> {
    return new (getVectorConstructor.getVisitFn<T>(data)())(data, ...args) as V<T>;
}

/** @ignore */
export interface VectorBuilderOptions<T extends DataType, TNull = any> extends IterableBuilderOptions<T, TNull> { values: Iterable<T['TValue'] | TNull>; }
/** @ignore */
export interface VectorBuilderOptionsAsync<T extends DataType, TNull = any> extends IterableBuilderOptions<T, TNull> { values: AsyncIterable<T['TValue'] | TNull>; }

/** @ignore */
export function vectorFromValuesWithType<T extends DataType, TNull = any>(newDataType: () => T, input: Iterable<T['TValue'] | TNull> | AsyncIterable<T['TValue'] | TNull> | VectorBuilderOptions<T, TNull> | VectorBuilderOptionsAsync<T, TNull>) {
    if (isIterable(input)) {
        return Vector.from({ 'nullValues': [null, undefined], type: newDataType(), 'values': input }) as V<T>;
    } else if (isAsyncIterable(input)) {
        return Vector.from({ 'nullValues': [null, undefined], type: newDataType(), 'values': input }) as Promise<V<T>>;
    }
    const {
        'values': values = [],
        'type': type = newDataType(),
        'nullValues': nullValues = [null, undefined],
    } = { ...input };
    return isIterable(values)
        ? Vector.from({ nullValues, ...input, type } as VectorBuilderOptions<T, TNull>)
        : Vector.from({ nullValues, ...input, type } as VectorBuilderOptionsAsync<T, TNull>);
}

/** @ignore */
function vectorFrom<T extends DataType = any, TNull = any>(input: VectorBuilderOptions<T, TNull>): Vector<T>;
function vectorFrom<T extends DataType = any, TNull = any>(input: VectorBuilderOptionsAsync<T, TNull>): Promise<Vector<T>>;
function vectorFrom<T extends DataType = any, TNull = any>(input: VectorBuilderOptions<T, TNull> | VectorBuilderOptionsAsync<T, TNull>) {
    const { 'values': values = [], ...options } = { 'nullValues': [null, undefined], ...input } as VectorBuilderOptions<T, TNull> | VectorBuilderOptionsAsync<T, TNull>;
    if (isIterable<T['TValue'] | TNull>(values)) {
        const chunks = [...Builder.throughIterable(options)(values)];
        return chunks.length === 1 ? chunks[0] : Chunked.concat<T>(chunks);
    }
    return (async (chunks: V<T>[]) => {
        const transform = Builder.throughAsyncIterable(options);
        for await (const chunk of transform(values)) {
            chunks.push(chunk);
        }
        return chunks.length === 1 ? chunks[0] : Chunked.concat<T>(chunks);
    })([]);
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
    .map((T: any) => Type[T] as any)
    .filter((T: any): T is Type => typeof T === 'number')
    .filter((typeId) => typeId !== Type.NONE)
    .forEach((typeId) => {
        const VectorCtor = getVectorConstructor.visit(typeId);
        VectorCtor.prototype['get'] = fn.partial1(getVisitor.getVisitFn(typeId));
        VectorCtor.prototype['set'] = fn.partial2(setVisitor.getVisitFn(typeId));
        VectorCtor.prototype['indexOf'] = fn.partial2(indexOfVisitor.getVisitFn(typeId));
        VectorCtor.prototype['toArray'] = fn.partial0(toArrayVisitor.getVisitFn(typeId));
        VectorCtor.prototype['getByteWidth'] = partialType0(byteWidthVisitor.getVisitFn(typeId));
        VectorCtor.prototype[Symbol.iterator] = fn.partial0(iteratorVisitor.getVisitFn(typeId));
    });

/** @ignore */
function partialType0<T extends Vector>(visit: (node: T['type']) => any) {
    return function(this: T) { return visit(this.type); };
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

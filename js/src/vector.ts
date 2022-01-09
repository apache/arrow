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

import { Type } from './enum.js';
import { clampRange } from './util/vector.js';
import { DataType, strideForType } from './type.js';
import { Data, makeData, DataProps } from './data.js';
import { Builder, IterableBuilderOptions } from './builder.js';
import { ArrayDataType, BigIntArray, JavaScriptArrayDataType, TypedArray, TypedArrayDataType } from './interfaces.js';

import {
    ChunkedIterator,
    isChunkedValid,
    computeChunkOffsets,
    computeChunkNullCounts,
    sliceChunks,
    wrapChunkedCall1,
    wrapChunkedCall2,
    wrapChunkedIndexOf,
} from './util/chunk.js';

import { NumericIndexingProxyHandlerMixin } from './util/proxy.js';

import { instance as getVisitor } from './visitor/get.js';
import { instance as setVisitor } from './visitor/set.js';
import { instance as indexOfVisitor } from './visitor/indexof.js';
import { instance as toArrayVisitor } from './visitor/toarray.js';
import { instance as byteLengthVisitor } from './visitor/bytelength.js';

export interface Vector<T extends DataType = any> {
    ///
    // Virtual properties for the TypeScript compiler.
    // These do not exist at runtime.
    ///
    readonly TType: T['TType'];
    readonly TArray: T['TArray'];
    readonly TValue: T['TValue'];

    /**
     * @see https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Symbol/isConcatSpreadable
     */
    [Symbol.isConcatSpreadable]: true;
}

const vectorPrototypesByTypeId = {} as { [typeId: number]: any };

/**
 * Array-like data structure. Use the convenience method {@link makeVector} and {@link vectorFromArray} to create vectors.
 */
export class Vector<T extends DataType = any> {

    constructor(...args: Data<T>[]);
    constructor(...args: Vector<T>[]);
    constructor(...args: (readonly (Data<T> | Vector<T>)[])[]);
    constructor(...args: any[]) {
        const data = args.flat(1).flatMap(
            /** Specialized version of {@link unwrapInputs} so we don't need makeVector and prevent treeshaking. */
            arg => arg instanceof Data ? arg : arg.data
        );
        if (data.some((x) => !(x instanceof Data))) {
            throw new TypeError('Vector constructor expects an Array of Data instances.');
        }
        this.data = data;
        this.type = data[0]?.type;
        switch (data.length) {
            case 0: this._offsets = new Uint32Array([0]); break;
            case 1: this._offsets = new Uint32Array([0, data[0].length]); break;
            default: this._offsets = computeChunkOffsets(data); break;
        }
        this.stride = strideForType(this.type);
        this.numChildren = this.type.children?.length ?? 0;
        this.length = this._offsets[this._offsets.length - 1];
        Object.setPrototypeOf(this, vectorPrototypesByTypeId[this.type.typeId]);
    }

    declare protected _offsets: Uint32Array;
    declare protected _nullCount: number;
    declare protected _byteLength: number;

    /**
     * Get and set elements by index.
     */
    [index: number]: T['TValue'] | null;

    /**
     * The {@link DataType `DataType`} of this Vector.
     */
    public declare readonly type: T;

    /**
     * The primitive {@link Data `Data`} instances for this Vector's elements.
     */
    public declare readonly data: ReadonlyArray<Data<T>>;

    /**
     * The number of elements in this Vector.
     */
    public declare readonly length: number;

    /**
     * The number of primitive values per Vector element.
     */
    public declare readonly stride: number;

    /**
     * The number of child Vectors if this Vector is a nested dtype.
     */
    public declare readonly numChildren: number;

    /**
     * The aggregate size (in bytes) of this Vector's buffers and/or child Vectors.
     */
    public get byteLength() {
        if (this._byteLength === -1) {
            this._byteLength = this.data.reduce((byteLength, data) => byteLength + data.byteLength, 0);
        }
        return this._byteLength;
    }

    /**
     * The number of null elements in this Vector.
     */
    public get nullCount() {
        if (this._nullCount === -1) {
            this._nullCount = computeChunkNullCounts(this.data);
        }
        return this._nullCount;
    }

    /**
     * The Array or TypedAray constructor used for the JS representation
     *  of the element's values in {@link Vector.prototype.toArray `toArray()`}.
     */
    public get ArrayType(): T['ArrayType'] { return this.type.ArrayType; }

    /**
     * The name that should be printed when the Vector is logged in a message.
     */
    public get [Symbol.toStringTag]() {
        return `${this.VectorName}<${this.type[Symbol.toStringTag]}>`;
    }

    /**
     * The name of this Vector.
     */
    public get VectorName() { return `${Type[this.type.typeId]}Vector`; }

    /**
     * Check whether an element is null.
     * @param index The index at which to read the validity bitmap.
     */
    // @ts-ignore
    public isValid(index: number): boolean { return false; }

    /**
     * Get an element value by position.
     * @param index The index of the element to read.
     */
    // @ts-ignore
    public get(index: number): T['TValue'] | null { return null; }

    /**
     * Set an element value by position.
     * @param index The index of the element to write.
     * @param value The value to set.
     */
    // @ts-ignore
    public set(index: number, value: T['TValue'] | null): void { return; }

    /**
     * Retrieve the index of the first occurrence of a value in an Vector.
     * @param element The value to locate in the Vector.
     * @param offset The index at which to begin the search. If offset is omitted, the search starts at index 0.
     */
    // @ts-ignore
    public indexOf(element: T['TValue'], offset?: number): number { return -1; }

    public includes(element: T['TValue'], offset?: number): boolean { return this.indexOf(element, offset) > 0; }

    /**
     * Get the size in bytes of an element by index.
     * @param index The index at which to get the byteLength.
     */
    // @ts-ignore
    public getByteLength(index: number): number { return 0; }

    /**
     * Iterator for the Vector's elements.
     */
    public [Symbol.iterator](): IterableIterator<T['TValue'] | null> {
        return new ChunkedIterator(this.data);
    }

    /**
     * Combines two or more Vectors of the same type.
     * @param others Additional Vectors to add to the end of this Vector.
     */
    public concat(...others: Vector<T>[]): Vector<T> {
        return new Vector(this.data.concat(others.flatMap((x) => x.data).flat(Infinity)));
    }

    /**
     * Return a zero-copy sub-section of this Vector.
     * @param start The beginning of the specified portion of the Vector.
     * @param end The end of the specified portion of the Vector. This is exclusive of the element at the index 'end'.
     */
    public slice(begin?: number, end?: number): Vector<T> {
        return new Vector(clampRange(this, begin, end, ({ data, _offsets }, begin, end) =>
            sliceChunks(data, _offsets, begin, end)
        ));
    }

    public toJSON() { return [...this]; }

    /**
     * Return a JavaScript Array or TypedArray of the Vector's elements.
     *
     * @note If this Vector contains a single Data chunk and the Vector's type is a
     *  primitive numeric type corresponding to one of the JavaScript TypedArrays, this
     *  method returns a zero-copy slice of the underlying TypedArray values. If there's
     *  more than one chunk, the resulting TypedArray will be a copy of the data from each
     *  chunk's underlying TypedArray values.
     *
     * @returns An Array or TypedArray of the Vector's elements, based on the Vector's DataType.
     */
    public toArray(): T['TArray'] {
        const data = this.data;
        const toArray = toArrayVisitor.getVisitFn(this.type.typeId);
        switch (data.length) {
            case 1: return toArray(data[0]);
            case 0: return new this.ArrayType();
        }
        let { ArrayType } = this;
        const arrays = data.map(toArray);
        if (ArrayType !== arrays[0].constructor) {
            ArrayType = arrays[0].constructor;
        }
        return ArrayType === Array ? arrays.flat(1) : arrays.reduce((memo, array) => {
            memo.array.set(array, memo.offset);
            memo.offset += array.length;
            return memo;
        }, { array: new ArrayType(this.length * this.stride), offset: 0 }).array;
    }

    /**
     * Returns a string representation of the Vector.
     *
     * @returns A string representation of the Vector.
     */
    public toString() {
        return `[${[...this].join(',')}]`;
    }

    /**
     * Returns a child Vector by name, or null if this Vector has no child with the given name.
     * @param name The name of the child to retrieve.
     */
    public getChild<R extends keyof T['TChildren']>(name: R) {
        return this.getChildAt(this.type.children?.findIndex((f) => f.name === name));
    }

    /**
     * Returns a child Vector by index, or null if this Vector has no child at the supplied index.
     * @param index The index of the child to retrieve.
     */
    public getChildAt<R extends DataType = any>(index: number): Vector<R> | null {
        if (index > -1 && index < this.numChildren) {
            return new Vector(this.data.map(({ children }) => children[index] as Data<R>));
        }
        return null;
    }

    /**
     * Adds memoization to the Vector's {@link get} method.
     * For dictionary vectors, this method return a vector that memoizes only the dictionary values.
     *
     * @returns A new vector that memoizes calls to {@link get}.
     */
    public memoize(): MemoizedVector<T> {
        if (DataType.isDictionary(this.type)) {
            const dictionary = new MemoizedVector(this.data[0].dictionary!);
            const newData = this.data.map((data) => {
                const newData = data.clone();
                newData.dictionary = dictionary;
                return newData;
            });
            return new Vector(newData);
        }
        return new MemoizedVector(this);
    }

    /**
     * Returns a new vector without memoization of the {@link get} method.
     * Memoization is very useful when decoding a value is expensive such as Uft8.
     * The memoization creates a cache of the size of the Vector and therfore increases memory usage.
     *
     * @returns A new vector without memoization.
     */
    public unmemoize(): Vector<T> {
        return this;
    }

    // Initialize this static property via an IIFE so bundlers don't tree-shake
    // out this logic, but also so we're still compliant with `"sideEffects": false`
    protected static [Symbol.toStringTag] = ((proto: Vector) => {
        (proto as any).type = DataType.prototype;
        (proto as any).data = [];
        (proto as any).length = 0;
        (proto as any).stride = 1;
        (proto as any).numChildren = 0;
        (proto as any)._nullCount = -1;
        (proto as any)._byteLength = -1;
        (proto as any)._offsets = new Uint32Array([0]);
        (proto as any)[Symbol.isConcatSpreadable] = true;
        Object.setPrototypeOf(proto, new Proxy({}, new NumericIndexingProxyHandlerMixin(
            (inst, key) => inst.get(key),
            (inst, key, val) => inst.set(key, val)
        )));

        Object.assign(vectorPrototypesByTypeId, Object
            .keys(Type).map((T: any) => Type[T] as any)
            .filter((T: any) => typeof T === 'number' && T !== Type.NONE)
            .reduce((prototypes, typeId) => ({
                ...prototypes,
                [typeId]: Object.create(proto, {
                    ['isValid']: { value: wrapChunkedCall1(isChunkedValid) },
                    ['get']: { value: wrapChunkedCall1(getVisitor.getVisitFnByTypeId(typeId)) },
                    ['set']: { value: wrapChunkedCall2(setVisitor.getVisitFnByTypeId(typeId)) },
                    ['indexOf']: { value: wrapChunkedIndexOf(indexOfVisitor.getVisitFnByTypeId(typeId)) },
                    ['getByteLength']: { value: wrapChunkedCall1(byteLengthVisitor.getVisitFnByTypeId(typeId)) },
                })
            }), {}));

        return 'Vector';
    })(Vector.prototype);
}

export class MemoizedVector<T extends DataType = any> extends Vector<T> {

    public constructor(vector: Vector<T>) {
        super(vector.data);

        const get = this.get;
        const set = this.set;

        const cache = new Array<T['TValue'] | null>(this.length);

        Object.defineProperty(this, 'get', {
            value: (index: number) => {
                const cachedValue = cache[index];
                if (cachedValue !== undefined) {
                    return cachedValue;
                }
                const value = get.call(this, index);
                cache[index] = value;
                return value;
            }
        });

        Object.defineProperty(this, 'set', {
            value: (index: number, value: T['TValue'] | null) => {
                set.call(this, index, value);
                cache[index] = value;
            }
        });
    }

    public memoize() {
        return this;
    }

    public unmemoize() {
        if (DataType.isDictionary(this.type)) {
            const dictionary = this.data[0].dictionary!.unmemoize();
            const newData = this.data.map((data) => {
                const newData = data.clone();
                newData.dictionary = dictionary;
                return newData;
            });
            return new Vector(newData);
        }
        return new Vector(this.data);
    }
}

import * as dtypes from './type.js';

/**
 * Creates a Vector without data copies.
 *
 * @example
 * ```ts
 * const vector = makeVector(new Int32Array([1, 2, 3]));
 * ```
 */
export function makeVector<T extends TypedArray | BigIntArray>(data: T | readonly T[]): Vector<TypedArrayDataType<T>>;
export function makeVector<T extends DataView>(data: T | readonly T[]): Vector<dtypes.Int8>;
export function makeVector<T extends DataType>(data: Data<T> | readonly Data<T>[]): Vector<T>;
export function makeVector<T extends DataType>(data: Vector<T> | readonly Vector<T>[]): Vector<T>;
export function makeVector<T extends DataType>(data: DataProps<T> | readonly DataProps<T>[]): Vector<T>;

export function makeVector(init: any) {
    if (init) {
        if (init instanceof Data) { return new Vector(init); }
        if (init instanceof Vector) { return new Vector(init.data); }
        if (init.type instanceof DataType) { return new Vector([makeData(init)]); }
        if (Array.isArray(init)) {
            return new Vector(init.flatMap(unwrapInputs));
        }
        if (ArrayBuffer.isView(init)) {
            if (init instanceof DataView) {
                init = new Uint8Array(init.buffer);
            }
            const props = { offset: 0, length: init.length, nullCount: 0, data: init };
            if (init instanceof Int8Array) { return new Vector([makeData({ ...props, type: new dtypes.Int8 })]); }
            if (init instanceof Int16Array) { return new Vector([makeData({ ...props, type: new dtypes.Int16 })]); }
            if (init instanceof Int32Array) { return new Vector([makeData({ ...props, type: new dtypes.Int32 })]); }
            if (init instanceof BigInt64Array) { return new Vector([makeData({ ...props, type: new dtypes.Int64 })]); }
            if (init instanceof Uint8Array || init instanceof Uint8ClampedArray) { return new Vector([makeData({ ...props, type: new dtypes.Uint8 })]); }
            if (init instanceof Uint16Array) { return new Vector([makeData({ ...props, type: new dtypes.Uint16 })]); }
            if (init instanceof Uint32Array) { return new Vector([makeData({ ...props, type: new dtypes.Uint32 })]); }
            if (init instanceof BigUint64Array) { return new Vector([makeData({ ...props, type: new dtypes.Uint64 })]); }
            if (init instanceof Float32Array) { return new Vector([makeData({ ...props, type: new dtypes.Float32 })]); }
            if (init instanceof Float64Array) { return new Vector([makeData({ ...props, type: new dtypes.Float64 })]); }
            throw new Error('Unrecognized input');
        }
    }
    throw new Error('Unrecognized input');
}


function inferType(value: readonly unknown[]): DataType {
    if (value.length === 0) { return new dtypes.Null; }
    let nullsCount = 0;
    // @ts-ignore
    let arraysCount = 0;
    // @ts-ignore
    let objectsCount = 0;
    let numbersCount = 0;
    let stringsCount = 0;
    let bigintsCount = 0;
    let booleansCount = 0;
    let datesCount = 0;

    for (const val of value) {
        if (val == null) { ++nullsCount; continue; }
        switch (typeof val) {
            case 'bigint': ++bigintsCount; continue;
            case 'boolean': ++booleansCount; continue;
            case 'number': ++numbersCount; continue;
            case 'string': ++stringsCount; continue;
            case 'object':
                if (Array.isArray(val)) {
                    ++arraysCount;
                } else if (Object.prototype.toString.call(val) === '[object Date]') {
                    ++datesCount;
                } else {
                    ++objectsCount;
                }
                continue;
        }
        throw new TypeError('Unable to infer Vector type from input values, explicit type declaration expected');
    }

    if (numbersCount + nullsCount === value.length) {
        return new dtypes.Float64;
    } else if (stringsCount + nullsCount === value.length) {
        return new dtypes.Dictionary(new dtypes.Utf8, new dtypes.Int32);
    } else if (bigintsCount + nullsCount === value.length) {
        return new dtypes.Int64;
    } else if (booleansCount + nullsCount === value.length) {
        return new dtypes.Bool;
    } else if (datesCount + nullsCount === value.length) {
        return new dtypes.TimestampMillisecond;
    }
    // TODO: add more types to infererence

    throw new TypeError('Unable to infer Vector type from input values, explicit type declaration expected');
}

/**
 * Creates a Vector from a JavaScript array. Use {@link makeVector} if you only want to create a vector from a typed array.
 *
 * @example
 * ```ts
 * const vf64 = vectorFromArray([1, 2, 3]);
 * const vi8 = vectorFromArray([1, 2, 3], new Int8);
 * const vdict = vectorFromArray(['foo', 'bar']);
 * ```
 */
export function vectorFromArray(values: readonly (null | undefined)[], type?: dtypes.Null): Vector<dtypes.Null>;
export function vectorFromArray(values: readonly (null | undefined | boolean)[], type?: dtypes.Bool): Vector<dtypes.Bool>;
export function vectorFromArray<T extends dtypes.Utf8 | dtypes.Dictionary<dtypes.Utf8> = dtypes.Dictionary<dtypes.Utf8, dtypes.Int32>>(values: readonly (null | undefined | string)[], type?: T): Vector<T>;
export function vectorFromArray<T extends dtypes.Date_>(values: readonly (null | undefined | Date)[], type?: T): Vector<T>;
export function vectorFromArray<T extends dtypes.Int>(values: readonly (null | undefined | number)[], type: T): Vector<T>;
export function vectorFromArray<T extends dtypes.Int64 | dtypes.Uint64 = dtypes.Int64>(values: readonly (null | undefined | bigint)[], type?: T): Vector<T>;
export function vectorFromArray<T extends dtypes.Float = dtypes.Float64>(values: readonly (null | undefined | number)[], type?: T): Vector<T>;
export function vectorFromArray<T extends DataType>(values: readonly (unknown)[], type: T): Vector<T>;
export function vectorFromArray<T extends readonly unknown[]>(values: T): Vector<JavaScriptArrayDataType<T>>;
/** Creates a Vector from a typed array via {@link makeVector}. */
export function vectorFromArray<T extends TypedArray | BigIntArray>(data: T): Vector<TypedArrayDataType<T>>;

export function vectorFromArray<T extends DataType>(data: Data<T>): Vector<T>;
export function vectorFromArray<T extends DataType>(data: Vector<T>): Vector<T>;
export function vectorFromArray<T extends DataType>(data: DataProps<T>): Vector<T>;
export function vectorFromArray<T extends TypedArray | BigIntArray | readonly unknown[]>(data: T): Vector<ArrayDataType<T>>;

export function vectorFromArray(init: any, type?: DataType) {
    if (init instanceof Data || init instanceof Vector || init.type instanceof DataType || ArrayBuffer.isView(init)) {
        return makeVector(init as any);
    }
    const options: IterableBuilderOptions = { type: type ?? inferType(init) };
    const chunks = [...Builder.throughIterable(options)(init)];
    const vector = chunks.length === 1 ? chunks[0] : chunks.reduce((a, b) => a.concat(b));
    if (DataType.isDictionary(vector.type)) {
        return vector.memoize();
    }
    return vector;
}

function unwrapInputs(x: any) {
    return x instanceof Data ? [x] : x instanceof Vector ? x.data : makeVector(x).data;
}

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
import { Type } from '../enum';
import { DataType } from '../type';
import { Chunked } from './chunked';
import { clampRange } from '../util/vector';
import { VectorType as V } from '../interfaces';
import { AbstractVector, Vector, Clonable, Sliceable, Applicative } from '../vector';

/** @ignore */
export interface BaseVector<T extends DataType = any> extends Clonable<V<T>>, Sliceable<V<T>>, Applicative<T, Chunked<T>> {
    slice(begin?: number, end?: number): V<T>;
    concat(...others: Vector<T>[]): Chunked<T>;
    clone<R extends DataType = T>(data: Data<R>, children?: Vector<R>[]): V<R>;
}

/** @ignore */
export abstract class BaseVector<T extends DataType = any> extends AbstractVector<T>
    implements Clonable<V<T>>, Sliceable<V<T>>, Applicative<T, Chunked<T>> {

    protected _children?: Vector[];

    constructor(data: Data<T>, children?: Vector[]) {
        super();
        this._children = children;
        this.numChildren = data.childData.length;
        this._bindDataAccessors(this.data = data);
    }

    public readonly data: Data<T>;
    public readonly numChildren: number;

    public get type() { return this.data.type; }
    public get typeId() { return this.data.typeId; }
    public get length() { return this.data.length; }
    public get offset() { return this.data.offset; }
    public get stride() { return this.data.stride; }
    public get nullCount() { return this.data.nullCount; }
    public get byteLength() { return this.data.byteLength; }
    public get VectorName() { return `${Type[this.typeId]}Vector`; }

    public get ArrayType(): T['ArrayType'] { return this.type.ArrayType; }

    public get values() { return this.data.values; }
    public get typeIds() { return this.data.typeIds; }
    public get nullBitmap() { return this.data.nullBitmap; }
    public get valueOffsets() { return this.data.valueOffsets; }

    public get [Symbol.toStringTag]() { return `${this.VectorName}<${this.type[Symbol.toStringTag]}>`; }

    public clone<R extends DataType = T>(data: Data<R>, children = this._children) {
        return Vector.new<R>(data, children) as any;
    }

    public concat(...others: Vector<T>[]) {
        return Chunked.concat<T>(this, ...others);
    }

    public slice(begin?: number, end?: number) {
        // Adjust args similar to Array.prototype.slice. Normalize begin/end to
        // clamp between 0 and length, and wrap around on negative indices, e.g.
        // slice(-1, 5) or slice(5, -1)
        return clampRange(this, begin, end, this._sliceInternal);
    }

    public isValid(index: number): boolean {
        if (this.nullCount > 0) {
            const idx = this.offset + index;
            const val = this.nullBitmap[idx >> 3];
            const mask = (val & (1 << (idx % 8)));
            return mask !== 0;
        }
        return true;
    }

    public getChildAt<R extends DataType = any>(index: number): Vector<R> | null {
        return index < 0 || index >= this.numChildren ? null : (
            (this._children || (this._children = []))[index] ||
            (this._children[index] = Vector.new<R>(this.data.childData[index] as Data<R>))
        ) as Vector<R>;
    }

    public toJSON(): any { return [...this]; }

    protected _sliceInternal(self: this, begin: number, end: number) {
        return self.clone(self.data.slice(begin, end - begin));
    }

    // @ts-ignore
    protected _bindDataAccessors(data: Data<T>) {
        // Implementation in src/vectors/index.ts due to circular dependency/packaging shenanigans
    }
}

(BaseVector.prototype as any)[Symbol.isConcatSpreadable] = true;

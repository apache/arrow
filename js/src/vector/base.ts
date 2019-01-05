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
import { Vector } from '../vector';
import { DataType } from '../type';
import { Chunked } from './chunked';
import { clampRange } from '../util/vector';
import { Vector as VType } from '../interfaces';
import { Clonable, Sliceable, Applicative } from '../vector';

export interface BaseVector<T extends DataType = any> extends Clonable<VType<T>>, Sliceable<VType<T>>, Applicative<T, Chunked<T>> {
    slice(begin?: number, end?: number): VType<T>;
    concat(...others: Vector<T>[]): Chunked<T>;
    clone<R extends DataType = T>(data: Data<R>, children?: Vector<R>[], stride?: number): VType<R>;
}

export abstract class BaseVector<T extends DataType = any> extends Vector<T>
    implements Clonable<VType<T>>, Sliceable<VType<T>>, Applicative<T, Chunked<T>> {

    // @ts-ignore
    protected _data: Data<T>;
    protected _stride: number = 1;
    protected _numChildren: number = 0;
    protected _children?: Vector[];

    constructor(data: Data<T>, children?: Vector[], stride?: number) {
        super();
        this._children = children;
        this._numChildren = data.childData.length;
        this._bindDataAccessors(this._data = data);
        this._stride = Math.floor(Math.max(stride || 1, 1));
    }

    public get data() { return this._data; }
    public get stride() { return this._stride; }
    public get numChildren() { return this._numChildren; }

    public get type() { return this._data.type; }
    public get typeId() { return this._data.typeId as T['TType']; }
    public get length() { return this._data.length; }
    public get offset() { return this._data.offset; }
    public get nullCount() { return this._data.nullCount; }
    public get VectorName() { return this.constructor.name; }

    public get ArrayType(): T['ArrayType'] { return this._data.ArrayType; }

    public get values() { return this._data.values; }
    public get typeIds() { return this._data.typeIds; }
    public get nullBitmap() { return this._data.nullBitmap; }
    public get valueOffsets() { return this._data.valueOffsets; }

    public get [Symbol.toStringTag]() { return `${this.VectorName}<${this.type[Symbol.toStringTag]}>`; }

    public clone<R extends DataType = T>(data: Data<R>, children = this._children, stride = this._stride) {
        return Vector.new<R>(data, children, stride) as any;
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
            (this._children[index] = Vector.new<R>(this._data.childData[index] as Data<R>))
        ) as Vector<R>;
    }

    // @ts-ignore
    public toJSON(): any { return [...this]; }

    protected _sliceInternal(self: this, offset: number, length: number) {
        const stride = self.stride;
        return self.clone(self.data.slice(offset * stride, (length - offset) * stride));
    }

    // @ts-ignore
    protected _bindDataAccessors(data: Data<T>) {
        // Implementation in src/vectors/index.ts due to circular dependency/packaging shenanigans
    }
}

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
import { AbstractVector, Vector } from '../vector';
import { DataType } from '../type';
import { Chunked } from './chunked';
import { clampRange } from '../util/vector';
import { Vector as VType } from '../interfaces';
import { Clonable, Sliceable, Applicative } from '../vector';

export interface BaseVector<T extends DataType = any> extends Clonable<VType<T>>, Sliceable<VType<T>>, Applicative<T, Chunked<T>> {
    slice(begin?: number, end?: number): VType<T>;
    concat(...others: Vector<T>[]): Chunked<T>;
    clone<R extends DataType = T>(data: Data<R>, children?: Vector<R>[]): VType<R>;
}

export abstract class BaseVector<T extends DataType = any> extends AbstractVector<T>
    implements Clonable<VType<T>>, Sliceable<VType<T>>, Applicative<T, Chunked<T>> {

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
    public get VectorName() { return this.constructor.name; }

    public get ArrayType(): T['ArrayType'] { return this.data.ArrayType; }

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

    // @ts-ignore
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

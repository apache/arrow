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
import { IterableArrayLike } from '../type';
import { View, Vector, createVector } from '../vector';
import { DataType, NestedType, DenseUnion, SparseUnion, Struct, Map_ } from '../type';

export abstract class NestedView<T extends NestedType> implements View<T> {
    // @ts-ignore
    public length: number;
    // @ts-ignore
    public numChildren: number;
    // @ts-ignore
    public childData: Data<any>[];
    // @ts-ignore
    protected children: Vector<any>[];
    constructor(data: Data<T>, children?: Vector<any>[]) {
        this.length = data.length;
        this.childData = data.childData;
        this.numChildren = data.childData.length;
        this.children = children || new Array(this.numChildren);
    }
    public clone(data: Data<T>): this {
        return new (<any> this.constructor)(data, this.children) as this;
    }
    public isValid(): boolean {
        return true;
    }
    public toArray(): IterableArrayLike<T['TValue']> {
        return [...this];
    }
    public toJSON() { return this.toArray(); }
    public toString() {
        return [...this].map((x) => stringify(x)).join(', ');
    }
    public get(index: number): T['TValue'] {
        return this.getNested(this, index);
    }
    public set(index: number, value: T['TValue']): void {
        return this.setNested(this, index, value);
    }
    protected abstract getNested(self: NestedView<T>, index: number): T['TValue'];
    protected abstract setNested(self: NestedView<T>, index: number, value: T['TValue']): void;
    public getChildAt<R extends DataType = DataType>(index: number) {
        return this.children[index] || (
               this.children[index] = createVector<R>(this.childData[index]));
    }
    public *[Symbol.iterator](): IterableIterator<T['TValue']> {
        const get = this.getNested;
        const length = this.length;
        for (let index = -1; ++index < length;) {
            yield get(this, index);
        }
    }
}

export class UnionView<T extends (DenseUnion | SparseUnion) = SparseUnion> extends NestedView<T> {
    // @ts-ignore
    public typeIds: Int8Array;
    // @ts-ignore
    public valueOffsets?: Int32Array;
    constructor(data: Data<T>, children?: Vector<any>[]) {
        super(data, children);
        this.length = data.length;
        this.typeIds = data.typeIds;
    }
    protected getNested(self: UnionView<T>, index: number): T['TValue'] {
        return self.getChildValue(self, index, self.typeIds, self.valueOffsets);
    }
    protected setNested(self: UnionView<T>, index: number, value: T['TValue']): void {
        return self.setChildValue(self, index, value, self.typeIds, self.valueOffsets);
    }
    protected getChildValue(self: NestedView<T>, index: number, typeIds: Int8Array, _valueOffsets?: any): any | null {
        const child = self.getChildAt(typeIds[index]);
        return child ? child.get(index) : null;
    }
    protected setChildValue(self: NestedView<T>, index: number, value: T['TValue'], typeIds: Int8Array, _valueOffsets?: any): any | null {
        const child = self.getChildAt(typeIds[index]);
        return child ? child.set(index, value) : null;
    }
    public *[Symbol.iterator](): IterableIterator<T['TValue']> {
        const length = this.length;
        const get = this.getChildValue;
        const { typeIds, valueOffsets } = this;
        for (let index = -1; ++index < length;) {
            yield get(this, index, typeIds, valueOffsets);
        }
    }
}

export class DenseUnionView extends UnionView<DenseUnion> {
    // @ts-ignore
    public valueOffsets: Int32Array;
    constructor(data: Data<DenseUnion>, children?: Vector<any>[]) {
        super(data, children);
        this.valueOffsets = data.valueOffsets;
    }
    protected getNested(self: DenseUnionView, index: number): any | null {
        return self.getChildValue(self, index, self.typeIds, self.valueOffsets);
    }
    protected getChildValue(self: NestedView<DenseUnion>, index: number, typeIds: Int8Array, valueOffsets: any): any | null {
        const child = self.getChildAt(typeIds[index]);
        return child ? child.get(valueOffsets[index]) : null;
    }
    protected setChildValue(self: NestedView<DenseUnion>, index: number, value: any, typeIds: Int8Array, valueOffsets?: any): any | null {
        const child = self.getChildAt(typeIds[index]);
        return child ? child.set(valueOffsets[index], value) : null;
    }
}

export class StructView extends NestedView<Struct> {
    protected getNested(self: StructView, index: number) {
        return new RowView(self as any, self.children, index);
    }
    protected setNested(self: StructView, index: number, value: any): void {
        for (let idx = -1, len = self.numChildren; ++idx < len;) {
            self.getChildAt(index).set(index, value[idx]);
        }
    }
}

export class MapView extends NestedView<Map_> {
    // @ts-ignore
    public typeIds: { [k: string]: number };
    constructor(data: Data<Map_>, children?: Vector<any>[]) {
        super(data, children);
        this.typeIds = data.type.children.reduce((xs, x, i) =>
            (xs[x.name] = i) && xs || xs, Object.create(null));
    }
    protected getNested(self: MapView, index: number) {
        return new MapRowView(self as any, self.children, index);
    }
    protected setNested(self: MapView, index: number, value: { [k: string]: any }): void {
        for (const [key, idx] of Object.entries(self.typeIds)) {
            self.getChildAt(idx).set(index, value[key]);
        }
    }
}

export class RowView extends UnionView<SparseUnion> {
    protected rowIndex: number;
    constructor(data: Data<SparseUnion> & NestedView<any>, children?: Vector<any>[], rowIndex?: number) {
        super(data, children);
        this.rowIndex = rowIndex || 0;
        this.length = data.numChildren;
    }
    public clone(data: Data<SparseUnion> & NestedView<any>): this {
        return new (<any> this.constructor)(data, this.children, this.rowIndex) as this;
    }
    protected getChildValue(self: RowView, index: number, _typeIds: any, _valueOffsets?: any): any | null {
        const child = self.getChildAt(index);
        return child ? child.get(self.rowIndex) : null;
    }
    protected setChildValue(self: RowView, index: number, value: any, _typeIds: Int8Array, _valueOffsets?: any): any | null {
        const child = self.getChildAt(index);
        return child ? child.set(self.rowIndex, value) : null;
    }
}

export class MapRowView extends RowView {
    protected getChildValue(self: MapRowView, index: number, typeIds: any, _valueOffsets: any): any | null {
        const child = self.getChildAt(typeIds[index]);
        return child ? child.get(self.rowIndex) : null;
    }
    protected setChildValue(self: MapRowView, index: number, value: any, typeIds: Int8Array, _valueOffsets?: any): any | null {
        const child = self.getChildAt(typeIds[index]);
        return child ? child.set(self.rowIndex, value) : null;
    }
}

function stringify(x: any) {
    return typeof x === 'string' ? `"${x}"` : Array.isArray(x) ? JSON.stringify(x) : ArrayBuffer.isView(x) ? `[${x}]` : `${x}`;
}

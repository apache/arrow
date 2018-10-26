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
import { View, Vector } from '../vector';
import { IterableArrayLike } from '../type';
import { valueToString } from '../util/pretty';
import { DataType, NestedType, DenseUnion, SparseUnion, Struct, StructData, StructValue, Map_ } from '../type';

export abstract class NestedView<T extends NestedType> implements View<T> {
    public length: number;
    public numChildren: number;
    public childData: Data<any>[];
    protected _children: Vector<any>[];
    constructor(data: Data<T>, children?: Vector<any>[]) {
        this.length = data.length;
        this.childData = data.childData;
        this.numChildren = data.childData.length;
        this._children = children || new Array(this.numChildren);
    }
    public clone(data: Data<T>): this {
        return new (<any> this.constructor)(data, new Array(this.numChildren)) as this;
    }
    public isValid(): boolean {
        return true;
    }
    public toArray(): IterableArrayLike<T['TValue']> {
        return [...this];
    }
    public indexOf(_: T['TValue']): number {
        throw new Error(`Not implemented yet`);
    }
    public toJSON(): any { return this.toArray(); }
    public toString() {
        return [...this].map((x) => valueToString(x)).join(', ');
    }
    public get(index: number): T['TValue'] {
        return this.getNested(this, index);
    }
    public set(index: number, value: T['TValue']): void {
        return this.setNested(this, index, value);
    }
    protected abstract getNested(self: NestedView<T>, index: number): T['TValue'];
    protected abstract setNested(self: NestedView<T>, index: number, value: T['TValue']): void;
    public getChildAt<R extends DataType = DataType>(index: number): Vector<R> | null {
        return index < 0 || index >= this.numChildren
            ? null
            : (this._children[index] as Vector<R>) ||
              (this._children[index] = Vector.create<R>(this.childData[index]));
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
    // @ts-ignore
    protected typeIdToChildIndex: { [key: number]: number };
    constructor(data: Data<T>, children?: Vector<any>[]) {
        super(data, children);
        this.length = data.length;
        this.typeIds = data.typeIds;
        this.typeIdToChildIndex = data.typeIdToChildIndex;
    }
    protected getNested(self: UnionView<T>, index: number): T['TValue'] {
        return self.getChildValue(self, index, self.typeIds, self.valueOffsets, self.typeIdToChildIndex);
    }
    protected setNested(self: UnionView<T>, index: number, value: T['TValue']): void {
        return self.setChildValue(self, index, value, self.typeIds, self.valueOffsets, self.typeIdToChildIndex);
    }
    protected getChildValue(self: NestedView<T>, index: number, typeIds: Int8Array, _valueOffsets: any, typeIdToChildIndex: { [key: number]: number }): any | null {
        const child = self.getChildAt(typeIdToChildIndex[typeIds[index]]);
        return child ? child.get(index) : null;
    }
    protected setChildValue(self: NestedView<T>, index: number, value: T['TValue'], typeIds: Int8Array, _valueOffsets: any, typeIdToChildIndex: { [key: number]: number }): any | null {
        const child = self.getChildAt(typeIdToChildIndex[typeIds[index]]);
        return child ? child.set(index, value) : null;
    }
    public *[Symbol.iterator](): IterableIterator<T['TValue']> {
        const length = this.length;
        const get = this.getChildValue;
        const { typeIdToChildIndex } = this;
        const { typeIds, valueOffsets } = this;
        for (let index = -1; ++index < length;) {
            yield get(this, index, typeIds, valueOffsets, typeIdToChildIndex);
        }
    }
}

export class DenseUnionView extends UnionView<DenseUnion> {
    public valueOffsets: Int32Array;
    constructor(data: Data<DenseUnion>, children?: Vector<any>[]) {
        super(data, children);
        this.valueOffsets = data.valueOffsets;
    }
    protected getNested(self: DenseUnionView, index: number): any | null {
        return self.getChildValue(self, index, self.typeIds, self.valueOffsets, self.typeIdToChildIndex);
    }
    protected getChildValue(self: NestedView<DenseUnion>, index: number, typeIds: Int8Array, valueOffsets: any, typeIdToChildIndex: { [key: number]: number }): any | null {
        const child = self.getChildAt(typeIdToChildIndex[typeIds[index]]);
        return child ? child.get(valueOffsets[index]) : null;
    }
    protected setChildValue(self: NestedView<DenseUnion>, index: number, value: any, typeIds: Int8Array, valueOffsets: any, typeIdToChildIndex: { [key: number]: number }): any | null {
        const child = self.getChildAt(typeIdToChildIndex[typeIds[index]]);
        return child ? child.set(valueOffsets[index], value) : null;
    }
}

interface RowViewConstructor<T extends StructData = StructData> {
    readonly prototype: StructValue<T> & RowView;
    new (data: Data<SparseUnion> & NestedView<any>, children?: Vector<any>[], rowIndex?: number): StructValue<T> & RowView;
}

export class StructView<T extends StructData = StructData> extends NestedView<Struct<T>> {
    private RowView: RowViewConstructor<T>;

    constructor(data: Data<Struct<T>>, children?: Vector<any>[]) {
        super(data, children);

        // Make a customized RowView that includes proxies for
        class RowProxy extends RowView {}

        const proto = RowProxy.prototype;

        data.type.children.forEach(function (f, i) {
            Object.defineProperty(proto, f.name, {
                get: function () {
                    return (this as any as RowView).get(i);
                },
                enumerable: true
            });
        });

        this.RowView = (RowProxy as any);
    }
    protected getNested(self: StructView<T>, index: number) {
        return new self.RowView(self as any, self._children, index);
    }
    protected setNested(self: StructView<T>, index: number, value: any): void {
        let idx = -1, len = self.numChildren, child: Vector | null;
        if (!(value instanceof NestedView || value instanceof Vector)) {
            while (++idx < len) {
                if (child = self.getChildAt(idx)) {
                    child.set(index, value[idx]);
                }
            }
        } else {
            while (++idx < len) {
                if (child = self.getChildAt(idx)) {
                    child.set(index, value.get(idx));
                }
            }
        }
    }
}

export class MapView extends NestedView<Map_> {
    public typeIds: { [k: string]: number };
    constructor(data: Data<Map_>, children?: Vector<any>[]) {
        super(data, children);
        this.typeIds = data.type.children.reduce((xs, x, i) =>
            (xs[x.name] = i) && xs || xs, Object.create(null));
    }
    protected getNested(self: MapView, index: number) {
        return new MapRowView(self as any, self._children, index);
    }
    protected setNested(self: MapView, index: number, value: { [k: string]: any }): void {
        let typeIds = self.typeIds as any, child: Vector | null;
        if (!(value instanceof NestedView || value instanceof Vector)) {
            for (const key in typeIds) {
                if (child = self.getChildAt(typeIds[key])) {
                    child.set(index, value[key]);
                }
            }
        } else {
            for (const key in typeIds) {
                if (child = self.getChildAt(typeIds[key])) {
                    child.set(index, value.get(key as any));
                }
            }
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
        return new (<any> this.constructor)(data, this._children, this.rowIndex) as this;
    }
    protected getChildValue(self: RowView, index: number, _typeIds: any, _valueOffsets?: any): any | null {
        const child = self.getChildAt(index);
        return child ? child.get(self.rowIndex) : null;
    }
    protected setChildValue(self: RowView, index: number, value: any, _typeIds: any, _valueOffsets?: any): any | null {
        const child = self.getChildAt(index);
        return child ? child.set(self.rowIndex, value) : null;
    }
}

export class MapRowView extends RowView {
    // @ts-ignore
    public typeIds: any;
    public toJSON() {
        const get = this.getChildValue;
        const result = {} as { [k: string]: any };
        const typeIds = this.typeIds as { [k: string]: number };
        for (const name in typeIds) {
            result[name] = get(this, name, typeIds, null);
        }
        return result;
    }
    protected getChildValue(self: MapRowView, key: any, typeIds: any, _valueOffsets: any): any | null {
        const child = self.getChildAt(typeIds[key]);
        return child ? child.get(self.rowIndex) : null;
    }
    protected setChildValue(self: MapRowView, key: any, value: any, typeIds: any, _valueOffsets?: any): any | null {
        const child = self.getChildAt(typeIds[key]);
        return child ? child.set(self.rowIndex, value) : null;
    }
}

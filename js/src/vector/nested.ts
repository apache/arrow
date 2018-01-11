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
    public typeIds: Int8Array;
    // @ts-ignore
    public valueOffsets: any;
    public readonly numChildren: number;
    public readonly childData: Data<any>[];
    protected children: { [k: number]: Vector<any> } = Object.create(null);
    constructor(protected data: Data<T>) {
        this.childData = data.childData;
        this.numChildren = data.childData.length;
    }
    public isValid(): boolean {
        return true;
    }
    public toArray(): IterableArrayLike<T['TValue']> {
        return [...this];
    }
    public get(index: number): T['TValue'] {
        return this.getNested(this, index);
    }
    protected abstract getNested(view: NestedView<T>, index: number): T['TValue'];
    public getChildAt<R extends DataType = DataType>(index: number) {
        return this.children[index] || (
               this.children[index] = createVector<R>(this.childData[index]));
    }
    public *[Symbol.iterator](): IterableIterator<T['TValue']> {
        const get = this.getNested;
        const length = this.data.length;
        for (let index = -1; ++index < length;) {
            yield get(this, index);
        }
    }
}

export class UnionView<T extends (DenseUnion | SparseUnion) = SparseUnion> extends NestedView<T> {
    constructor(data: Data<T>) {
        super(data);
        this.typeIds = this.typeIds;
    }
    public getNested(view: NestedView<T>, index: number): T['TValue'] {
        return this.getUnionValue(view, index, this.typeIds, this.valueOffsets);
    }
    protected getUnionValue(view: NestedView<T>, index: number, typeIds: Int8Array, _valueOffsets?: any): any | null {
        const child = view.getChildAt(typeIds[index]);
        return child ? child.get(index) : null;
    }
    public *[Symbol.iterator](): IterableIterator<T['TValue']> {
        const get = this.getUnionValue;
        const length = this.data.length;
        const { typeIds, valueOffsets } = this;
        for (let index = -1; ++index < length;) {
            yield get(this, index, typeIds, valueOffsets);
        }
    }
}

export class DenseUnionView extends UnionView<DenseUnion> {
    constructor(data: Data<DenseUnion>) {
        super(data);
        this.valueOffsets = data.valueOffsets;
    }
    public getNested(view: NestedView<DenseUnion>, index: number): any | null {
        return this.getUnionValue(view, index, this.typeIds, this.valueOffsets);
    }
    protected getUnionValue(view: NestedView<DenseUnion>, index: number, typeIds: Int8Array, valueOffsets: any): any | null {
        const child = view.getChildAt(typeIds[index]);
        return child ? child.get(valueOffsets[index]) : null;
    }
}

export abstract class TabularView<T extends NestedType> extends NestedView<T> {
    constructor(data: Data<T>) {
        super(data);
        this.typeIds = new Int8Array(data.childData.map((x) => x.typeId));
        this.valueOffsets = data.type.children.reduce((xs, x) =>
            (xs[x.name] = x.typeId) && xs || xs, Object.create(null));
    }
}

export class MapView extends TabularView<Map_> {
    public getNested(view: MapView, index: number) {
        return new MapRow(view as any, index);
    }
}

export class MapRow extends DenseUnionView {
    constructor(data: Data<DenseUnion>, protected rowIndex: number) {
        super(data);
    }
    protected getUnionValue(view: NestedView<DenseUnion>, index: number, typeIds: Int8Array, valueOffsets: any): any | null {
        const child = view.getChildAt(typeIds[valueOffsets[index]]);
        return child ? child.get(this.rowIndex) : null;
    }
}
export class StructView extends TabularView<Struct> {
    public getNested(view: StructView, index: number) {
        return new StructRow(view as any, index);
    }
}

export class StructRow extends UnionView<SparseUnion> {
    constructor(data: Data<SparseUnion>, protected rowIndex: number) {
        super(data);
    }
    protected getUnionValue(view: NestedView<SparseUnion>, index: number, typeIds: Int8Array, _valueOffsets?: any): any | null {
        const child = view.getChildAt(typeIds[index]);
        return child ? child.get(this.rowIndex) : null;
    }
}

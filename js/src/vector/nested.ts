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
    public length: number;
    // @ts-ignore
    public typeIds: Int8Array;
    // @ts-ignore
    public valueOffsets?: Int32Array;
    constructor(data: Data<T>) {
        super(data);
        this.length = data.length;
        this.typeIds = data.typeIds;
    }
    public getNested(view: NestedView<T>, index: number): T['TValue'] {
        return this.getUnionValue(view, index, this.typeIds, this.valueOffsets);
    }
    protected getUnionValue(view: NestedView<T>, index: number, typeIds: Int8Array, _valueOffsets?: any): any | null {
        const child = view.getChildAt(typeIds[index]);
        return child ? child.get(index) : null;
    }
    public *[Symbol.iterator](): IterableIterator<T['TValue']> {
        const length = this.length;
        const get = this.getUnionValue;
        const { typeIds, valueOffsets } = this;
        for (let index = -1; ++index < length;) {
            yield get(this, index, typeIds, valueOffsets);
        }
    }
}

export class DenseUnionView extends UnionView<DenseUnion> {
    // @ts-ignore
    public valueOffsets: Int32Array;
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

export class StructView extends NestedView<Struct> {
    public getNested(view: StructView, index: number) {
        return new RowView(view as any, index);
    }
}

export class MapView extends NestedView<Map_> {
    // @ts-ignore
    public typeIds: { [k: string]: number };
    constructor(data: Data<Map_>) {
        super(data);
        this.typeIds = data.type.children.reduce((xs, x, i) =>
            (xs[x.name] = i) && xs || xs, Object.create(null));
    }
    public getNested(view: MapView, index: number) {
        return new MapRowView(view as any, index);
    }
}

export class RowView extends UnionView<SparseUnion> {
    constructor(data: Data<SparseUnion> & NestedView<any>, protected rowIndex: number) {
        super(data);
        this.length = data.numChildren;
    }
    protected getUnionValue(view: NestedView<SparseUnion>, index: number, _typeIds: any, _valueOffsets?: any): any | null {
        const child = view.getChildAt(index);
        return child ? child.get(this.rowIndex) : null;
    }
}

export class MapRowView extends RowView {
    protected getUnionValue(view: NestedView<SparseUnion>, index: number, typeIds: any, _valueOffsets: any): any | null {
        const child = view.getChildAt(typeIds[index]);
        return child ? child.get(this.rowIndex) : null;
    }
}

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

import { Field } from '../schema';
import { Union, SparseUnion, DenseUnion } from '../type';
import { Builder, NestedBuilder, BuilderOptions } from './base';

export interface UnionBuilderOptions<T extends Union = any, TNull = any> extends BuilderOptions<T, TNull> {
    valueToChildTypeId?: (builder: Builder<T, TNull>, value: any, offset: number) => number;
}

export class UnionBuilder<T extends Union, TNull = any> extends NestedBuilder<T, TNull> {
    constructor(options: UnionBuilderOptions<T, TNull>) {
        super(options);
        this.typeIds = new Int8Array(0);
        if (typeof options['valueToChildTypeId'] === 'function') {
            this._valueToChildTypeId = options['valueToChildTypeId'];
        }
    }
    public get typeIdToChildIndex() { return this._type.typeIdToChildIndex; }
    public get bytesReserved() {
        return this.children.reduce(
            (acc, { bytesReserved }) => acc + bytesReserved,
            this.typeIds.byteLength + this.nullBitmap.byteLength
        );
    }
    /** @ignore */
    public set(offset: number, value: T['TValue'] | TNull, childTypeId?: number): void {
        if (childTypeId === undefined) {
            childTypeId = this._valueToChildTypeId(this, value, offset);
        }
        if (this.writeValid(this.isValid(value), offset)) {
            this.writeValue(value, offset, childTypeId);
        }
        const length = offset + 1;
        this.length = length;
        this._updateBytesUsed(offset, length);
    }
    public write(value: any | TNull, childTypeId?: number) {
        const offset = this.length;
        if (childTypeId === undefined) {
            childTypeId = this._valueToChildTypeId(this, value, offset);
        }
        if (this.writeValid(this.isValid(value), offset)) {
            this.writeValue(value, offset, childTypeId);
        }
        const length = offset + 1;
        this.length = length;
        return this._updateBytesUsed(offset, length);
    }
    public addChild(child: Builder, name = `${this.children.length}`): number {
        const childTypeId = this.children.push(child);
        const { type: { children, mode, typeIds } } = this;
        const fields = [...children, new Field(name, child.type)];
        this._type = new Union(mode, [...typeIds, childTypeId], fields) as T;
        return childTypeId;
    }
    /** @ignore */
    public writeValue(value: any, offset: number, childTypeId?: number) {
        this._getTypeIds(offset)[offset] = childTypeId!;
        return super.writeValue(value, offset);
    }
    /** @ignore */
    protected _updateBytesUsed(offset: number, length: number) {
        this._bytesUsed += 1;
        return super._updateBytesUsed(offset, length);
    }
    /** @ignore */
    // @ts-ignore
    protected _valueToChildTypeId(builder: Builder<T, TNull>, value: any, offset: number): number {
        throw new Error(`Cannot map UnionVector value to child typeId. \
Pass the \`childTypeId\` as the second argument to unionBuilder.write(), \
or supply a \`valueToChildTypeId\` function as part of the UnionBuilder constructor options.`);
    }
}

export class SparseUnionBuilder<T extends SparseUnion, TNull = any> extends UnionBuilder<T, TNull> {}

export class DenseUnionBuilder<T extends DenseUnion, TNull = any> extends UnionBuilder<T, TNull> {
    constructor(options: BuilderOptions<T, TNull>) {
        super(options);
        this.valueOffsets = new Int32Array(0);
    }
    /** @ignore */
    public writeValue(value: any, offset: number, childTypeId?: number) {
        const childIndex = this._type.typeIdToChildIndex[childTypeId!];
        this._getValueOffsets(offset)[offset] = this.getChildAt(childIndex)!.length;
        return super.writeValue(value, offset, childTypeId);
    }
    /** @ignore */
    protected _updateBytesUsed(offset: number, length: number) {
        this._bytesUsed += 4;
        return super._updateBytesUsed(offset, length);
    }
}

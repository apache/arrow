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

import { DataType, List } from '../type';
import { BuilderOptions, NestedBuilder } from './base';

export class ListBuilder<T extends DataType = any, TNull = any> extends NestedBuilder<List<T>, TNull> {
    private row = new RowLike<T, TNull>();
    constructor(options: BuilderOptions<List<T>, TNull>) {
        super(options);
        this.valueOffsets = new Int32Array(0);
    }
    public reset() {
        (this.row as any).values = null;
        return super.reset();
    }
    public writeValid(isValid: boolean, offset: number) {
        if (!super.writeValid(isValid, offset)) {
            const length = this.length;
            const valueOffsets = this._getValueOffsets(offset);
            (offset - length === 0)
                ? (valueOffsets[offset + 1] = valueOffsets[offset])
                : (valueOffsets.fill(valueOffsets[length], length, offset + 2));
        }
        return isValid;
    }
    public writeValue(value: any, offset: number) {
        const length = this.length;
        const valueOffsets = this._getValueOffsets(offset);
        if (length < offset) {
            valueOffsets.fill(valueOffsets[length], length, offset + 1);
        }
        valueOffsets[offset + 1] = valueOffsets[offset] + value.length;
        const row = this.row;
        row.values = value;
        super.writeValue(row as any, offset);
    }
}

class RowLike<T extends DataType = any, TNull = any> {
    // @ts-ignore
    public values: ArrayLike<T['TValue'] | TNull>;
    public get length() { return this.values.length; }
    public get(index: number) { return this.values[index]; }
}

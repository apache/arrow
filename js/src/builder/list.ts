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

import { Row } from './row';
import { Field } from '../schema';
import { DataType, List } from '../type';
import { OffsetsBufferBuilder } from './buffer';
import { Builder, BuilderOptions, VariableWidthBuilder } from './base';

export class ListBuilder<T extends DataType = any, TNull = any> extends Builder<List<T>, TNull> {
    protected _row = new Row<T, TNull>();
    protected _offsets: OffsetsBufferBuilder;
    constructor(opts: BuilderOptions<List<T>, TNull>) {
        super(opts);
        this._offsets = new OffsetsBufferBuilder();
    }
    public setValue(index: number, value: T['TValue']) {
        this._offsets.set(index, value.length);
        super.setValue(index, this._row.bind(value));
    }
    public addChild(child: Builder<T>, name = '0') {
        if (this.numChildren > 0) {
            throw new Error('ListBuilder can only have one child.');
        }
        this.children[this.numChildren] = child;
        this.type = new List(new Field(name, child.type, true));
        return this.numChildren - 1;
    }
    public clear() {
        this._row.clear();
        return super.clear();
    }
}

ListBuilder.prototype.setValid = VariableWidthBuilder.prototype.setValid;

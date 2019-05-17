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

import { DataType, FixedSizeList } from '../type';
import { NestedBuilder } from './base';

export class FixedSizeListBuilder<T extends DataType = any, TNull = any> extends NestedBuilder<FixedSizeList<T>, TNull> {
    private row = new RowLike<T, TNull>();
    public writeValue(value: any, offset: number) {
        const row = this.row;
        row.values = value;
        super.writeValue(row as any, offset);
        row.values = null;
    }
}

class RowLike<T extends DataType = any, TNull = any> {
    public values: null | ArrayLike<T['TValue'] | TNull> = null;
    get(index: number) {
        return this.values ? this.values[index] : null;
    }
}

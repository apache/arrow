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
import { Vector } from '../vector';
import { BaseVector } from './base';
import { RowProxyGenerator } from './row';
import { DataType, Map_, Struct } from '../type';

export class StructVector<T extends { [key: string]: DataType } = any> extends BaseVector<Struct<T>> {
    public asMap(keysSorted: boolean = false) {
        return Vector.new(this.data.clone(new Map_<T>(this.type.children as Field<T[keyof T]>[], keysSorted)));
    }
    // @ts-ignore
    private _rowProxy: RowProxyGenerator<T>;
    public get rowProxy(): RowProxyGenerator<T> {
        return this._rowProxy || (this._rowProxy = RowProxyGenerator.new<T>(this, this.type.children || [], false));
    }
}

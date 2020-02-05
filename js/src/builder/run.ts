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

import { Vector } from '../vector';
import { DataType } from '../type';

/** @ignore */
export class Run<T extends DataType = any, TNull = any> {
    // @ts-ignore
    protected _values: ArrayLike<T['TValue'] | TNull>;
    public get length() { return this._values.length; }
    public get(index: number) { return this._values[index]; }
    public clear() { this._values = <any> null; return this; }
    public bind(values: Vector<T> | ArrayLike<T['TValue'] | TNull>) {
        if (values instanceof Vector) {
            return values;
        }
        this._values = values;
        return this as any;
    }
}

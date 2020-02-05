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
import { Chunked } from './chunked';
import { BaseVector } from './base';
import { Binary, Utf8 } from '../type';
import { VectorBuilderOptions } from './index';
import { vectorFromValuesWithType } from './index';
import { VectorBuilderOptionsAsync } from './index';

/** @ignore */
export class Utf8Vector extends BaseVector<Utf8> {
    public static from<TNull = any>(input: Iterable<string | TNull>): Utf8Vector;
    public static from<TNull = any>(input: AsyncIterable<string | TNull>): Promise<Utf8Vector>;
    public static from<TNull = any>(input: VectorBuilderOptions<Utf8, TNull>): Chunked<Utf8>;
    public static from<TNull = any>(input: VectorBuilderOptionsAsync<Utf8, TNull>): Promise<Chunked<Utf8>>;
    /** @nocollapse */
    public static from<TNull = any>(input: Iterable<string | TNull> | AsyncIterable<string | TNull> | VectorBuilderOptions<Utf8, TNull> | VectorBuilderOptionsAsync<Utf8, TNull>) {
        return vectorFromValuesWithType(() => new Utf8(), input);
    }
    public asBinary() {
        return Vector.new(this.data.clone(new Binary()));
    }
}

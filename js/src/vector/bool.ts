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

import { Bool } from '../type';
import { Chunked } from './chunked';
import { BaseVector } from './base';
import { VectorBuilderOptions } from './index';
import { vectorFromValuesWithType } from './index';
import { VectorBuilderOptionsAsync } from './index';

/** @ignore */
export class BoolVector extends BaseVector<Bool> {
    public static from<TNull = any>(input: Iterable<boolean | TNull>): BoolVector;
    public static from<TNull = any>(input: AsyncIterable<boolean | TNull>): Promise<BoolVector>;
    public static from<TNull = any>(input: VectorBuilderOptions<Bool, TNull>): Chunked<Bool>;
    public static from<TNull = any>(input: VectorBuilderOptionsAsync<Bool, TNull>): Promise<Chunked<Bool>>;
    /** @nocollapse */
    public static from<TNull = any>(input: Iterable<boolean | TNull> | AsyncIterable<boolean | TNull> | VectorBuilderOptions<Bool, TNull> | VectorBuilderOptionsAsync<Bool, TNull>) {
        return vectorFromValuesWithType(() => new Bool(), input);
    }
}

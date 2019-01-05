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
import { Vector } from '../vector';
import { BaseVector } from './base';
import { Binary, Utf8 } from '../type';
import { encodeUtf8 } from '../util/utf8';

export class Utf8Vector extends BaseVector<Utf8> {
    /** @nocollapse */
    public static from(values: string[]) {
        const length = values.length;
        const data = encodeUtf8(values.join(''));
        const offsets = values.reduce((offsets, str, idx) => (
            (!(offsets[idx + 1] = offsets[idx] + str.length) || true) && offsets
        ), new Uint32Array(values.length + 1));
        return Vector.new(Data.Utf8(new Utf8(), 0, length, 0, null, offsets, data));
    }
    public asBinary() {
        return Vector.new(this.data.clone(new Binary()));
    }
}

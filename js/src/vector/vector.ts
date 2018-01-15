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

import * as Schema_ from '../format/fb/Schema';
import Type = Schema_.org.apache.arrow.flatbuf.Type;

export interface Vector<T = any> extends Iterable<T | null> {
    readonly name: string;
    readonly type: string;
    readonly length: number;
    readonly nullable: boolean;
    readonly nullCount: number;
    readonly metadata: Map<string, string>;
    get(index: number): T | null;
    concat(...vectors: Vector<T>[]): Vector<T>;
    slice<R = T[]>(start?: number, end?: number): R;
}

export class Vector<T = any> implements Vector<T> {
    slice<R = T[]>(start?: number, end?: number): R {
        let { length } = this, from = start! | 0;
        let to = end === undefined ? length : Math.max(end | 0, from);
        let result = new Array<T | null>(to - Math.min(from, to));
        for (let i = -1, n = result.length; ++i < n;) {
            result[i] = this.get(i + from);
        }
        return result as any;
    }
    *[Symbol.iterator]() {
        for (let i = -1, n = this.length; ++i < n;) {
            yield this.get(i);
        }
    }
}

(Vector.prototype as any).name = '';
(Vector.prototype as any).stride = 1;
(Vector.prototype as any).nullable = !1;
(Vector.prototype as any).nullCount = 0;
(Vector.prototype as any).metadata = new Map();
(Vector.prototype as any).type = Type[Type.NONE];

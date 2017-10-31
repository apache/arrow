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

import { TypedVector } from './typed';

export class DateVector extends TypedVector<Date, Uint32Array> {
    get(index: number): Date {
        return new Date(4294967296   * /* 2^32 */
            (super.get(index * 2 + 1) as any) + /* high */
            (super.get(index * 2) as any)       /*  low */
        );
    }
}

(DateVector.prototype as any).stride = 2;

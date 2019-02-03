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
import { Float, Float16, Float32, Float64 } from '../type';

export class FloatVector<T extends Float = Float> extends BaseVector<T> {
    /** @nocollapse */
    public static from<T extends Float>(data: T['TArray']) {
        switch (data.constructor) {
            case Uint16Array: return Vector.new(Data.Float(new Float16(), 0, data.length, 0, null, data));
            case Float32Array: return Vector.new(Data.Float(new Float32(), 0, data.length, 0, null, data));
            case Float64Array: return Vector.new(Data.Float(new Float64(), 0, data.length, 0, null, data));
        }
        throw new TypeError('Unrecognized Float data');
    }
}

export class Float16Vector extends FloatVector<Float16> {}
export class Float32Vector extends FloatVector<Float32> {}
export class Float64Vector extends FloatVector<Float64> {}

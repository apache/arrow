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

import { FlatBuilder } from './base';
import { Float, Float16, Float32, Float64 } from '../type';

export interface FloatBuilder<T extends Float = Float, TNull = any>extends FlatBuilder<T, TNull> {
    nullBitmap: Uint8Array; values: T['TArray'];
}

export interface Float16Builder<TNull = any> extends FloatBuilder<Float16, TNull> {}
export interface Float32Builder<TNull = any> extends FloatBuilder<Float32, TNull> {}
export interface Float64Builder<TNull = any> extends FloatBuilder<Float64, TNull> {}

export class FloatBuilder<T extends Float = Float, TNull = any> extends FlatBuilder<T, TNull> {}
export class Float16Builder<TNull = any> extends FloatBuilder<Float16, TNull> {}
export class Float32Builder<TNull = any> extends FloatBuilder<Float32, TNull> {}
export class Float64Builder<TNull = any> extends FloatBuilder<Float64, TNull> {}

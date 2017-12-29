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
import * as vectors from './vectors';
import { nullableMixin } from './mixins';

export { Vector };
export const NullableListVector = nullableMixin(vectors.ListVector);
export class ListVector extends NullableListVector {}
export const NullableBinaryVector = nullableMixin(vectors.BinaryVector);
export class BinaryVector extends NullableBinaryVector {}
export const NullableUtf8Vector = nullableMixin(vectors.Utf8Vector);
export class Utf8Vector extends NullableUtf8Vector {}
export const NullableBoolVector = nullableMixin(vectors.BoolVector);
export class BoolVector extends NullableBoolVector {}
export const NullableInt8Vector = nullableMixin(vectors.Int8Vector);
export class Int8Vector extends NullableInt8Vector {}
export const NullableInt16Vector = nullableMixin(vectors.Int16Vector);
export class Int16Vector extends NullableInt16Vector {}
export const NullableInt32Vector = nullableMixin(vectors.Int32Vector);
export class Int32Vector extends NullableInt32Vector {}
export const NullableInt64Vector = nullableMixin(vectors.Int64Vector);
export class Int64Vector extends NullableInt64Vector {}
export const NullableUint8Vector = nullableMixin(vectors.Uint8Vector);
export class Uint8Vector extends NullableUint8Vector {}
export const NullableUint16Vector = nullableMixin(vectors.Uint16Vector);
export class Uint16Vector extends NullableUint16Vector {}
export const NullableUint32Vector = nullableMixin(vectors.Uint32Vector);
export class Uint32Vector extends NullableUint32Vector {}
export const NullableUint64Vector = nullableMixin(vectors.Uint64Vector);
export class Uint64Vector extends NullableUint64Vector {}
export const NullableDate32Vector = nullableMixin(vectors.Date32Vector);
export class Date32Vector extends NullableDate32Vector {}
export const NullableDate64Vector = nullableMixin(vectors.Date64Vector);
export class Date64Vector extends NullableDate64Vector {}
export const NullableTime32Vector = nullableMixin(vectors.Time32Vector);
export class Time32Vector extends NullableTime32Vector {}
export const NullableTime64Vector = nullableMixin(vectors.Time64Vector);
export class Time64Vector extends NullableTime64Vector {}
export const NullableFloat16Vector = nullableMixin(vectors.Float16Vector);
export class Float16Vector extends NullableFloat16Vector {}
export const NullableFloat32Vector = nullableMixin(vectors.Float32Vector);
export class Float32Vector extends NullableFloat32Vector {}
export const NullableFloat64Vector = nullableMixin(vectors.Float64Vector);
export class Float64Vector extends NullableFloat64Vector {}
export const NullableStructVector = nullableMixin(vectors.StructVector);
export class StructVector extends NullableStructVector {}
export const NullableDecimalVector = nullableMixin(vectors.DecimalVector);
export class DecimalVector extends NullableDecimalVector {}
export const NullableTimestampVector = nullableMixin(vectors.TimestampVector);
export class TimestampVector extends NullableTimestampVector {}
export const NullableDictionaryVector = nullableMixin(vectors.DictionaryVector);
export class DictionaryVector extends NullableDictionaryVector {}
export const NullableFixedSizeListVector = nullableMixin(vectors.FixedSizeListVector);
export class FixedSizeListVector extends NullableFixedSizeListVector {}
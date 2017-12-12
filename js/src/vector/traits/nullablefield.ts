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
import { nullableMixin, fieldMixin } from './mixins';
import { Field, FieldNode } from '../../format/arrow';
export { Vector, Field, FieldNode };

export const NullableFieldListVector = nullableMixin(fieldMixin(vectors.ListVector));
export class ListVector extends NullableFieldListVector {}
export const NullableFieldBinaryVector = nullableMixin(fieldMixin(vectors.BinaryVector));
export class BinaryVector extends NullableFieldBinaryVector {}
export const NullableFieldUtf8Vector = nullableMixin(fieldMixin(vectors.Utf8Vector));
export class Utf8Vector extends NullableFieldUtf8Vector {}
export const NullableFieldBoolVector = nullableMixin(fieldMixin(vectors.BoolVector));
export class BoolVector extends NullableFieldBoolVector {}
export const NullableFieldInt8Vector = nullableMixin(fieldMixin(vectors.Int8Vector));
export class Int8Vector extends NullableFieldInt8Vector {}
export const NullableFieldInt16Vector = nullableMixin(fieldMixin(vectors.Int16Vector));
export class Int16Vector extends NullableFieldInt16Vector {}
export const NullableFieldInt32Vector = nullableMixin(fieldMixin(vectors.Int32Vector));
export class Int32Vector extends NullableFieldInt32Vector {}
export const NullableFieldInt64Vector = nullableMixin(fieldMixin(vectors.Int64Vector));
export class Int64Vector extends NullableFieldInt64Vector {}
export const NullableFieldUint8Vector = nullableMixin(fieldMixin(vectors.Uint8Vector));
export class Uint8Vector extends NullableFieldUint8Vector {}
export const NullableFieldUint16Vector = nullableMixin(fieldMixin(vectors.Uint16Vector));
export class Uint16Vector extends NullableFieldUint16Vector {}
export const NullableFieldUint32Vector = nullableMixin(fieldMixin(vectors.Uint32Vector));
export class Uint32Vector extends NullableFieldUint32Vector {}
export const NullableFieldUint64Vector = nullableMixin(fieldMixin(vectors.Uint64Vector));
export class Uint64Vector extends NullableFieldUint64Vector {}
export const NullableFieldDate32Vector = nullableMixin(fieldMixin(vectors.Date32Vector));
export class Date32Vector extends NullableFieldDate32Vector {}
export const NullableFieldDate64Vector = nullableMixin(fieldMixin(vectors.Date64Vector));
export class Date64Vector extends NullableFieldDate64Vector {}
export const NullableFieldTime32Vector = nullableMixin(fieldMixin(vectors.Time32Vector));
export class Time32Vector extends NullableFieldTime32Vector {}
export const NullableFieldTime64Vector = nullableMixin(fieldMixin(vectors.Time64Vector));
export class Time64Vector extends NullableFieldTime64Vector {}
export const NullableFieldFloat16Vector = nullableMixin(fieldMixin(vectors.Float16Vector));
export class Float16Vector extends NullableFieldFloat16Vector {}
export const NullableFieldFloat32Vector = nullableMixin(fieldMixin(vectors.Float32Vector));
export class Float32Vector extends NullableFieldFloat32Vector {}
export const NullableFieldFloat64Vector = nullableMixin(fieldMixin(vectors.Float64Vector));
export class Float64Vector extends NullableFieldFloat64Vector {}
export const NullableFieldStructVector = nullableMixin(fieldMixin(vectors.StructVector));
export class StructVector extends NullableFieldStructVector {}
export const NullableFieldDecimalVector = nullableMixin(fieldMixin(vectors.DecimalVector));
export class DecimalVector extends NullableFieldDecimalVector {}
export const NullableFieldTimestampVector = nullableMixin(fieldMixin(vectors.TimestampVector));
export class TimestampVector extends NullableFieldTimestampVector {}
export const NullableFieldDictionaryVector = nullableMixin(fieldMixin(vectors.DictionaryVector));
export class DictionaryVector extends NullableFieldDictionaryVector {}
export const NullableFieldFixedSizeListVector = nullableMixin(fieldMixin(vectors.FixedSizeListVector));
export class FixedSizeListVector extends NullableFieldFixedSizeListVector {}
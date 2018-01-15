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

import { Vector } from './vector';
import * as vectors from './traits/vectors';
import * as fieldVectors from './traits/field';
import * as nullableVectors from './traits/nullable';
import * as nullableFieldVectors from './traits/nullablefield';
import { Field, FieldNode } from '../format/arrow';
import { isFieldArgv, isNullableArgv } from './traits/mixins';

function MixinArrowTraits<T extends Vector<any>, TArgv>(
    Base: new (argv: TArgv) => T,
    Field: new (argv: TArgv & { field: Field, fieldNode: FieldNode }) => T,
    Nullable: new (argv: TArgv & { validity: Uint8Array }) => T,
    NullableField: new (argv: TArgv & { validity: Uint8Array, field: Field, fieldNode: FieldNode }) => T
) {
    return function(argv: TArgv | (TArgv & { validity: Uint8Array }) | (TArgv & { field: Field, fieldNode: FieldNode })) {
        return new (!isFieldArgv(argv)
            ? !isNullableArgv(argv) ? Base : Nullable
            : !isNullableArgv(argv) ? Field : NullableField
        )(argv as any);
    } as any as { new (argv: TArgv | (TArgv & { validity: Uint8Array }) | (TArgv & { field: Field, fieldNode: FieldNode })): T };
}

export { Vector };
export const MixinListVector = MixinArrowTraits(vectors.ListVector as any, fieldVectors.ListVector as any, nullableVectors.ListVector as any, nullableFieldVectors.ListVector as any);
export class ListVector extends MixinListVector {}
export const MixinBinaryVector = MixinArrowTraits(vectors.BinaryVector as any, fieldVectors.BinaryVector as any, nullableVectors.BinaryVector as any, nullableFieldVectors.BinaryVector as any);
export class BinaryVector extends MixinBinaryVector {}
export const MixinUtf8Vector = MixinArrowTraits(vectors.Utf8Vector as any, fieldVectors.Utf8Vector as any, nullableVectors.Utf8Vector as any, nullableFieldVectors.Utf8Vector as any);
export class Utf8Vector extends MixinUtf8Vector {}
export const MixinBoolVector = MixinArrowTraits(vectors.BoolVector as any, fieldVectors.BoolVector as any, nullableVectors.BoolVector as any, nullableFieldVectors.BoolVector as any);
export class BoolVector extends MixinBoolVector {}
export const MixinInt8Vector = MixinArrowTraits(vectors.Int8Vector as any, fieldVectors.Int8Vector as any, nullableVectors.Int8Vector as any, nullableFieldVectors.Int8Vector as any);
export class Int8Vector extends MixinInt8Vector {}
export const MixinInt16Vector = MixinArrowTraits(vectors.Int16Vector as any, fieldVectors.Int16Vector as any, nullableVectors.Int16Vector as any, nullableFieldVectors.Int16Vector as any);
export class Int16Vector extends MixinInt16Vector {}
export const MixinInt32Vector = MixinArrowTraits(vectors.Int32Vector as any, fieldVectors.Int32Vector as any, nullableVectors.Int32Vector as any, nullableFieldVectors.Int32Vector as any);
export class Int32Vector extends MixinInt32Vector {}
export const MixinInt64Vector = MixinArrowTraits(vectors.Int64Vector as any, fieldVectors.Int64Vector as any, nullableVectors.Int64Vector as any, nullableFieldVectors.Int64Vector as any);
export class Int64Vector extends MixinInt64Vector {}
export const MixinUint8Vector = MixinArrowTraits(vectors.Uint8Vector as any, fieldVectors.Uint8Vector as any, nullableVectors.Uint8Vector as any, nullableFieldVectors.Uint8Vector as any);
export class Uint8Vector extends MixinUint8Vector {}
export const MixinUint16Vector = MixinArrowTraits(vectors.Uint16Vector as any, fieldVectors.Uint16Vector as any, nullableVectors.Uint16Vector as any, nullableFieldVectors.Uint16Vector as any);
export class Uint16Vector extends MixinUint16Vector {}
export const MixinUint32Vector = MixinArrowTraits(vectors.Uint32Vector as any, fieldVectors.Uint32Vector as any, nullableVectors.Uint32Vector as any, nullableFieldVectors.Uint32Vector as any);
export class Uint32Vector extends MixinUint32Vector {}
export const MixinUint64Vector = MixinArrowTraits(vectors.Uint64Vector as any, fieldVectors.Uint64Vector as any, nullableVectors.Uint64Vector as any, nullableFieldVectors.Uint64Vector as any);
export class Uint64Vector extends MixinUint64Vector {}
export const MixinDate32Vector = MixinArrowTraits(vectors.Date32Vector as any, fieldVectors.Date32Vector as any, nullableVectors.Date32Vector as any, nullableFieldVectors.Date32Vector as any);
export class Date32Vector extends MixinDate32Vector {}
export const MixinDate64Vector = MixinArrowTraits(vectors.Date64Vector as any, fieldVectors.Date64Vector as any, nullableVectors.Date64Vector as any, nullableFieldVectors.Date64Vector as any);
export class Date64Vector extends MixinDate64Vector {}
export const MixinTime32Vector = MixinArrowTraits(vectors.Time32Vector as any, fieldVectors.Time32Vector as any, nullableVectors.Time32Vector as any, nullableFieldVectors.Time32Vector as any);
export class Time32Vector extends MixinTime32Vector {}
export const MixinTime64Vector = MixinArrowTraits(vectors.Time64Vector as any, fieldVectors.Time64Vector as any, nullableVectors.Time64Vector as any, nullableFieldVectors.Time64Vector as any);
export class Time64Vector extends MixinTime64Vector {}
export const MixinFloat16Vector = MixinArrowTraits(vectors.Float16Vector as any, fieldVectors.Float16Vector as any, nullableVectors.Float16Vector as any, nullableFieldVectors.Float16Vector as any);
export class Float16Vector extends MixinFloat16Vector {}
export const MixinFloat32Vector = MixinArrowTraits(vectors.Float32Vector as any, fieldVectors.Float32Vector as any, nullableVectors.Float32Vector as any, nullableFieldVectors.Float32Vector as any);
export class Float32Vector extends MixinFloat32Vector {}
export const MixinFloat64Vector = MixinArrowTraits(vectors.Float64Vector as any, fieldVectors.Float64Vector as any, nullableVectors.Float64Vector as any, nullableFieldVectors.Float64Vector as any);
export class Float64Vector extends MixinFloat64Vector {}
export const MixinStructVector = MixinArrowTraits(vectors.StructVector as any, fieldVectors.StructVector as any, nullableVectors.StructVector as any, nullableFieldVectors.StructVector as any);
export class StructVector extends MixinStructVector {}
export const MixinDecimalVector = MixinArrowTraits(vectors.DecimalVector as any, fieldVectors.DecimalVector as any, nullableVectors.DecimalVector as any, nullableFieldVectors.DecimalVector as any);
export class DecimalVector extends MixinDecimalVector {}
export const MixinTimestampVector = MixinArrowTraits(vectors.TimestampVector as any, fieldVectors.TimestampVector as any, nullableVectors.TimestampVector as any, nullableFieldVectors.TimestampVector as any);
export class TimestampVector extends MixinTimestampVector {}
export const MixinDictionaryVector = MixinArrowTraits(vectors.DictionaryVector as any, fieldVectors.DictionaryVector as any, nullableVectors.DictionaryVector as any, nullableFieldVectors.DictionaryVector as any);
export class DictionaryVector extends MixinDictionaryVector {}
export const MixinFixedSizeListVector = MixinArrowTraits(vectors.FixedSizeListVector as any, fieldVectors.FixedSizeListVector as any, nullableVectors.FixedSizeListVector as any, nullableFieldVectors.FixedSizeListVector as any);
export class FixedSizeListVector extends MixinFixedSizeListVector {}

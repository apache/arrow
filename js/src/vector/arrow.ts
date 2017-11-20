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

import * as Schema_ from '../format/Schema';
import * as Message_ from '../format/Message';
import Field = Schema_.org.apache.arrow.flatbuf.Field;
import FieldNode = Message_.org.apache.arrow.flatbuf.FieldNode;

import { Vector } from './vector';
import { Utf8Vector as Utf8VectorBase } from './utf8';
import { StructVector as StructVectorBase } from './struct';
import { DictionaryVector as DictionaryVectorBase } from './dictionary';
import {
    ListVector as ListVectorBase,
    BinaryVector as BinaryVectorBase,
    FixedSizeListVector as FixedSizeListVectorBase
} from './list';

import {
    BoolVector as BoolVectorBase,
    Int8Vector as Int8VectorBase,
    Int16Vector as Int16VectorBase,
    Int32Vector as Int32VectorBase,
    Int64Vector as Int64VectorBase,
    Uint8Vector as Uint8VectorBase,
    Uint16Vector as Uint16VectorBase,
    Uint32Vector as Uint32VectorBase,
    Uint64Vector as Uint64VectorBase,
    Float16Vector as Float16VectorBase,
    Float32Vector as Float32VectorBase,
    Float64Vector as Float64VectorBase,
    Date32Vector as Date32VectorBase,
    Date64Vector as Date64VectorBase,
    Time32Vector as Time32VectorBase,
    Time64Vector as Time64VectorBase,
    DecimalVector as DecimalVectorBase,
    TimestampVector as TimestampVectorBase,
} from './numeric';

import { nullableMixin, fieldMixin } from './traits';

function MixinArrowTraits<T extends Vector<any>, TArgv>(
    Base: new (argv: TArgv) => T,
    Field: new (argv: TArgv & { field: Field, fieldNode: FieldNode }) => T,
    Nullable: new (argv: TArgv & { validity: Uint8Array }) => T,
    NullableField: new (argv: TArgv & { validity: Uint8Array, field: Field, fieldNode: FieldNode }) => T,
) {
    return function(argv: TArgv | (TArgv & { validity: Uint8Array }) | (TArgv & { field: Field, fieldNode: FieldNode })) {
        return new (!isFieldArgv(argv)
            ? !isNullableArgv(argv) ? Base : Nullable
            : !isNullableArgv(argv) ? Field : NullableField
        )(argv as any);
    } as any as { new (argv: TArgv | (TArgv & { validity: Uint8Array }) | (TArgv & { field: Field, fieldNode: FieldNode })): T };
}

function isFieldArgv(x: any): x is { field: Field, fieldNode: FieldNode } {
    return x && x.field instanceof Field && x.fieldNode instanceof FieldNode;
}

function isNullableArgv(x: any): x is { validity: Uint8Array } {
    return x && x.validity && ArrayBuffer.isView(x.validity) && x.validity instanceof Uint8Array;
}

export { Vector };
export class ListVector extends MixinArrowTraits(
    ListVectorBase,
    class ListVector extends fieldMixin(ListVectorBase) {} as any,
    class ListVector extends nullableMixin(ListVectorBase) {} as any,
    class ListVector extends nullableMixin(fieldMixin(ListVectorBase)) {} as any
) {}

export class BinaryVector extends MixinArrowTraits(
    BinaryVectorBase,
    class BinaryVector extends fieldMixin(BinaryVectorBase) {} as any,
    class BinaryVector extends nullableMixin(BinaryVectorBase) {} as any,
    class BinaryVector extends nullableMixin(fieldMixin(BinaryVectorBase)) {} as any
) {}

export class Utf8Vector extends MixinArrowTraits(
    Utf8VectorBase,
    class Utf8Vector extends fieldMixin(Utf8VectorBase) {} as any,
    class Utf8Vector extends nullableMixin(Utf8VectorBase) {} as any,
    class Utf8Vector extends nullableMixin(fieldMixin(Utf8VectorBase)) {} as any
) {}

export class BoolVector extends MixinArrowTraits(
    BoolVectorBase,
    class BoolVector extends fieldMixin(BoolVectorBase) {} as any,
    class BoolVector extends nullableMixin(BoolVectorBase) {} as any,
    class BoolVector extends nullableMixin(fieldMixin(BoolVectorBase)) {} as any
) {}

export class Int8Vector extends MixinArrowTraits(
    Int8VectorBase,
    class Int8Vector extends fieldMixin(Int8VectorBase) {} as any,
    class Int8Vector extends nullableMixin(Int8VectorBase) {} as any,
    class Int8Vector extends nullableMixin(fieldMixin(Int8VectorBase)) {} as any
) {}

export class Int16Vector extends MixinArrowTraits(
    Int16VectorBase,
    class Int16Vector extends fieldMixin(Int16VectorBase) {} as any,
    class Int16Vector extends nullableMixin(Int16VectorBase) {} as any,
    class Int16Vector extends nullableMixin(fieldMixin(Int16VectorBase)) {} as any
) {}

export class Int32Vector extends MixinArrowTraits(
    Int32VectorBase,
    class Int32Vector extends fieldMixin(Int32VectorBase) {} as any,
    class Int32Vector extends nullableMixin(Int32VectorBase) {} as any,
    class Int32Vector extends nullableMixin(fieldMixin(Int32VectorBase)) {} as any
) {}

export class Int64Vector extends MixinArrowTraits(
    Int64VectorBase,
    class Int64Vector extends fieldMixin(Int64VectorBase) {} as any,
    class Int64Vector extends nullableMixin(Int64VectorBase) {} as any,
    class Int64Vector extends nullableMixin(fieldMixin(Int64VectorBase)) {} as any
) {}

export class Uint8Vector extends MixinArrowTraits(
    Uint8VectorBase,
    class Uint8Vector extends fieldMixin(Uint8VectorBase) {} as any,
    class Uint8Vector extends nullableMixin(Uint8VectorBase) {} as any,
    class Uint8Vector extends nullableMixin(fieldMixin(Uint8VectorBase)) {} as any
) {}

export class Uint16Vector extends MixinArrowTraits(
    Uint16VectorBase,
    class Uint16Vector extends fieldMixin(Uint16VectorBase) {} as any,
    class Uint16Vector extends nullableMixin(Uint16VectorBase) {} as any,
    class Uint16Vector extends nullableMixin(fieldMixin(Uint16VectorBase)) {} as any
) {}

export class Uint32Vector extends MixinArrowTraits(
    Uint32VectorBase,
    class Uint32Vector extends fieldMixin(Uint32VectorBase) {} as any,
    class Uint32Vector extends nullableMixin(Uint32VectorBase) {} as any,
    class Uint32Vector extends nullableMixin(fieldMixin(Uint32VectorBase)) {} as any
) {}

export class Uint64Vector extends MixinArrowTraits(
    Uint64VectorBase,
    class Uint64Vector extends fieldMixin(Uint64VectorBase) {} as any,
    class Uint64Vector extends nullableMixin(Uint64VectorBase) {} as any,
    class Uint64Vector extends nullableMixin(fieldMixin(Uint64VectorBase)) {} as any
) {}

export class Date32Vector extends MixinArrowTraits(
    Date32VectorBase,
    class Date32Vector extends fieldMixin(Date32VectorBase) {} as any,
    class Date32Vector extends nullableMixin(Date32VectorBase) {} as any,
    class Date32Vector extends nullableMixin(fieldMixin(Date32VectorBase)) {} as any
) {}

export class Date64Vector extends MixinArrowTraits(
    Date64VectorBase,
    class Date64Vector extends fieldMixin(Date64VectorBase) {} as any,
    class Date64Vector extends nullableMixin(Date64VectorBase) {} as any,
    class Date64Vector extends nullableMixin(fieldMixin(Date64VectorBase)) {} as any
) {}

export class Time32Vector extends MixinArrowTraits(
    Time32VectorBase,
    class Time32Vector extends fieldMixin(Time32VectorBase) {} as any,
    class Time32Vector extends nullableMixin(Time32VectorBase) {} as any,
    class Time32Vector extends nullableMixin(fieldMixin(Time32VectorBase)) {} as any
) {}

export class Time64Vector extends MixinArrowTraits(
    Time64VectorBase,
    class Time64Vector extends fieldMixin(Time64VectorBase) {} as any,
    class Time64Vector extends nullableMixin(Time64VectorBase) {} as any,
    class Time64Vector extends nullableMixin(fieldMixin(Time64VectorBase)) {} as any
) {}

export class Float16Vector extends MixinArrowTraits(
    Float16VectorBase,
    class Float16Vector extends fieldMixin(Float16VectorBase) {} as any,
    class Float16Vector extends nullableMixin(Float16VectorBase) {} as any,
    class Float16Vector extends nullableMixin(fieldMixin(Float16VectorBase)) {} as any
) {}

export class Float32Vector extends MixinArrowTraits(
    Float32VectorBase,
    class Float32Vector extends fieldMixin(Float32VectorBase) {} as any,
    class Float32Vector extends nullableMixin(Float32VectorBase) {} as any,
    class Float32Vector extends nullableMixin(fieldMixin(Float32VectorBase)) {} as any
) {}

export class Float64Vector extends MixinArrowTraits(
    Float64VectorBase,
    class Float64Vector extends fieldMixin(Float64VectorBase) {} as any,
    class Float64Vector extends nullableMixin(Float64VectorBase) {} as any,
    class Float64Vector extends nullableMixin(fieldMixin(Float64VectorBase)) {} as any
) {}

export class StructVector extends MixinArrowTraits(
    StructVectorBase,
    class StructVector extends fieldMixin(StructVectorBase) {} as any,
    class StructVector extends nullableMixin(StructVectorBase) {} as any,
    class StructVector extends nullableMixin(fieldMixin(StructVectorBase)) {} as any
) {}

export class DecimalVector extends MixinArrowTraits(
    DecimalVectorBase,
    class DecimalVector extends fieldMixin(DecimalVectorBase) {} as any,
    class DecimalVector extends nullableMixin(DecimalVectorBase) {} as any,
    class DecimalVector extends nullableMixin(fieldMixin(DecimalVectorBase)) {} as any
) {}

export class TimestampVector extends MixinArrowTraits(
    TimestampVectorBase,
    class TimestampVector extends fieldMixin(TimestampVectorBase) {} as any,
    class TimestampVector extends nullableMixin(TimestampVectorBase) {} as any,
    class TimestampVector extends nullableMixin(fieldMixin(TimestampVectorBase)) {} as any
) {}

export class DictionaryVector extends MixinArrowTraits(
    DictionaryVectorBase,
    class DictionaryVector extends fieldMixin(DictionaryVectorBase) {} as any,
    class DictionaryVector extends nullableMixin(DictionaryVectorBase) {} as any,
    class DictionaryVector extends nullableMixin(fieldMixin(DictionaryVectorBase)) {} as any
) {}

export class FixedSizeListVector extends MixinArrowTraits(
    FixedSizeListVectorBase,
    class FixedSizeListVector extends fieldMixin(FixedSizeListVectorBase) {} as any,
    class FixedSizeListVector extends nullableMixin(FixedSizeListVectorBase) {} as any,
    class FixedSizeListVector extends nullableMixin(fieldMixin(FixedSizeListVectorBase)) {} as any
) {}

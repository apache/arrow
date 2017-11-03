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

import * as Schema_ from '../format/Schema_generated';
import * as Message_ from '../format/Message_generated';
import Field = Schema_.org.apache.arrow.flatbuf.Field;
import FieldNode = Message_.org.apache.arrow.flatbuf.FieldNode;

import { BoolVector } from './vector/bool';
import { DictionaryVector } from './dictionary';
import { nullableMixin, fieldMixin } from './vector/traits';
import { ListVector as ListVectorBase } from './list';
import { Utf8Vector as Utf8VectorBase } from './utf8';
import { Vector, Column, TypedArray } from './types';
import { DateVector as DateVectorBase } from './vector/date';
import { TableVector as TableVectorBase } from './table/table';
import { StructVector as StructVectorBase } from './table/struct';
import { FixedSizeListVector as FixedSizeListVectorBase } from './fixedsizelist';
import {
    LongVector as LongVectorBase,
    Int64Vector as Int64VectorBase,
    Uint64Vector as Uint64VectorBase,
} from './vector/long';
import {
    TypedVector,
    Int8Vector as Int8VectorBase,
    Int16Vector as Int16VectorBase,
    Int32Vector as Int32VectorBase,
    Uint8Vector as Uint8VectorBase,
    Uint16Vector as Uint16VectorBase,
    Uint32Vector as Uint32VectorBase,
    Float32Vector as Float32VectorBase,
    Float64Vector as Float64VectorBase,
} from './vector/typed';

export { TypedArray, TypedVector };
export { Column, BoolVector, DictionaryVector };
export class ListVector extends MixinArrowTraits(ListVectorBase) {}
export class Utf8Vector extends MixinArrowTraits(Utf8VectorBase) {}
export class TableVector extends MixinArrowTraits(TableVectorBase) {}
export class StructVector extends MixinArrowTraits(StructVectorBase) {}
export class FixedSizeListVector extends MixinArrowTraits(FixedSizeListVectorBase) {}
export class DateVector extends MixinArrowTraits(DateVectorBase) {}
export class LongVector extends MixinArrowTraits(LongVectorBase) {}
export class Int8Vector extends MixinArrowTraits(Int8VectorBase) {}
export class Int16Vector extends MixinArrowTraits(Int16VectorBase) {}
export class Int32Vector extends MixinArrowTraits(Int32VectorBase) {}
export class Int64Vector extends MixinArrowTraits(Int64VectorBase) {}
export class Uint8Vector extends MixinArrowTraits(Uint8VectorBase) {}
export class Uint16Vector extends MixinArrowTraits(Uint16VectorBase) {}
export class Uint32Vector extends MixinArrowTraits(Uint32VectorBase) {}
export class Uint64Vector extends MixinArrowTraits(Uint64VectorBase) {}
export class Float32Vector extends MixinArrowTraits(Float32VectorBase) {}
export class Float64Vector extends MixinArrowTraits(Float64VectorBase) {}

export function MixinArrowTraits<T extends Vector<any>, TArgv>(BaseVector: new (argv: TArgv) => T) {
    const FieldVector = fieldMixin(BaseVector);
    const NullableVector = nullableMixin(BaseVector);
    const NullableFieldVector = nullableMixin(FieldVector);
    return function(this: any, argv: TArgv & (object | { validity: Uint8Array } | { field: Field, fieldNode: FieldNode })) {
        return new ((!isFieldArgv(argv) ? !isNullableArgv(argv) ?
            BaseVector : NullableVector : !isNullableArgv(argv) ?
            FieldVector : NullableFieldVector
        ) as any)(argv);
    } as any as { new (argv: TArgv & (object | { validity: Uint8Array } | { field: Field, fieldNode: FieldNode })): T };
}

function isFieldArgv(x: any): x is { field: Field, fieldNode: FieldNode } {
    return x && x.field instanceof Field && x.fieldNode instanceof FieldNode;
}

function isNullableArgv(x: any): x is { validity: Uint8Array } {
    return x && x.validity && ArrayBuffer.isView(x.validity) && x.validity instanceof Uint8Array;
}

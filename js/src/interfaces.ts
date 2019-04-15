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

import { Data } from './data';
import { Type } from './enum';
import * as type from './type';
import { DataType } from './type';
import * as vecs from './vector/index';
import * as builders from './builder/index';
import { DataBuilderOptions } from './builder/base';

/** @ignore */ type FloatArray = Float32Array | Float64Array;
/** @ignore */ type IntArray = Int8Array | Int16Array | Int32Array;
/** @ignore */ type UintArray = Uint8Array | Uint16Array | Uint32Array | Uint8ClampedArray;
/** @ignore */
export type TypedArray = FloatArray | IntArray | UintArray;
export type BigIntArray = BigInt64Array | BigUint64Array;

/** @ignore */
export interface TypedArrayConstructor<T extends TypedArray> {
    readonly prototype: T;
    new(length?: number): T;
    new(array: Iterable<number>): T;
    new(buffer: ArrayBufferLike, byteOffset?: number, length?: number): T;
    /**
      * The size in bytes of each element in the array.
      */
    readonly BYTES_PER_ELEMENT: number;
    /**
      * Returns a new array from a set of elements.
      * @param items A set of elements to include in the new array object.
      */
    of(...items: number[]): T;
    /**
      * Creates an array from an array-like or iterable object.
      * @param arrayLike An array-like or iterable object to convert to an array.
      * @param mapfn A mapping function to call on every element of the array.
      * @param thisArg Value of 'this' used to invoke the mapfn.
      */
    from(arrayLike: ArrayLike<number>, mapfn?: (v: number, k: number) => number, thisArg?: any): T;
    from<U>(arrayLike: ArrayLike<U>, mapfn: (v: U, k: number) => number, thisArg?: any): T;
}

/** @ignore */
export interface BigIntArrayConstructor<T extends BigIntArray> {
    readonly prototype: T;
    new(length?: number): T;
    new(array: Iterable<bigint>): T;
    new(buffer: ArrayBufferLike, byteOffset?: number, length?: number): T;
    /**
      * The size in bytes of each element in the array.
      */
    readonly BYTES_PER_ELEMENT: number;
    /**
      * Returns a new array from a set of elements.
      * @param items A set of elements to include in the new array object.
      */
    of(...items: bigint[]): T;
    /**
      * Creates an array from an array-like or iterable object.
      * @param arrayLike An array-like or iterable object to convert to an array.
      * @param mapfn A mapping function to call on every element of the array.
      * @param thisArg Value of 'this' used to invoke the mapfn.
      */
    from(arrayLike: ArrayLike<bigint>, mapfn?: (v: bigint, k: number) => bigint, thisArg?: any): T;
    from<U>(arrayLike: ArrayLike<U>, mapfn: (v: U, k: number) => bigint, thisArg?: any): T;
}

/** @ignore */
export type VectorCtorArgs<
    T extends Vector<R>,
    R extends DataType = any,
    TArgs extends any[] = any[],
    TCtor extends new (data: Data<R>, ...args: TArgs) => T =
                  new (data: Data<R>, ...args: TArgs) => T
> = TCtor extends new (data: Data<R>, ...args: infer TArgs) => T ? TArgs : never;

/** @ignore */
export type BuilderCtorArgs<
    T extends Builder<R, any>,
    R extends DataType = any,
    TArgs extends any[] = any[],
    TCtor extends new (type: R, ...args: TArgs) => T =
                  new (type: R, ...args: TArgs) => T
> = TCtor extends new (type: R, ...args: infer TArgs) => T ? TArgs : never;

/**
 * Obtain the constructor function of an instance type
 * @ignore
 */
export type ConstructorType<
    T,
    TCtor extends new (...args: any[]) => T =
                  new (...args: any[]) => T
> = TCtor extends new (...args: any[]) => T ? TCtor : never;

/** @ignore */
export type VectorCtorType<
    T extends Vector<R>,
    R extends DataType = any,
    TCtor extends new (data: Data<R>, ...args: VectorCtorArgs<T, R>) => T =
                  new (data: Data<R>, ...args: VectorCtorArgs<T, R>) => T
> = TCtor extends new (data: Data<R>, ...args: VectorCtorArgs<T, R>) => T ? TCtor : never;

/** @ignore */
export type BuilderCtorType<
    T extends Builder<R, any>,
    R extends DataType = any,
    TCtor extends new (options: DataBuilderOptions<R, any>) => T =
                  new (options: DataBuilderOptions<R, any>) => T
> = TCtor extends new (options: DataBuilderOptions<R, any>) => T ? TCtor : never;

/** @ignore */
export type Vector<T extends Type | DataType = any> =
    T extends Type          ? TypeToVector<T>     :
    T extends DataType      ? DataTypeToVector<T> :
                              vecs.BaseVector<any>
    ;

/** @ignore */
export type Builder<T extends Type | DataType = any, TNull = any> =
    T extends Type          ? TypeToBuilder<T, TNull>         :
    T extends DataType      ? DataTypeToBuilder<T, TNull>     :
                              builders.Builder<any, TNull>
    ;

/** @ignore */
export type VectorCtor<T extends Type | DataType | Vector> =
    T extends Vector        ? VectorCtorType<T>                  :
    T extends Type          ? VectorCtorType<Vector<T>>          :
    T extends DataType      ? VectorCtorType<Vector<T['TType']>> :
                              VectorCtorType<vecs.BaseVector>
    ;

/** @ignore */
export type BuilderCtor<T extends Type | DataType = any> =
    T extends Type          ? BuilderCtorType<Builder<T>> :
    T extends DataType      ? BuilderCtorType<Builder<T>> :
                              BuilderCtorType<builders.Builder>
    ;

/** @ignore */
export type DataTypeCtor<T extends Type | DataType | Vector = any> =
    T extends DataType      ? ConstructorType<T>                 :
    T extends Vector        ? ConstructorType<T['type']>         :
    T extends Type          ? ConstructorType<TypeToDataType<T>> :
                              never
    ;

/** @ignore */
type TypeToVector<T extends Type> =
    T extends Type.Null                 ? vecs.NullVector                 :
    T extends Type.Bool                 ? vecs.BoolVector                 :
    T extends Type.Int8                 ? vecs.Int8Vector                 :
    T extends Type.Int16                ? vecs.Int16Vector                :
    T extends Type.Int32                ? vecs.Int32Vector                :
    T extends Type.Int64                ? vecs.Int64Vector                :
    T extends Type.Uint8                ? vecs.Uint8Vector                :
    T extends Type.Uint16               ? vecs.Uint16Vector               :
    T extends Type.Uint32               ? vecs.Uint32Vector               :
    T extends Type.Uint64               ? vecs.Uint64Vector               :
    T extends Type.Int                  ? vecs.IntVector                  :
    T extends Type.Float16              ? vecs.Float16Vector              :
    T extends Type.Float32              ? vecs.Float32Vector              :
    T extends Type.Float64              ? vecs.Float64Vector              :
    T extends Type.Float                ? vecs.FloatVector                :
    T extends Type.Utf8                 ? vecs.Utf8Vector                 :
    T extends Type.Binary               ? vecs.BinaryVector               :
    T extends Type.FixedSizeBinary      ? vecs.FixedSizeBinaryVector      :
    T extends Type.Date                 ? vecs.DateVector                 :
    T extends Type.DateDay              ? vecs.DateDayVector              :
    T extends Type.DateMillisecond      ? vecs.DateMillisecondVector      :
    T extends Type.Timestamp            ? vecs.TimestampVector            :
    T extends Type.TimestampSecond      ? vecs.TimestampSecondVector      :
    T extends Type.TimestampMillisecond ? vecs.TimestampMillisecondVector :
    T extends Type.TimestampMicrosecond ? vecs.TimestampMicrosecondVector :
    T extends Type.TimestampNanosecond  ? vecs.TimestampNanosecondVector  :
    T extends Type.Time                 ? vecs.TimeVector                 :
    T extends Type.TimeSecond           ? vecs.TimeSecondVector           :
    T extends Type.TimeMillisecond      ? vecs.TimeMillisecondVector      :
    T extends Type.TimeMicrosecond      ? vecs.TimeMicrosecondVector      :
    T extends Type.TimeNanosecond       ? vecs.TimeNanosecondVector       :
    T extends Type.Decimal              ? vecs.DecimalVector              :
    T extends Type.Union                ? vecs.UnionVector                :
    T extends Type.DenseUnion           ? vecs.DenseUnionVector           :
    T extends Type.SparseUnion          ? vecs.SparseUnionVector          :
    T extends Type.Interval             ? vecs.IntervalVector             :
    T extends Type.IntervalDayTime      ? vecs.IntervalDayTimeVector      :
    T extends Type.IntervalYearMonth    ? vecs.IntervalYearMonthVector    :
    T extends Type.Map                  ? vecs.MapVector                  :
    T extends Type.List                 ? vecs.ListVector                 :
    T extends Type.Struct               ? vecs.StructVector               :
    T extends Type.Dictionary           ? vecs.DictionaryVector           :
    T extends Type.FixedSizeList        ? vecs.FixedSizeListVector        :
                                          vecs.BaseVector
    ;

/** @ignore */
type DataTypeToVector<T extends DataType = any> =
    T extends type.Null                 ? vecs.NullVector                          :
    T extends type.Bool                 ? vecs.BoolVector                          :
    T extends type.Int8                 ? vecs.Int8Vector                          :
    T extends type.Int16                ? vecs.Int16Vector                         :
    T extends type.Int32                ? vecs.Int32Vector                         :
    T extends type.Int64                ? vecs.Int64Vector                         :
    T extends type.Uint8                ? vecs.Uint8Vector                         :
    T extends type.Uint16               ? vecs.Uint16Vector                        :
    T extends type.Uint32               ? vecs.Uint32Vector                        :
    T extends type.Uint64               ? vecs.Uint64Vector                        :
    T extends type.Int                  ? vecs.IntVector                           :
    T extends type.Float16              ? vecs.Float16Vector                       :
    T extends type.Float32              ? vecs.Float32Vector                       :
    T extends type.Float64              ? vecs.Float64Vector                       :
    T extends type.Float                ? vecs.FloatVector                         :
    T extends type.Utf8                 ? vecs.Utf8Vector                          :
    T extends type.Binary               ? vecs.BinaryVector                        :
    T extends type.FixedSizeBinary      ? vecs.FixedSizeBinaryVector               :
    T extends type.Date_                ? vecs.DateVector                          :
    T extends type.DateDay              ? vecs.DateDayVector                       :
    T extends type.DateMillisecond      ? vecs.DateMillisecondVector               :
    T extends type.Timestamp            ? vecs.TimestampVector                     :
    T extends type.TimestampSecond      ? vecs.TimestampSecondVector               :
    T extends type.TimestampMillisecond ? vecs.TimestampMillisecondVector          :
    T extends type.TimestampMicrosecond ? vecs.TimestampMicrosecondVector          :
    T extends type.TimestampNanosecond  ? vecs.TimestampNanosecondVector           :
    T extends type.Time                 ? vecs.TimeVector                          :
    T extends type.TimeSecond           ? vecs.TimeSecondVector                    :
    T extends type.TimeMillisecond      ? vecs.TimeMillisecondVector               :
    T extends type.TimeMicrosecond      ? vecs.TimeMicrosecondVector               :
    T extends type.TimeNanosecond       ? vecs.TimeNanosecondVector                :
    T extends type.Decimal              ? vecs.DecimalVector                       :
    T extends type.Union                ? vecs.UnionVector                         :
    T extends type.DenseUnion           ? vecs.DenseUnionVector                    :
    T extends type.SparseUnion          ? vecs.SparseUnionVector                   :
    T extends type.Interval             ? vecs.IntervalVector                      :
    T extends type.IntervalDayTime      ? vecs.IntervalDayTimeVector               :
    T extends type.IntervalYearMonth    ? vecs.IntervalYearMonthVector             :
    T extends type.Map_                 ? vecs.MapVector<T['dataTypes']>           :
    T extends type.List                 ? vecs.ListVector<T['valueType']>          :
    T extends type.Struct               ? vecs.StructVector<T['dataTypes']>        :
    T extends type.Dictionary           ? vecs.DictionaryVector<T['valueType'], T['indices']> :
    T extends type.FixedSizeList        ? vecs.FixedSizeListVector<T['valueType']> :
                                          vecs.BaseVector<T>
    ;

/** @ignore */
export type TypeToDataType<T extends Type> =
      T extends Type.Null                 ? type.Null
    : T extends Type.Bool                 ? type.Bool
    : T extends Type.Int                  ? type.Int
    : T extends Type.Int16                ? type.Int16
    : T extends Type.Int32                ? type.Int32
    : T extends Type.Int64                ? type.Int64
    : T extends Type.Uint8                ? type.Uint8
    : T extends Type.Uint16               ? type.Uint16
    : T extends Type.Uint32               ? type.Uint32
    : T extends Type.Uint64               ? type.Uint64
    : T extends Type.Int8                 ? type.Int8
    : T extends Type.Float16              ? type.Float16
    : T extends Type.Float32              ? type.Float32
    : T extends Type.Float64              ? type.Float64
    : T extends Type.Float                ? type.Float
    : T extends Type.Utf8                 ? type.Utf8
    : T extends Type.Binary               ? type.Binary
    : T extends Type.FixedSizeBinary      ? type.FixedSizeBinary
    : T extends Type.Date                 ? type.Date_
    : T extends Type.DateDay              ? type.DateDay
    : T extends Type.DateMillisecond      ? type.DateMillisecond
    : T extends Type.Timestamp            ? type.Timestamp
    : T extends Type.TimestampSecond      ? type.TimestampSecond
    : T extends Type.TimestampMillisecond ? type.TimestampMillisecond
    : T extends Type.TimestampMicrosecond ? type.TimestampMicrosecond
    : T extends Type.TimestampNanosecond  ? type.TimestampNanosecond
    : T extends Type.Time                 ? type.Time
    : T extends Type.TimeSecond           ? type.TimeSecond
    : T extends Type.TimeMillisecond      ? type.TimeMillisecond
    : T extends Type.TimeMicrosecond      ? type.TimeMicrosecond
    : T extends Type.TimeNanosecond       ? type.TimeNanosecond
    : T extends Type.Decimal              ? type.Decimal
    : T extends Type.Union                ? type.Union
    : T extends Type.DenseUnion           ? type.DenseUnion
    : T extends Type.SparseUnion          ? type.SparseUnion
    : T extends Type.Interval             ? type.Interval
    : T extends Type.IntervalDayTime      ? type.IntervalDayTime
    : T extends Type.IntervalYearMonth    ? type.IntervalYearMonth
    : T extends Type.Map                  ? type.Map_
    : T extends Type.List                 ? type.List
    : T extends Type.Struct               ? type.Struct
    : T extends Type.Dictionary           ? type.Dictionary
    : T extends Type.FixedSizeList        ? type.FixedSizeList
                                          : DataType
    ;

/** @ignore */
type TypeToBuilder<T extends Type = any, TNull = any> =
    T extends Type.Null                 ? builders.NullBuilder<TNull>                 :
    T extends Type.Bool                 ? builders.BoolBuilder<TNull>                 :
    T extends Type.Int8                 ? builders.Int8Builder<TNull>                 :
    T extends Type.Int16                ? builders.Int16Builder<TNull>                :
    T extends Type.Int32                ? builders.Int32Builder<TNull>                :
    T extends Type.Int64                ? builders.Int64Builder<TNull>                :
    T extends Type.Uint8                ? builders.Uint8Builder<TNull>                :
    T extends Type.Uint16               ? builders.Uint16Builder<TNull>               :
    T extends Type.Uint32               ? builders.Uint32Builder<TNull>               :
    T extends Type.Uint64               ? builders.Uint64Builder<TNull>               :
    T extends Type.Int                  ? builders.IntBuilder<any, TNull>             :
    T extends Type.Float16              ? builders.Float16Builder<TNull>              :
    T extends Type.Float32              ? builders.Float32Builder<TNull>              :
    T extends Type.Float64              ? builders.Float64Builder<TNull>              :
    T extends Type.Float                ? builders.FloatBuilder<any, TNull>           :
    T extends Type.Utf8                 ? builders.Utf8Builder<TNull>                 :
    T extends Type.Binary               ? builders.BinaryBuilder<TNull>               :
    T extends Type.FixedSizeBinary      ? builders.FixedSizeBinaryBuilder<TNull>      :
    T extends Type.Date                 ? builders.DateBuilder<any, TNull>            :
    T extends Type.DateDay              ? builders.DateDayBuilder<TNull>              :
    T extends Type.DateMillisecond      ? builders.DateMillisecondBuilder<TNull>      :
    T extends Type.Timestamp            ? builders.TimestampBuilder<any, TNull>       :
    T extends Type.TimestampSecond      ? builders.TimestampSecondBuilder<TNull>      :
    T extends Type.TimestampMillisecond ? builders.TimestampMillisecondBuilder<TNull> :
    T extends Type.TimestampMicrosecond ? builders.TimestampMicrosecondBuilder<TNull> :
    T extends Type.TimestampNanosecond  ? builders.TimestampNanosecondBuilder<TNull>  :
    T extends Type.Time                 ? builders.TimeBuilder<any, TNull>            :
    T extends Type.TimeSecond           ? builders.TimeSecondBuilder<TNull>           :
    T extends Type.TimeMillisecond      ? builders.TimeMillisecondBuilder<TNull>      :
    T extends Type.TimeMicrosecond      ? builders.TimeMicrosecondBuilder<TNull>      :
    T extends Type.TimeNanosecond       ? builders.TimeNanosecondBuilder<TNull>       :
    T extends Type.Decimal              ? builders.DecimalBuilder<TNull>              :
    T extends Type.Union                ? builders.UnionBuilder<any, TNull>           :
    T extends Type.DenseUnion           ? builders.DenseUnionBuilder<any, TNull>      :
    T extends Type.SparseUnion          ? builders.SparseUnionBuilder<any, TNull>     :
    T extends Type.Interval             ? builders.IntervalBuilder<any, TNull>        :
    T extends Type.IntervalDayTime      ? builders.IntervalDayTimeBuilder<TNull>      :
    T extends Type.IntervalYearMonth    ? builders.IntervalYearMonthBuilder<TNull>    :
    T extends Type.Map                  ? builders.MapBuilder<any, TNull>             :
    T extends Type.List                 ? builders.ListBuilder<any, TNull>            :
    T extends Type.Struct               ? builders.StructBuilder<any, TNull>          :
    T extends Type.Dictionary           ? builders.DictionaryBuilder<any, TNull>      :
    T extends Type.FixedSizeList        ? builders.FixedSizeListBuilder<any, TNull>   :
                                          builders.Builder<any, TNull>
    ;

/** @ignore */
type DataTypeToBuilder<T extends DataType = any, TNull = any> =
    T extends type.Null                 ? builders.NullBuilder<TNull>                          :
    T extends type.Bool                 ? builders.BoolBuilder<TNull>                          :
    T extends type.Int8                 ? builders.Int8Builder<TNull>                          :
    T extends type.Int16                ? builders.Int16Builder<TNull>                         :
    T extends type.Int32                ? builders.Int32Builder<TNull>                         :
    T extends type.Int64                ? builders.Int64Builder<TNull>                         :
    T extends type.Uint8                ? builders.Uint8Builder<TNull>                         :
    T extends type.Uint16               ? builders.Uint16Builder<TNull>                        :
    T extends type.Uint32               ? builders.Uint32Builder<TNull>                        :
    T extends type.Uint64               ? builders.Uint64Builder<TNull>                        :
    T extends type.Int                  ? builders.IntBuilder<T, TNull>                        :
    T extends type.Float16              ? builders.Float16Builder<TNull>                       :
    T extends type.Float32              ? builders.Float32Builder<TNull>                       :
    T extends type.Float64              ? builders.Float64Builder<TNull>                       :
    T extends type.Float                ? builders.FloatBuilder<T, TNull>                      :
    T extends type.Utf8                 ? builders.Utf8Builder<TNull>                          :
    T extends type.Binary               ? builders.BinaryBuilder<TNull>                        :
    T extends type.FixedSizeBinary      ? builders.FixedSizeBinaryBuilder<TNull>               :
    T extends type.Date_                ? builders.DateBuilder<T, TNull>                       :
    T extends type.DateDay              ? builders.DateDayBuilder<TNull>                       :
    T extends type.DateMillisecond      ? builders.DateMillisecondBuilder<TNull>               :
    T extends type.Timestamp            ? builders.TimestampBuilder<T, TNull>                  :
    T extends type.TimestampSecond      ? builders.TimestampSecondBuilder<TNull>               :
    T extends type.TimestampMillisecond ? builders.TimestampMillisecondBuilder<TNull>          :
    T extends type.TimestampMicrosecond ? builders.TimestampMicrosecondBuilder<TNull>          :
    T extends type.TimestampNanosecond  ? builders.TimestampNanosecondBuilder<TNull>           :
    T extends type.Time                 ? builders.TimeBuilder<T, TNull>                       :
    T extends type.TimeSecond           ? builders.TimeSecondBuilder<TNull>                    :
    T extends type.TimeMillisecond      ? builders.TimeMillisecondBuilder<TNull>               :
    T extends type.TimeMicrosecond      ? builders.TimeMicrosecondBuilder<TNull>               :
    T extends type.TimeNanosecond       ? builders.TimeNanosecondBuilder<TNull>                :
    T extends type.Decimal              ? builders.DecimalBuilder<TNull>                       :
    T extends type.Union                ? builders.UnionBuilder<T, TNull>                      :
    T extends type.DenseUnion           ? builders.DenseUnionBuilder<T, TNull>                 :
    T extends type.SparseUnion          ? builders.SparseUnionBuilder<T, TNull>                :
    T extends type.Interval             ? builders.IntervalBuilder<T, TNull>                   :
    T extends type.IntervalDayTime      ? builders.IntervalDayTimeBuilder<TNull>               :
    T extends type.IntervalYearMonth    ? builders.IntervalYearMonthBuilder<TNull>             :
    T extends type.Map_                 ? builders.MapBuilder<T['dataTypes'], TNull>           :
    T extends type.List                 ? builders.ListBuilder<T['valueType'], TNull>          :
    T extends type.Struct               ? builders.StructBuilder<T['dataTypes'], TNull>        :
    T extends type.Dictionary           ? builders.DictionaryBuilder<T, TNull>                 :
    T extends type.FixedSizeList        ? builders.FixedSizeListBuilder<T['valueType'], TNull> :
                                          builders.Builder<any, TNull>
    ;

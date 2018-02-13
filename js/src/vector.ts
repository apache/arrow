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

import { Data, ChunkedData, FlatData, BoolData, FlatListData, NestedData, DictionaryData } from './data';
import { VisitorNode, TypeVisitor, VectorVisitor } from './visitor';
import { DataType, ListType, FlatType, NestedType, FlatListType, TimeUnit } from './type';
import { IterableArrayLike, Precision, DateUnit, IntervalUnit, UnionMode } from './type';

export interface VectorLike { length: number; nullCount: number; }

export interface View<T extends DataType> {
    clone(data: Data<T>): this;
    isValid(index: number): boolean;
    get(index: number): T['TValue'] | null;
    set(index: number, value: T['TValue']): void;
    toArray(): IterableArrayLike<T['TValue'] | null>;
    [Symbol.iterator](): IterableIterator<T['TValue'] | null>;
}

export class Vector<T extends DataType = any> implements VectorLike, View<T>, VisitorNode {
    public static create<T extends DataType>(data: Data<T>): Vector<T> {
        return createVector(data);
    }
    public static concat<T extends DataType>(source?: Vector<T> | null, ...others: Vector<T>[]): Vector<T> {
        return others.reduce((a, b) => a ? a.concat(b) : b, source!);
    }
    public type: T;
    public length: number;
    public readonly data: Data<T>;
    public readonly view: View<T>;
    constructor(data: Data<T>, view: View<T>) {
        this.data = data;
        this.type = data.type;
        this.length = data.length;
        let nulls: Uint8Array;
        if ((<any> data instanceof ChunkedData) && !(view instanceof ChunkedView)) {
            this.view = new ChunkedView(data);
        } else if (!(view instanceof ValidityView) && (nulls = data.nullBitmap!) && nulls.length > 0 && data.nullCount > 0) {
            this.view = new ValidityView(data, view);
        } else {
            this.view = view;
        }
    }

    public get nullCount() { return this.data.nullCount; }
    public get nullBitmap() { return this.data.nullBitmap; }
    public get [Symbol.toStringTag]() {
        return `Vector<${this.type[Symbol.toStringTag]}>`;
    }
    public toJSON(): any { return this.toArray(); }
    public clone<R extends T>(data: Data<R>, view: View<R> = this.view.clone(data) as any): this {
        return new (this.constructor as any)(data, view);
    }
    public isValid(index: number): boolean {
        return this.view.isValid(index);
    }
    public get(index: number): T['TValue'] | null {
        return this.view.get(index);
    }
    public set(index: number, value: T['TValue']): void {
        return this.view.set(index, value);
    }
    public toArray(): IterableArrayLike<T['TValue'] | null> {
        return this.view.toArray();
    }
    public [Symbol.iterator](): IterableIterator<T['TValue'] | null> {
        return this.view[Symbol.iterator]();
    }
    public concat(...others: Vector<T>[]): this {
        if ((others = others.filter(Boolean)).length === 0) {
            return this;
        }
        const { view } = this;
        const vecs = !(view instanceof ChunkedView)
            ? [this, ...others]
            : [...view.chunkVectors, ...others];
        const offsets = ChunkedData.computeOffsets(vecs);
        const chunksLength = offsets[offsets.length - 1];
        const chunkedData = new ChunkedData(this.type, chunksLength, vecs, 0, -1, offsets);
        return this.clone(chunkedData, new ChunkedView(chunkedData)) as this;
    }
    public slice(begin?: number, end?: number): this {
        let { length } = this;
        let size = (this.view as any).size || 1;
        let total = length, from = (begin || 0) * size;
        let to = (typeof end === 'number' ? end : total) * size;
        if (to < 0) { to = total - (to * -1) % total; }
        if (from < 0) { from = total - (from * -1) % total; }
        if (to < from) { [from, to] = [to, from]; }
        total = !isFinite(total = (to - from)) || total < 0 ? 0 : total;
        const slicedData = this.data.slice(from, Math.min(total, length));
        return this.clone(slicedData, this.view.clone(slicedData)) as this;
    }

    public acceptTypeVisitor(visitor: TypeVisitor): any {
        return TypeVisitor.visitTypeInline(visitor, this.type);
    }
    public acceptVectorVisitor(visitor: VectorVisitor): any {
        return VectorVisitor.visitTypeInline(visitor, this.type, this);
    }
}

export abstract class FlatVector<T extends FlatType> extends Vector<T> {
    public get values() { return this.data.values; }
    public lows(): IntVector<Int32> { return this.asInt32(0, 2); }
    public highs(): IntVector<Int32> { return this.asInt32(1, 2); }
    public asInt32(offset: number = 0, stride: number = 2): IntVector<Int32> {
        let data = (this.data as FlatData<any>).clone(new Int32());
        if (offset > 0) {
            data = data.slice(offset, this.length - offset);
        }
        const int32s = new IntVector(data, new PrimitiveView(data, stride));
        int32s.length = this.length / stride | 0;
        return int32s;
    }
}

export abstract class ListVectorBase<T extends (ListType | FlatListType)> extends Vector<T> {
    public get values() { return this.data.values; }
    public get valueOffsets() { return this.data.valueOffsets; }
    public getValueOffset(index: number) {
        return this.valueOffsets[index];
    }
    public getValueLength(index: number) {
        return this.valueOffsets[index + 1] - this.valueOffsets[index];
    }
}

export abstract class NestedVector<T extends NestedType> extends Vector<T>  {
    // @ts-ignore
    public readonly view: NestedView<T>;
    // @ts-ignore
    protected _childData: Data<any>[];
    public getChildAt<R extends DataType = DataType>(index: number): Vector<R> | null {
        return this.view.getChildAt<R>(index);
    }
    public get childData(): Data<any>[] {
        let data: Data<T> | Data<any>[];
        if ((data = this._childData)) {
            // Return the cached childData reference first
            return data as Data<any>[];
        } else if (!(<any> (data = this.data) instanceof ChunkedData)) {
            // If data isn't chunked, cache and return NestedData's childData
            return this._childData = (data as NestedData<T>).childData;
        }
        // Otherwise if the data is chunked, concatenate the childVectors from each chunk
        // to construct a single chunked Vector for each column. Then return the ChunkedData
        // instance from each unified chunked column as the childData of a chunked NestedVector
        const chunks = ((data as ChunkedData<T>).chunkVectors as NestedVector<T>[]);
        return this._childData = chunks
            .reduce<(Vector<T> | null)[][]>((cols, chunk) => chunk.childData
            .reduce<(Vector<T> | null)[][]>((cols, _, i) => (
                (cols[i] || (cols[i] = [])).push(chunk.getChildAt(i))
            ) && cols || cols, cols), [] as Vector<T>[][])
        .map((vecs) => Vector.concat<T>(...vecs).data);
    }
}

import { List, Binary, Utf8, Bool, } from './type';
import { Null, Int, Float, Decimal, Date_, Time, Timestamp, Interval } from './type';
import { Uint8, Uint16, Uint32, Uint64, Int8, Int16, Int32, Int64, Float16, Float32, Float64 } from './type';
import { Struct, Union, SparseUnion, DenseUnion, FixedSizeBinary, FixedSizeList, Map_, Dictionary } from './type';

import { ChunkedView } from './vector/chunked';
import { DictionaryView } from './vector/dictionary';
import { ListView, FixedSizeListView, BinaryView, Utf8View } from './vector/list';
import { UnionView, DenseUnionView, NestedView, StructView, MapView } from './vector/nested';
import { FlatView, NullView, BoolView, ValidityView, PrimitiveView, FixedSizeView, Float16View } from './vector/flat';
import { DateDayView, DateMillisecondView, IntervalYearMonthView } from './vector/flat';
import { TimestampDayView, TimestampSecondView, TimestampMillisecondView, TimestampMicrosecondView, TimestampNanosecondView } from './vector/flat';
import { packBools } from './util/bit';

export class NullVector extends Vector<Null> {
    constructor(data: Data<Null>, view: View<Null> = new NullView(data)) {
        super(data, view);
    }
}

export class BoolVector extends Vector<Bool> {
    public static from(data: IterableArrayLike<boolean>) {
        return new BoolVector(new BoolData(new Bool(), data.length, null, packBools(data)));
    }
    public get values() { return this.data.values; }
    constructor(data: Data<Bool>, view: View<Bool> = new BoolView(data)) {
        super(data, view);
    }
}

export class IntVector<T extends Int = Int<any>> extends FlatVector<T> {
    public static from(data: Int8Array): IntVector<Int8>;
    public static from(data: Int16Array): IntVector<Int16>;
    public static from(data: Int32Array): IntVector<Int32>;
    public static from(data: Uint8Array): IntVector<Uint8>;
    public static from(data: Uint16Array): IntVector<Uint16>;
    public static from(data: Uint32Array): IntVector<Uint32>;
    public static from(data: Int32Array, is64: true): IntVector<Int64>;
    public static from(data: Uint32Array, is64: true): IntVector<Uint64>;
    public static from(data: any, is64?: boolean) {
        if (is64 === true) {
            return data instanceof Int32Array
                ? new IntVector(new FlatData(new Int64(), data.length, null, data))
                : new IntVector(new FlatData(new Uint64(), data.length, null, data));
        }
        switch (data.constructor) {
            case Int8Array: return new IntVector(new FlatData(new Int8(), data.length, null, data));
            case Int16Array: return new IntVector(new FlatData(new Int16(), data.length, null, data));
            case Int32Array: return new IntVector(new FlatData(new Int32(), data.length, null, data));
            case Uint8Array: return new IntVector(new FlatData(new Uint8(), data.length, null, data));
            case Uint16Array: return new IntVector(new FlatData(new Uint16(), data.length, null, data));
            case Uint32Array: return new IntVector(new FlatData(new Uint32(), data.length, null, data));
        }
        throw new TypeError('Unrecognized Int data');
    }
    static defaultView<T extends Int>(data: Data<T>) {
        return data.type.bitWidth <= 32 ? new FlatView(data) : new FixedSizeView(data, (data.type.bitWidth / 32) | 0);
    }
    constructor(data: Data<T>, view: View<T> = IntVector.defaultView(data)) {
        super(data, view);
    }
}

export class FloatVector<T extends Float = Float<any>> extends FlatVector<T> {
    public static from(data: Uint16Array): FloatVector<Float16>;
    public static from(data: Float32Array): FloatVector<Float32>;
    public static from(data: Float64Array): FloatVector<Float64>;
    public static from(data: any) {
        switch (data.constructor) {
            case Uint16Array: return new FloatVector(new FlatData(new Float16(), data.length, null, data));
            case Float32Array: return new FloatVector(new FlatData(new Float32(), data.length, null, data));
            case Float64Array: return new FloatVector(new FlatData(new Float64(), data.length, null, data));
        }
        throw new TypeError('Unrecognized Float data');
    }
    static defaultView<T extends Float>(data: Data<T>): FlatView<any> {
        return data.type.precision !== Precision.HALF ? new FlatView(data) : new Float16View(data as Data<Float16>);
    }
    constructor(data: Data<T>, view: View<T> = FloatVector.defaultView(data)) {
        super(data, view);
    }
}

export class DateVector extends FlatVector<Date_> {
    static defaultView<T extends Date_>(data: Data<T>) {
        return data.type.unit === DateUnit.DAY ? new DateDayView(data) : new DateMillisecondView(data, 2);
    }
    constructor(data: Data<Date_>, view: View<Date_> = DateVector.defaultView(data)) {
        super(data, view);
    }
    public lows(): IntVector<Int32> {
        return this.type.unit === DateUnit.DAY ? this.asInt32(0, 1) : this.asInt32(0, 2);
    }
    public highs(): IntVector<Int32> {
        return this.type.unit === DateUnit.DAY ? this.asInt32(0, 1) : this.asInt32(1, 2);
    }
    public asEpochMilliseconds(): IntVector<Int32> {
        let data = (this.data as FlatData<any>).clone(new Int32());
        switch (this.type.unit) {
            case DateUnit.DAY: return new IntVector(data, new TimestampDayView(data as any, 1) as any);
            case DateUnit.MILLISECOND: return new IntVector(data, new TimestampMillisecondView(data as any, 2) as any);
        }
        throw new TypeError(`Unrecognized date unit "${DateUnit[this.type.unit]}"`);
    }
}

export class DecimalVector extends FlatVector<Decimal> {
    constructor(data: Data<Decimal>, view: View<Decimal> = new FixedSizeView(data, 4)) {
        super(data, view);
    }
}

export class TimeVector extends FlatVector<Time> {
    static defaultView<T extends Time>(data: Data<T>) {
        return data.type.bitWidth <= 32 ? new FlatView(data) : new FixedSizeView(data, (data.type.bitWidth / 32) | 0);
    }
    constructor(data: Data<Time>, view: View<Time> = TimeVector.defaultView(data)) {
        super(data, view);
    }
    public lows(): IntVector<Int32> {
        return this.type.bitWidth <= 32 ? this.asInt32(0, 1) : this.asInt32(0, 2);
    }
    public highs(): IntVector<Int32> {
        return this.type.bitWidth <= 32 ? this.asInt32(0, 1) : this.asInt32(1, 2);
    }
}

export class TimestampVector extends FlatVector<Timestamp> {
    constructor(data: Data<Timestamp>, view: View<Timestamp> = new FixedSizeView(data, 2)) {
        super(data, view);
    }
    public asEpochMilliseconds(): IntVector<Int32> {
        let data = (this.data as FlatData<any>).clone(new Int32());
        switch (this.type.unit) {
            case TimeUnit.SECOND: return new IntVector(data, new TimestampSecondView(data as any, 1) as any);
            case TimeUnit.MILLISECOND: return new IntVector(data, new TimestampMillisecondView(data as any, 2) as any);
            case TimeUnit.MICROSECOND: return new IntVector(data, new TimestampMicrosecondView(data as any, 2) as any);
            case TimeUnit.NANOSECOND: return new IntVector(data, new TimestampNanosecondView(data as any, 2) as any);
        }
        throw new TypeError(`Unrecognized time unit "${TimeUnit[this.type.unit]}"`);
    }
}

export class IntervalVector extends FlatVector<Interval> {
    static defaultView<T extends Interval>(data: Data<T>) {
        return data.type.unit === IntervalUnit.YEAR_MONTH ? new IntervalYearMonthView(data) : new FixedSizeView(data, 2);
    }
    constructor(data: Data<Interval>, view: View<Interval> = IntervalVector.defaultView(data)) {
        super(data, view);
    }
    public lows(): IntVector<Int32> {
        return this.type.unit === IntervalUnit.YEAR_MONTH ? this.asInt32(0, 1) : this.asInt32(0, 2);
    }
    public highs(): IntVector<Int32> {
        return this.type.unit === IntervalUnit.YEAR_MONTH ? this.asInt32(0, 1) : this.asInt32(1, 2);
    }
}

export class BinaryVector extends ListVectorBase<Binary> {
    constructor(data: Data<Binary>, view: View<Binary> = new BinaryView(data)) {
        super(data, view);
    }
    public asUtf8() {
        return new Utf8Vector((this.data as FlatListData<any>).clone(new Utf8()));
    }
}

export class FixedSizeBinaryVector extends FlatVector<FixedSizeBinary> {
    constructor(data: Data<FixedSizeBinary>, view: View<FixedSizeBinary> = new FixedSizeView(data, data.type.byteWidth)) {
        super(data, view);
    }
}

export class Utf8Vector extends ListVectorBase<Utf8> {
    constructor(data: Data<Utf8>, view: View<Utf8> = new Utf8View(data)) {
        super(data, view);
    }
    public asBinary() {
        return new BinaryVector((this.data as FlatListData<any>).clone(new Binary()));
    }
}

export class ListVector<T extends DataType = DataType> extends ListVectorBase<List<T>> {
    constructor(data: Data<List<T>>, view: View<List<T>> = new ListView(data)) {
        super(data, view);
    }
}

export class FixedSizeListVector extends Vector<FixedSizeList> {
    constructor(data: Data<FixedSizeList>, view: View<FixedSizeList> = new FixedSizeListView(data)) {
        super(data, view);
    }
}

export class MapVector extends NestedVector<Map_> {
    constructor(data: Data<Map_>, view: View<Map_> = new MapView(data)) {
        super(data, view);
    }
    public asStruct() {
        return new StructVector((this.data as NestedData<any>).clone(new Struct(this.type.children)));
    }
}

export class StructVector extends NestedVector<Struct> {
    constructor(data: Data<Struct>, view: View<Struct> = new StructView(data)) {
        super(data, view);
    }
    public asMap(keysSorted: boolean = false) {
        return new MapVector((this.data as NestedData<any>).clone(new Map_(keysSorted, this.type.children)));
    }
}

export class UnionVector<T extends (SparseUnion | DenseUnion) = any> extends NestedVector<T> {
    constructor(data: Data<T>, view: View<T> = <any> (data.type.mode === UnionMode.Sparse ? new UnionView<SparseUnion>(data as Data<SparseUnion>) : new DenseUnionView(data as Data<DenseUnion>))) {
        super(data, view);
    }
}

export class DictionaryVector<T extends DataType = DataType> extends Vector<Dictionary<T>> {
    // @ts-ignore
    public readonly indicies: Vector<Int>;
    // @ts-ignore
    public readonly dictionary: Vector<T>;
    constructor(data: Data<Dictionary<T>>, view: View<Dictionary<T>> = new DictionaryView<T>(data.dictionary, new IntVector(data.indicies))) {
        super(data as Data<any>, view);
        if (data instanceof DictionaryData && view instanceof DictionaryView) {
            this.indicies = view.indicies;
            this.dictionary = data.dictionary;
        } else if (data instanceof ChunkedData && view instanceof ChunkedView) {
            const chunks = view.chunkVectors as DictionaryVector<T>[];
            // Assume the last chunk's dictionary data is the most up-to-date,
            // including data from DictionaryBatches that were marked as deltas
            this.dictionary = chunks[chunks.length - 1].dictionary;
            this.indicies = chunks.reduce<Vector<Int> | null>(
                (idxs: Vector<Int> | null, dict: DictionaryVector<T>) =>
                    !idxs ? dict.indicies! : idxs.concat(dict.indicies!),
                null
            )!;
        } else {
            throw new TypeError(`Unrecognized DictionaryVector view`);
        }
    }
    public getKey(index: number) { return this.indicies.get(index); }
    public getValue(key: number) { return this.dictionary.get(key); }
}

export const createVector = ((VectorLoader: new <T extends DataType>(data: Data<T>) => TypeVisitor) => (
    <T extends DataType>(data: Data<T>) => TypeVisitor.visitTypeInline(new VectorLoader(data), data.type) as Vector<T>
))(class VectorLoader<T extends DataType> extends TypeVisitor {
    constructor(private data: Data<T>) { super(); }
    visitNull           (_type: Null)            { return new NullVector(this.data);            }
    visitInt            (_type: Int)             { return new IntVector(this.data);             }
    visitFloat          (_type: Float)           { return new FloatVector(this.data);           }
    visitBinary         (_type: Binary)          { return new BinaryVector(this.data);          }
    visitUtf8           (_type: Utf8)            { return new Utf8Vector(this.data);            }
    visitBool           (_type: Bool)            { return new BoolVector(this.data);            }
    visitDecimal        (_type: Decimal)         { return new DecimalVector(this.data);         }
    visitDate           (_type: Date_)           { return new DateVector(this.data);            }
    visitTime           (_type: Time)            { return new TimeVector(this.data);            }
    visitTimestamp      (_type: Timestamp)       { return new TimestampVector(this.data);       }
    visitInterval       (_type: Interval)        { return new IntervalVector(this.data);        }
    visitList           (_type: List)            { return new ListVector(this.data);            }
    visitStruct         (_type: Struct)          { return new StructVector(this.data);          }
    visitUnion          (_type: Union)           { return new UnionVector(this.data);           }
    visitFixedSizeBinary(_type: FixedSizeBinary) { return new FixedSizeBinaryVector(this.data); }
    visitFixedSizeList  (_type: FixedSizeList)   { return new FixedSizeListVector(this.data);   }
    visitMap            (_type: Map_)            { return new MapVector(this.data);             }
    visitDictionary     (_type: Dictionary)      { return new DictionaryVector(this.data);      }
});

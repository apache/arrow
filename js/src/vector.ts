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

import { Data, ChunkedData } from './data';
import { VisitorNode, TypeVisitor, VectorVisitor } from './visitor';
import { DataType, ListType, FlatType, NestedType, FlatListType } from './type';
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
    // @ts-ignore
    protected _data: Data<T>;
    // @ts-ignore
    protected _view: View<T>;
    constructor(data: Data<T>, view: View<T>) {
        this._data = data;
        let nulls: Uint8Array;
        if ((<any> data instanceof ChunkedData) && !(view instanceof ChunkedView)) {
            this._view = new ChunkedView(data);
        } else if (!(view instanceof ValidityView) && (nulls = data.nullBitmap!) && nulls.length > 0 && data.nullCount > 0) {
            this._view = new ValidityView(data, view);
        } else {
            this._view = view;
        }
    }

    public get data() { return this._data; }
    public get type() { return this._data.type; }
    public get length() { return this._data.length; }
    public get nullCount() { return this._data.nullCount; }
    public get nullBitmap() { return this._data.nullBitmap; }
    public get [Symbol.toStringTag]() {
        return `Vector<${this.type[Symbol.toStringTag]}>`;
    }
    public toJSON() { return this.toArray(); }
    public clone(data: Data<T>): this {
        return this._view.clone(this._data = data) && this || this;
    }
    public isValid(index: number): boolean {
        return this._view.isValid(index);
    }
    public get(index: number): T['TValue'] | null {
        return this._view.get(index);
    }
    public set(index: number, value: T['TValue']): void {
        return this._view.set(index, value);
    }
    public toArray(): IterableArrayLike<T['TValue'] | null> {
        return this._view.toArray();
    }
    public [Symbol.iterator](): IterableIterator<T['TValue'] | null> {
        return this._view[Symbol.iterator]();
    }
    public concat(...others: Vector<T>[]): this {
        if ((others = others.filter(Boolean)).length === 0) {
            return this;
        }
        const { _view: view } = this;
        const vecs = !(view instanceof ChunkedView)
            ? [this, ...others]
            : [...view.chunks, ...others];
        const offsets = ChunkedData.computeOffsets(vecs);
        const chunksLength = offsets[offsets.length - 1];
        const chunkedData = new ChunkedData(this.type, chunksLength, vecs, 0, -1, offsets);
        return new (this.constructor as any)(chunkedData, new ChunkedView(chunkedData)) as this;
    }
    public slice(begin?: number, end?: number): this {
        let { length } = this;
        let size = (this._view as any).size || 1;
        let total = length, from = (begin || 0) * size;
        let to = (typeof end === 'number' ? end : total) * size;
        if (to < 0) { to = total - (to * -1) % total; }
        if (from < 0) { from = total - (from * -1) % total; }
        if (to < from) { [from, to] = [to, from]; }
        total = !isFinite(total = (to - from)) || total < 0 ? 0 : total;
        const newData = this._data.slice(from, Math.min(total, length));
        return new (this.constructor as any)(newData, this._view.clone(newData)) as this;
    }

    public acceptTypeVisitor(visitor: TypeVisitor): any {
        return TypeVisitor.visitTypeInline(visitor, this.type);
    }
    public acceptVectorVisitor(visitor: VectorVisitor): any {
        return VectorVisitor.visitTypeInline(visitor, this.type, this);
    }
}

export abstract class FlatVector<T extends FlatType> extends Vector<T> {
    public get values() { return this._data.values; }
}

export abstract class ListVectorBase<T extends (ListType | FlatListType)> extends Vector<T> {
    public get values() { return this._data.values; }
    public get valueOffsets() { return this._data.valueOffsets; }
    public getValueOffset(index: number) {
        return this.valueOffsets[index];
    }
    public getValueLength(index: number) {
        return this.valueOffsets[index + 1] - this.valueOffsets[index];
    }
}

export abstract class NestedVector<T extends NestedType> extends Vector<T>  {
    // @ts-ignore
    protected _view: NestedView<T>;
    public get childData(): Data<any>[] {
        return this.data.childData;
    }
    public getChildAt<R extends DataType = DataType>(index: number) {
        return this._view.getChildAt<R>(index);
    }
}

import { List, Binary, Utf8, Bool, } from './type';
import { Null, Int, Float, Float16, Decimal, Date_, Time, Timestamp, Interval } from './type';
import { Struct, Union, SparseUnion, DenseUnion, FixedSizeBinary, FixedSizeList, Map_, Dictionary } from './type';

import { ChunkedView } from './vector/chunked';
import { DictionaryView } from './vector/dictionary';
import { ListView, FixedSizeListView, BinaryView, Utf8View } from './vector/list';
import { UnionView, DenseUnionView, NestedView, StructView, MapView } from './vector/nested';
import { FlatView, NullView, BoolView, ValidityView, FixedSizeView, Float16View, DateDayView, DateMillisecondView, IntervalYearMonthView } from './vector/flat';

export class NullVector extends Vector<Null> {
    constructor(data: Data<Null>, view: View<Null> = new NullView(data)) {
        super(data, view);
    }
}

export class BoolVector extends Vector<Bool> {
    constructor(data: Data<Bool>, view: View<Bool> = new BoolView(data)) {
        super(data, view);
    }
}

export class IntVector<T extends Int = Int<any>> extends FlatVector<T> {
    static defaultView<T extends Int>(data: Data<T>) {
        return data.type.bitWidth <= 32 ? new FlatView(data) : new FixedSizeView(data, (data.type.bitWidth / 32) | 0);
    }
    constructor(data: Data<T>, view: View<T> = IntVector.defaultView(data)) {
        super(data, view);
    }
}

export class FloatVector<T extends Float = Float<any>> extends FlatVector<T> {
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
}

export class TimestampVector extends FlatVector<Timestamp> {
    constructor(data: Data<Timestamp>, view: View<Timestamp> = new FixedSizeView(data, 2)) {
        super(data, view);
    }
}

export class IntervalVector extends FlatVector<Interval> {
    static defaultView<T extends Interval>(data: Data<T>) {
        return data.type.unit === IntervalUnit.YEAR_MONTH ? new IntervalYearMonthView(data) : new FixedSizeView(data, 2);
    }
    constructor(data: Data<Interval>, view: View<Interval> = IntervalVector.defaultView(data)) {
        super(data, view);
    }
}

export class BinaryVector extends ListVectorBase<Binary> {
    constructor(data: Data<Binary>, view: View<Binary> = new BinaryView(data)) {
        super(data, view);
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
}

export class ListVector<T extends DataType = DataType> extends ListVectorBase<List<T>> {
    constructor(data: Data<List<T>>, view: View<List<T>> = new ListView(data)) {
        super(data, view);
    }
}

export class FixedSizeListVector extends ListVectorBase<FixedSizeList> {
    constructor(data: Data<FixedSizeList>, view: View<FixedSizeList> = new FixedSizeListView(data)) {
        super(data, view);
    }
}

export class MapVector extends NestedVector<Map_> {
    constructor(data: Data<Map_>, view: View<Map_> = new MapView(data)) {
        super(data, view);
    }
}

export class StructVector extends NestedVector<Struct> {
    constructor(data: Data<Struct>, view: View<Struct> = new StructView(data)) {
        super(data, view);
    }
}

export class UnionVector<T extends (SparseUnion | DenseUnion) = any> extends NestedVector<T> {
    constructor(data: Data<T>, view: View<T> = <any> (data.type.mode === UnionMode.Sparse ? new UnionView<SparseUnion>(data as Data<SparseUnion>) : new DenseUnionView(data as Data<DenseUnion>))) {
        super(data, view);
    }
}

export class DictionaryVector<T extends DataType = DataType> extends Vector<Dictionary<T>> {
    public readonly indicies?: Vector<Int>;
    public readonly dictionary?: Vector<T>;
    constructor(data: Data<Dictionary<T>>, view: View<Dictionary<T>> = new DictionaryView<T>(data.dictionary, new IntVector(data.indicies))) {
        super(data as Data<any>, view);
        if (view instanceof DictionaryView) {
            this.indicies = view.indicies;
            this.dictionary = view.dictionary;
        } else if (view instanceof ChunkedView) {
            this.dictionary = (view.chunks[0] as DictionaryVector<T>).dictionary;
            this.indicies = (view.chunks as DictionaryVector<T>[]).reduce<Vector<Int> | null>(
                (idxs: Vector<Int> | null, dict: DictionaryVector<T>) =>
                    !idxs ? dict.indicies! : idxs.concat(dict.indicies!),
                null
            )!;
        }
    }
    public getKey(index: number) { return this.indicies!.get(index); }
    public getValue(key: number) { return this.dictionary!.get(key); }
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

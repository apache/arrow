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
import { VisitorNode, TypeVisitor, VectorVisitor } from './visitor';
import { DataType, ListType, FlatType, NestedType } from './type';
import { IterableArrayLike, Precision, DateUnit, IntervalUnit, UnionMode } from './type';

export interface VectorLike { length: number; nullCount: number; }

export interface View<T extends DataType> {
    isValid(index: number): boolean;
    get(index: number): T['TValue'] | null;
    toArray(): IterableArrayLike<T['TValue'] | null>;
    [Symbol.iterator](): IterableIterator<T['TValue'] | null>;
}

export class Vector<T extends DataType = any> implements VectorLike, View<T>, Partial<VisitorNode> {
    // @ts-ignore
    protected _data: Data<T>;
    // @ts-ignore
    protected _view: View<T>;
    constructor(data: Data<T>, view: View<T>) {
        this._view = view;
        const nullBitmap = (this._data = data).nullBitmap;
        if (nullBitmap && nullBitmap.length > 0 && data.nullCount > 0) {
            this._view = new ValidityView(data, this._view);
        }
    }

    public get data() { return this._data; }
    public get type() { return this._data.type; }
    public get length() { return this._data.length; }
    public get nullCount() { return this._data.nullCount; }
    public get nullBitmap() { return this._data.nullBitmap; }
    public get [Symbol.toStringTag]() { return `Vector<${this.type[Symbol.toStringTag]}>`; }

    public isValid(index: number): boolean { return this._view.isValid(index); }
    public get(index: number): T['TValue'] | null { return this._view.get(index); }
    public toArray(): IterableArrayLike<T['TValue'] | null> { return this._view.toArray(); }
    public [Symbol.iterator](): IterableIterator<T['TValue'] | null> { return this._view[Symbol.iterator](); }
    public slice(begin?: number, end?: number): this {
        let total = this.length, from = begin || 0;
        let to = typeof end === 'number' ? end : total;
        if (to < 0) { to = total + to; }
        if (from < 0) { from = total - (from * -1) % total; }
        if (to < from) { from = to; to = begin || 0; }
        total = !isFinite(total = (to - from)) || total < 0 ? 0 : total;
        const data = this._data.slice(from, Math.min(total, this.length));
        return new (this.constructor as any)(data, this._view) as this;
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

export abstract class ListVectorBase<T extends ListType> extends Vector<T> {
    public get values() { return this._data.values; }
    public get valueOffsets() { return this._data.valueOffsets; }
    public getValueOffset(index: number) {
        return this.data.valueOffsets[index];
    }
    public getValueLength(index: number) {
        return this.data.valueOffsets[index + 1] - this.data.valueOffsets[index];
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

import { DictionaryView } from './vector/dictionary';
import { ListView, FixedSizeListView, BinaryView, Utf8View } from './vector/list';
import { UnionView, DenseUnionView, NestedView, StructView, MapView } from './vector/nested';
import { FlatView, NullView, BoolView, ValidityView, FixedSizeView, Float16View, DateDayView, DateMillisecondView, IntervalYearMonthView } from './vector/flat';

export class NullVector extends Vector<Null> {
    constructor(data: Data<Null>) {
        super(data, new NullView(data));
    }
}

export class BoolVector extends Vector<Bool> {
    constructor(data: Data<Bool>) {
        super(data, new BoolView(data));
    }
}

export class IntVector extends FlatVector<Int<any>> {
    constructor(data: Data<Int>, view: View<Int> = IntVector.viewForBitWidth(data)) {
        super(data, view);
    }
    static viewForBitWidth<T extends Int | Time>(data: Data<T>) {
        return data.type.bitWidth <= 32 ? new FlatView(data) : new FixedSizeView(data, (data.type.bitWidth / 32) | 0);
    }
}

export class FloatVector extends FlatVector<Float<any>> {
    constructor(data: Data<Float>) {
        super(data, data.type.precision !== Precision.HALF ?
            new FlatView(data) :
            new Float16View(data as Data<Float16>));
    }
}

export class DateVector extends FlatVector<Date_> {
    constructor(data: Data<Date_>) {
        super(data, data.type.unit === DateUnit.DAY ? new DateDayView(data) : new DateMillisecondView(data, 2));
    }
}

export class DecimalVector extends FlatVector<Decimal> {
    constructor(data: Data<Decimal>) {
        super(data, new FixedSizeView(data, 4));
    }
}

export class TimeVector extends FlatVector<Time> {
    constructor(data: Data<Time>) {
        super(data, IntVector.viewForBitWidth(data));
    }
}

export class TimestampVector extends FlatVector<Timestamp> {
    constructor(data: Data<Timestamp>) {
        super(data, new FixedSizeView(data, 2));
    }
}

export class IntervalVector extends FlatVector<Interval> {
    constructor(data: Data<Interval>) {
        super(data, data.type.unit === IntervalUnit.YEAR_MONTH ? new IntervalYearMonthView(data) : new FixedSizeView(data, 2));
    }
}

export class BinaryVector extends ListVectorBase<Binary> {
    constructor(data: Data<Binary>) {
        super(data, new BinaryView(data));
    }
}

export class FixedSizeBinaryVector extends FlatVector<FixedSizeBinary> {
    constructor(data: Data<FixedSizeBinary>) {
        super(data, new FixedSizeView(data, data.type.byteWidth));
    }
}

export class Utf8Vector extends ListVectorBase<Utf8> {
    constructor(data: Data<Utf8>) {
        super(data, new Utf8View(data));
    }
}

export class ListVector<T extends DataType> extends ListVectorBase<List<T>> {
    constructor(data: Data<List<T>>) {
        super(data, new ListView(data));
    }
}

export class FixedSizeListVector extends ListVectorBase<FixedSizeList> {
    constructor(data: Data<FixedSizeList>) {
        super(data, new FixedSizeListView(data));
    }
}

export class MapVector extends NestedVector<Map_> {
    constructor(data: Data<Map_>) {
        super(data, new MapView(data));
    }
}

export class StructVector extends NestedVector<Struct> {
    constructor(data: Data<Struct>) {
        super(data, new StructView(data));
    }
}

export class UnionVector<T extends (SparseUnion | DenseUnion)> extends NestedVector<T> {
    constructor(data: Data<T>) {
        super(data, data.type.mode === UnionMode.Sparse ?
            new UnionView(data)  :
            new DenseUnionView(data as Data<DenseUnion>));
    }
}

export class DictionaryVector<T extends DataType> extends Vector<T> {
    // @ts-ignore
    protected _view: DictionaryView<T>;
    public get indicies() { return this._view.indicies; }
    public get dictionary() { return this._view.dictionary; }
    constructor(data: Dictionary<T>) {
        super(data.indicies as Data<any>, new DictionaryView<T>(data.dictionary, new IntVector(data.indicies)));
    }
    public getKey(index: number) {
        return this.indicies.get(index);
    }
    public getValue(key: number) {
        return this.dictionary.get(key);
    }
}

export const createVector = ((VectorFactory: new <T extends DataType>(data: Data<T>) => TypeVisitor) => (
    <T extends DataType>(data: Data<T>) => data.type.acceptTypeVisitor(new VectorFactory(data)) as Vector<T>
))(class VectorFactory<T extends DataType> extends TypeVisitor {
    constructor(private data: Data<T>) { super(); }
    visitNull           (_node: Null)            { return new NullVector(this.data);            }
    visitInt            (_node: Int)             { return new IntVector(this.data);             }
    visitFloat          (_node: Float)           { return new FloatVector(this.data);           }
    visitBinary         (_node: Binary)          { return new BinaryVector(this.data);          }
    visitUtf8           (_node: Utf8)            { return new Utf8Vector(this.data);            }
    visitBool           (_node: Bool)            { return new BoolVector(this.data);            }
    visitDecimal        (_node: Decimal)         { return new DecimalVector(this.data);         }
    visitDate           (_node: Date_)           { return new DateVector(this.data);            }
    visitTime           (_node: Time)            { return new TimeVector(this.data);            }
    visitTimestamp      (_node: Timestamp)       { return new TimestampVector(this.data);       }
    visitInterval       (_node: Interval)        { return new IntervalVector(this.data);        }
    visitList           (_node: List)            { return new ListVector(this.data);            }
    visitStruct         (_node: Struct)          { return new StructVector(this.data);          }
    visitUnion          (_node: Union)           { return new UnionVector(this.data);           }
    visitFixedSizeBinary(_node: FixedSizeBinary) { return new FixedSizeBinaryVector(this.data); }
    visitFixedSizeList  (_node: FixedSizeList)   { return new FixedSizeListVector(this.data);   }
    visitMap            (_node: Map_)            { return new MapVector(this.data);             }
    visitDictionary     (_node: Dictionary)      { return new DictionaryVector(this.data);      }
});

import { Data, Buffers } from '../data';
import { createBuilderFromType } from './index';
import {
    DataType,
    Float, Int, Date_, Interval, Time, Timestamp,
    Utf8, Binary, Decimal, FixedSizeBinary, /* FixedSizeList, Union, DenseUnion, SparseUnion, */
} from '../type';

type TArrayType = { BYTES_PER_ELEMENT: number };
const roundUpTo = ({ BYTES_PER_ELEMENT: BPE }: TArrayType, length = 0, to = 1) => (((length * BPE) + (to - 1)) & ~(to - 1)) / BPE;

export class DataBuilder<T extends DataType = any> extends Data<T> {

    public static new <T extends DataType = any>(type: T, nullValues?: any[], chunkLength: number | undefined = 1024): DataBuilder<T> {
        return createBuilderFromType(type, nullValues, chunkLength) as any;
    }

    public length = 0;
    public chunkLength = 1024;
    public nullable: boolean;
    public nullValues: Map<any, any>;

    // @ts-ignore
    public valueOffsets: Int32Array;
    // @ts-ignore
    public values: T['TArray'];
    // @ts-ignore
    public nullBitmap: Uint8Array;
    // @ts-ignore
    public typeIds: T['TArray'];

    constructor(type: T, nullValues: any[] = [null], chunkLength = 1024) {
        super(type, 0, 0, 0, [undefined]);
        this.useChunkLength(chunkLength);
        this.nullable = nullValues.length > 0;
        this.nullValues = nullValues.reduce((m, x) => m.set(x, x), new Map());
    }

    public useChunkLength(chunkLength: number) {
        this.chunkLength = Math.max(chunkLength, 1);
        return this;
    }

    public useNullValues(nullValues: any[]) {
        this.nullable = nullValues.length > 0;
        this.nullValues = nullValues.reduce((m, x) => m.set(x, x), new Map());
        return this;
    }

    public set(value: any, index?: number) {
        return this.nullable && this.nullValues.has(value)
            ? this.setNull(index)
            : this.setValue(value, index);
    }

    public reset() {
        this.length = 0;
        this._nullCount = 0;
        this.values = undefined;
        this.typeIds = undefined;
        this.nullBitmap = undefined as any;
        this.valueOffsets = undefined as any;
        return this;
    }

    public finish() { return this; }

    public flush() {
        const { type, nullCount, length, offset } = this;
        let { valueOffsets, values, nullBitmap, typeIds } = this;
        typeIds = typeIds ? typeIds.subarray(0, length) : undefined;
        valueOffsets = valueOffsets ? valueOffsets.subarray(0, length + 1) : undefined as any;
        valueOffsets || (values = values ? values.subarray(0, roundUpTo(values, length, 8) * this.stride) : undefined);
        nullBitmap = nullBitmap ? nullBitmap.subarray(0, roundUpTo(nullBitmap, length << 3, 64) >> 3) : new Uint8Array(0);
        const data = this.clone(type, offset, length, nullCount, [valueOffsets, values, nullBitmap, typeIds] as Buffers<T>);
        this.reset();
        return data;
    }

    public setNull(index = this.length): number {
        ++this._nullCount;
        this.getNullBitmap()[index >> 3] &= ~(1 << (index % 8));
        return (this.length = Math.max(index | 0, this.length) + 1);
    }

    public setValue(_value: any, index = this.length): number {
        return (this.length = Math.max(index | 0, this.length) + 1);
    }

    protected getNullBitmap(length?: number) {
        let buf = this.nullBitmap;
        if (!buf) {
            this.nullBitmap = buf = new Uint8Array((((length === undefined ? this.chunkLength : length) + 63) & ~63) >> 3).fill(255);
        } else if (((length === undefined ? this.length : length) >> 3) >= buf.length) {
            const cur = this.nullBitmap!;
            buf = new Uint8Array(buf.length * 2);
            buf.fill(255, cur.length);
            buf.set(cur);
            this.nullBitmap = buf;
        }
        return buf;
    }

    protected getOffsetsBuffer(length?: number) {
        let buf = this.valueOffsets;
        if (!buf) {
            this.valueOffsets = buf = new Int32Array((length === undefined ? this.chunkLength : length) + 1);
        } else if ((length === undefined ? this.length : length) >= buf.length) {
            buf = new Int32Array(1 + (buf.length - 1) * 2);
            buf.set(this.valueOffsets!);
            this.valueOffsets = buf;
        }
        return buf;
    }

    protected getValuesBuffer(length?: number): T['TArray'] {
        let buf = this.values;
        if (!buf) {
            this.values = buf = new this.ArrayType((length === undefined ? this.chunkLength : length) * this.stride);
        } else if ((length === undefined ? this.length : length) >= (buf.length / this.stride)) {
            buf = new this.ArrayType(buf.length * 2) as ArrayBufferView;
            buf.set(this.values!);
            this.values = buf;
        }
        return buf;
    }

    protected getTypeIdsBuffer(length?: number) {
        let typeIds = this.typeIds;
        if (!typeIds) {
            this.typeIds = typeIds = new Int32Array(length === undefined ? this.chunkLength : length);
        } else if ((length === undefined ? this.length : length) >= typeIds.length) {
            typeIds = new Int32Array(typeIds.length * 2);
            typeIds.set(this.typeIds!);
            this.typeIds = typeIds;
        }
        return typeIds;
    }

    public throughIterable(chunkLength?: number) {
        return (source: Iterable<any>) => this.fromIterable(source, chunkLength);
    }

    public fromIterable(source: Iterable<any>, chunkLength?: number) {
        return this.nullable
            ? this.fromIterableNullable(source, chunkLength)
            : this.fromIterableNonNullable(source, chunkLength);
    }

    public *fromIterableNonNullable(source: Iterable<any>, chunkLength = Infinity) {
        for (const value of source) {
            if (this.setValue(value) >= chunkLength) {
                yield this.flush();
            }
        }
        if (this.finish().length > 0) yield this.flush();
    }

    public *fromIterableNullable(source: Iterable<any>, chunkLength = Infinity) {
        for (const value of source) {
            if (this.set(value) >= chunkLength) {
                yield this.flush();
            }
        }
        if (this.finish().length > 0) yield this.flush();
    }
}

export abstract class FlatBuilder<T extends Int | Float | FixedSizeBinary | Date_ | Timestamp | Time | Decimal | Interval = any> extends DataBuilder<T> {
    protected _setValue(_self: this, _index: number, _value: T['TValue']): void {}
    public setValue(value: T['TValue'], index = this.length) {
        this.getValuesBuffer();
        this._setValue(this, index, value);
        return super.setValue(value, index);
    }
}

export abstract class FlatListBuilder<T extends Utf8 | Binary = any> extends DataBuilder<T> {

    protected _pending?: (null | T['TValue'])[];
    protected _setValue(_self: this, _index: number, _value: T['TValue']): void {}

    public setNull(index = this.length) {
        const length = super.setNull(index);
        const valueOffsets = this.getOffsetsBuffer();
        valueOffsets[++index] = valueOffsets[--index];
        (this._pending || (this._pending = []))[index] = null;
        return length;
    }

    public setValue(value: any, index = this.length) {
        const length = super.setValue(value, index);
        const valueOffsets = this.getOffsetsBuffer();
        valueOffsets[++index] = valueOffsets[--index] + value.length;
        (this._pending || (this._pending = []))[index] = value;
        return length;
    }

    public flush() {
        this._pending && ((xs, n) => {
            let i = -1, x: T['TValue'] | null;
            while (++i < n) {
                if ((x = xs[i]) !== null) {
                    this._setValue(this, i, x);
                }
            }
        })(this._pending, this.length);
        this._pending = undefined;
        return super.flush();
    }
}

// export abstract class ListTypeBuilder<T extends Utf8 | Binary | List | FixedSizeList = any> extends VectorBuilder<T> {
//     // @ts-ignore
//     protected _setValue(self: this, index: number, value: T['TValue']): void {}
//     public writeValue(value: T['TValue'], index = this.length) {
//     }
// }

// export abstract class NestedBuilder<T extends Struct | Map_ | Union = any> extends VectorBuilder<T> {
//     // @ts-ignore
//     protected _setValue(self: this, index: number, value: T['TValue']): void {}
//     public writeValue(value: T['TValue'], index = this.length) {
//     }
// }

// export abstract class UnionTypeBuilder<T extends Union = any> extends VectorBuilder<T> {
//     // @ts-ignore
//     protected _setValue(self: this, index: number, value: T['TValue']): void {}
//     public writeValue(value: T['TValue'], index = this.length) {
//     }
// }


// class NullBuilder<T extends Null> extends FlatBuilder<T> {}
// export class ListBuilder<T extends List> extends ListTypeBuilder<T> {}
// export class StructBuilder<T extends Struct> extends NestedBuilder<T> {}
// export class UnionBuilder<T extends Union> extends UnionTypeBuilder<T> {}
// export class DenseUnionBuilder<T extends DenseUnion> extends UnionBuilder<T> {}
// export class SparseUnionBuilder<T extends SparseUnion> extends UnionBuilder<T> {}
// export class FixedSizeListBuilder<T extends FixedSizeList> extends ListTypeBuilder<T> {}
// export class MapBuilder<T extends Map_> extends NestedBuilder<T> {}

// export interface ListBuilder<T extends List> extends ListTypeBuilder<T> { nullBitmap: Uint8Array; valueOffsets: Int32Array; }
// export interface StructBuilder<T extends Struct> extends NestedBuilder<T> { nullBitmap: Uint8Array; }
// export interface UnionBuilder<T extends Union> extends UnionTypeBuilder<T> { nullBitmap: Uint8Array; typeIds: T['TArray']; }
// export interface DenseUnionBuilder<T extends DenseUnion> extends UnionBuilder<T> { nullBitmap: Uint8Array; valueOffsets: Int32Array; typeIds: T['TArray']; }
// export interface SparseUnionBuilder<T extends SparseUnion> extends UnionBuilder<T> { nullBitmap: Uint8Array; typeIds: T['TArray']; }
// export interface FixedSizeListBuilder<T extends FixedSizeList> extends ListTypeBuilder<T> { nullBitmap: Uint8Array; }
// export interface MapBuilder<T extends Map_> extends NestedBuilder<T> { nullBitmap: Uint8Array; }

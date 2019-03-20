import { Type } from '../enum';
import { setBool } from '../util/bit';
import { memcpy } from '../util/buffer';
import { Data, Buffers } from '../data';
import { createBuilderFromType } from './index';
import { TypedArray, TypedArrayConstructor } from '../interfaces';
import {
    DataType,
    Float, Int, Date_, Interval, Time, Timestamp,
    Utf8, Binary, Decimal, FixedSizeBinary, FixedSizeList, /* Union, DenseUnion, SparseUnion, */
} from '../type';

function sliceOrExtendArray<T extends TypedArray>(array: T, alignedLength = 0) {
    return array.length >= alignedLength
        ? array.subarray(0, alignedLength) as T
        : memcpy(new (array.constructor as TypedArrayConstructor<T>)(alignedLength), array, 0) as T;
}

export class DataBuilder<T extends DataType = any> {

    /** @nocollapse */
    public static new <T extends DataType = any>(type: T, nullValues?: any[], chunkLength: number | undefined = 1024): DataBuilder<T> {
        return createBuilderFromType(type, nullValues, chunkLength) as any;
    }

    public length = 0;
    public offset = 0;
    public nullCount = 0;
    public chunkLength = 1024;
    public nullable: boolean;
    public nullValues: Map<any, any>;
    public readonly stride: number;

    public readonly type: T;
    public readonly childData: DataBuilder[];
    // @ts-ignore
    public valueOffsets: Int32Array;
    // @ts-ignore
    public values: T['TArray'];
    // @ts-ignore
    public nullBitmap: Uint8Array;
    // @ts-ignore
    public typeIds: T['TArray'];

    constructor(type: T, nullValues: any[] = [null], chunkLength = 1024) {
        this.type = type;
        this.nullCount = 0;
        this.childData = [];
        this.useChunkLength(chunkLength);
        this.nullable = nullValues.length > 0;
        this.nullValues = nullValues.reduce((m, x) => m.set(x, x), new Map());
        const t: any = type;
        switch (type.typeId) {
            case Type.Decimal: this.stride = 4; break;
            case Type.Timestamp: this.stride = 2; break;
            case Type.Date: this.stride = 1 + (t as Date_).unit; break;
            case Type.Interval: this.stride = 1 + (t as Interval).unit; break;
            case Type.Int: this.stride = 1 + +((t as Int).bitWidth > 32); break;
            case Type.Time: this.stride = 1 + +((t as Time).bitWidth > 32); break;
            case Type.FixedSizeList: this.stride = (t as FixedSizeList).listSize; break;
            case Type.FixedSizeBinary: this.stride = (t as FixedSizeBinary).byteWidth; break;
            default: this.stride = 1;
        }
    }

    public get ArrayType() { return this.type.ArrayType; }

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
            ? this.setValid(index, false)
            : this.setValue(value, index);
    }

    public reset() {
        this.length = 0;
        this.nullCount = 0;
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
        
        const BPE = values && values.BYTES_PER_ELEMENT || 0;
        // pad out to 64 bytes
        const valueOffsetsLength = ((((length + 1) * 4) + 63) & ~63) / 4;
        // pad out to 64 bytes
        const valuesLength = ((((length * this.stride) * BPE) + 63) & ~63) / BPE;
        // pad to 8 bytes if fewer than 64 elements, otherwise pad to 64 bytes
        const nullBitmapLength = length < 64 ? ((length + 7) & ~7) : ((length + 63) & ~63);
        // pad out to 64 bytes
        const typeIdsLength = (((length * 4) + 63) & ~63) / 4;

        if (typeIds) {
            // pad out to 64 bytes
            typeIds = sliceOrExtendArray(typeIds, typeIdsLength);
        }

        if (valueOffsets) {
            valueOffsets = sliceOrExtendArray(valueOffsets, valueOffsetsLength);
        } else if (values) {
            values = sliceOrExtendArray(values, valuesLength);
        }
        
        nullBitmap = nullBitmap ? sliceOrExtendArray(nullBitmap, nullBitmapLength) : new Uint8Array(0);

        const data = new Data(
            type, offset, length, nullCount,
            [valueOffsets, values, nullBitmap, typeIds] as Buffers<T>,
            this.childData.map((child) => child.flush())) as Data<T>;

        this.reset();

        return data;
    }

    public setValue(_value: any, index = this.length): number {
        return (this.length = this.setValid(index, true));
    }

    public setValid(index = this.length, isValid: boolean): number {
        isValid || ++this.nullCount;
        setBool(this._growOrRetrieveNullBitmap(), index, isValid);
        return (this.length = Math.max(index, this.length) + 1);
    }

    public throughIterable(chunkLength?: number) {
        return (source: Iterable<any>) => this.fromIterable(source, chunkLength);
    }

    public throughAsyncIterable(chunkLength?: number) {
        return (source: Iterable<any> | AsyncIterable<any>) => this.fromAsyncIterable(source, chunkLength);
    }

    public fromIterable(source: Iterable<any>, chunkLength?: number) {
        return this.nullable
            ? this.fromIterableNullable(source, chunkLength)
            : this.fromIterableNonNullable(source, chunkLength);
    }

    public fromAsyncIterable(source: Iterable<any> | AsyncIterable<any>, chunkLength?: number) {
        return this.nullable
            ? this.fromAsyncIterableNullable(source, chunkLength)
            : this.fromAsyncIterableNonNullable(source, chunkLength);
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

    public async *fromAsyncIterableNonNullable(source: Iterable<any> | AsyncIterable<any>, chunkLength = Infinity) {
        for await (const value of source) {
            if (this.setValue(value) >= chunkLength) {
                yield this.flush();
            }
        }
        if (this.finish().length > 0) yield this.flush();
    }

    public async *fromAsyncIterableNullable(source: Iterable<any> | AsyncIterable<any>, chunkLength = Infinity) {
        for await (const value of source) {
            if (this.set(value) >= chunkLength) {
                yield this.flush();
            }
        }
        if (this.finish().length > 0) yield this.flush();
    }

    protected _growOrRetrieveNullBitmap(length?: number) {
        let len: number;
        let buf = this.nullBitmap;
        if (!buf) {
            len = length === undefined ? this.chunkLength : length;
            // pad to 8 bytes if fewer than 64 elements, otherwise pad to 64 bytes
            len = (len < 64 ? ((len + 63) & ~63) : ((len + 511) & ~511)) >> 3;
            this.nullBitmap = buf = new Uint8Array(Math.max(len, 8));
        } else {
            len = this.length;
            len = Math.max(len, length === undefined ? len : length);
            if ((len >> 3) >= buf.length) {
                // pad to 8 bytes if fewer than 64 elements, otherwise pad to 64 bytes
                len = (len < 64 ? ((len + 63) & ~63) : ((len + 511) & ~511)) >> 3;
                this.nullBitmap = buf = memcpy(new Uint8Array(len * 2), buf);
            }
        }
        return buf;
    }

    protected _growOrRetrieveValueOffsets(length?: number) {
        let len: number;
        let buf = this.valueOffsets;
        if (!buf) {
            len = length === undefined ? this.chunkLength : length;
            len = ((((len + 1) * 4) + 63) & ~63) / 4; // pad out to 64 bytes
            this.valueOffsets = buf = new Int32Array(len);
        } else {
            len = this.length;
            len = Math.max(len, length === undefined ? len : length);
            if (len >= buf.length) {
                len = ((((len * 2 + 1) * 4) + 63) & ~63) / 4; // pad out to 64 bytes
                this.valueOffsets = buf = memcpy(new Int32Array(len), buf);
            }
        }
        return buf;
    }

    protected _growOrRetrieveValues(length?: number): T['TArray'] {
        let len: number;
        let BPE: number;
        let buf = this.values;
        if (!buf) {
            BPE = this.ArrayType.BYTES_PER_ELEMENT;
            len = (length === undefined ? this.chunkLength : length) * this.stride;
            len = (((len * BPE) + 63) & ~63) / BPE; // pad out to 64 bytes
            this.values = buf = new this.ArrayType(len);
        } else {
            len = this.length;
            len = Math.max(len, length === undefined ? len : length) * this.stride;
            if (len >= buf.length) {
                BPE = buf.BYTES_PER_ELEMENT;
                len = (((len * BPE) + 63) & ~63) / BPE; // pad out to 64 bytes
                this.values = buf = memcpy(new this.ArrayType(len * 2), buf);
            }
        }
        return buf;
    }

    protected _growOrRetrieveTypeIds(length?: number) {
        let len: number;
        let buf = this.typeIds;
        if (!buf) {
            len = length === undefined ? this.chunkLength : length;
            len = (((len * 4) + 63) & ~63) / 4; // pad out to 64 bytes
            this.typeIds = buf = new Int32Array(len);
        } else {
            len = this.length;
            len = Math.max(len, length === undefined ? len : length);
            if (len >= buf.length) {
                len = (((len * 4) + 63) & ~63) / 4; // pad out to 64 bytes
                this.typeIds = buf = memcpy(new Int32Array(len * 2), buf);
            }
        }
        return buf;
    }
}

export abstract class FlatBuilder<T extends Int | Float | FixedSizeBinary | Date_ | Timestamp | Time | Decimal | Interval = any> extends DataBuilder<T> {
    protected _setValue(_self: this, _index: number, _value: T['TValue']): void {}
    public setValue(value: T['TValue'], index = this.length) {
        this._growOrRetrieveValues();
        this._setValue(this, index, value);
        return super.setValue(value, index);
    }
}

export abstract class FlatListBuilder<T extends Utf8 | Binary = any> extends DataBuilder<T> {

    protected _pending?: (null | T['TValue'])[];
    protected _setValue(_self: this, _index: number, _value: T['TValue']): void {}

    public setValid(index = this.length, isValid: boolean) {
        const length = super.setValid(index, isValid);
        const valueOffsets = this._growOrRetrieveValueOffsets();
        valueOffsets[++index] = valueOffsets[--index];
        (this._pending || (this._pending = []))[index] = null;
        return length;
    }

    public setValue(value: any, index = this.length) {
        const length = super.setValue(value, index);
        const valueOffsets = this._growOrRetrieveValueOffsets();
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

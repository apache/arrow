import { Type } from '../enum';
import { setBool } from '../util/bit';
import { memcpy } from '../util/buffer';
import { Data, Buffers } from '../data';
import { createBuilderFromType } from './index';
import { TypedArray, TypedArrayConstructor } from '../interfaces';
import {
    DataType,
    Bool, Float, Int, Date_, Interval, Time, Timestamp,
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
    public readonly children: DataBuilder[];
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
        this.children = [];
        this.nullable = nullValues.length > 0;
        this.chunkLength = Math.max(chunkLength, 1);
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

    public set(value: any, index = this.length) {
        let valid = !(this.nullable && this.nullValues.has(value));
        this.setValid(valid, index) && this.setValue(value, index);
        return (this.length = Math.max(index, this.length) + 1);
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

    public finish() {
        this.children.forEach((child) => child.finish());
        return this;
    }

    public flush() {

        const { type, nullCount, length, offset } = this;
        let { valueOffsets, values, nullBitmap, typeIds } = this;
        
        if (valueOffsets) {
            // pad out to 64 bytes
            valueOffsets = sliceOrExtendArray(valueOffsets, ((((length + 1) * 4) + 63) & ~63) / 4);
            // pad out to 64 bytes
            values && (values = sliceOrExtendArray(values, ((values.length + 63) & ~63) / values.BYTES_PER_ELEMENT));
        } else if (values) {
            // pad out to 64 bytes
            values = sliceOrExtendArray(values, ((((length * this.stride) * values.BYTES_PER_ELEMENT) + 63) & ~63) / values.BYTES_PER_ELEMENT);
        }

        // pad out to 64 bytes
        nullBitmap && (nullBitmap = sliceOrExtendArray(nullBitmap, ((length + 511) & ~511) >> 3));
        // pad out to 64 bytes
        typeIds && (typeIds = sliceOrExtendArray(typeIds, (((length * 4) + 63) & ~63) / 4));

        const data = Data.new(
            type, offset, length, nullCount,
            [valueOffsets, values, nullBitmap, typeIds] as Buffers<T>,
            this.children.map((child) => child.flush())) as Data<T>;

        this.reset();

        return data;
    }

    public setValue(value: T['TValue'], _index?: number): T['TValue'] {
        return value;
    }

    public setValid(isValid: boolean, index = this.length): boolean {
        isValid || ++this.nullCount;
        setBool(this._growOrRetrieveNullBitmap(index), index, isValid);
        return isValid;
    }

    public throughIterable(chunkLength?: number) {
        return (source: Iterable<any>) => this.fromIterable(source, chunkLength);
    }

    public throughAsyncIterable(chunkLength?: number) {
        return (source: Iterable<any> | AsyncIterable<any>) => this.fromAsyncIterable(source, chunkLength);
    }

    public *fromIterable(source: Iterable<any>, chunkLength = Infinity) {
        for (const value of source) {
            if (this.set(value) >= chunkLength) {
                yield this.flush();
            }
        }
        if (this.finish().length > 0) yield this.flush();
    }

    public async *fromAsyncIterable(source: Iterable<any> | AsyncIterable<any>, chunkLength = Infinity) {
        for await (const value of source) {
            if (this.set(value) >= chunkLength) {
                yield this.flush();
            }
        }
        if (this.finish().length > 0) yield this.flush();
    }

    protected _growOrRetrieveNullBitmap(length?: number) {
        let buf = this.nullBitmap;
        let len = !buf ? this.chunkLength : this.length;
        length === undefined || (len = Math.max(len, length));
        if (!buf) {
            this.nullBitmap = buf = new Uint8Array((((len << 3) + 511) & ~511) >> 3); // pad out to 64 bytes
        } else if ((len >> 3) >= buf.length) {
            this.nullBitmap = buf = memcpy(new Uint8Array((((len << 3) + 511) & ~511) >> 3 * 2), buf); // pad out to 64 bytes
        }
        return buf;
    }

    protected _growOrRetrieveValueOffsets(length?: number) {
        let buf = this.valueOffsets;
        let len = !buf ? this.chunkLength : this.length;
        length === undefined || (len = Math.max(len, length));
        if (!buf) {
            this.valueOffsets = buf = new Int32Array(((((len + 1) * 4) + 63) & ~63) / 4); // pad out to 64 bytes
        } else if (len >= buf.length - 1) {
            this.valueOffsets = buf = memcpy(new Int32Array(((((len + 1) * 4) + 63) & ~63) / 4 * 2), buf); // pad out to 64 bytes
        }
        return buf;
    }

    protected _growOrRetrieveValues(length?: number): T['TArray'] {
        let BPE: number;
        let ArrayType: any;
        let buf = this.values;
        let len = !buf ? this.chunkLength : this.length;
        len = (length === undefined ? len : Math.max(len, length)) * this.stride;
        if (!buf) {
            BPE = (ArrayType = this.ArrayType).BYTES_PER_ELEMENT;
            this.values = buf = new ArrayType((((len * BPE) + 63) & ~63) / BPE); // pad out to 64 bytes
        } else if (len >= buf.length) {
            BPE = (ArrayType = this.ArrayType).BYTES_PER_ELEMENT;
            this.values = buf = memcpy(new ArrayType((((len * BPE) + 63) & ~63) / BPE * 2), buf); // pad out to 64 bytes
        }
        return buf;
    }

    protected _growOrRetrieveTypeIds(length?: number) {
        let buf = this.typeIds;
        let len = !buf ? this.chunkLength : this.length;
        length === undefined || (len = Math.max(len, length));
        if (!buf) {
            this.typeIds = buf = new Int32Array((((len * 4) + 63) & ~63) / 4); // pad out to 64 bytes
        } else if (len >= buf.length) {
            this.typeIds = buf = memcpy(new Int32Array((((len * 4) + 63) & ~63) / 4 * 2), buf); // pad out to 64 bytes
        }
        return buf;
    }
}

export abstract class FlatBuilder<T extends Bool | Int | Float | FixedSizeBinary | Date_ | Timestamp | Time | Decimal | Interval = any> extends DataBuilder<T> {
    protected _setValue(_self: this, _index: number, _value: T['TValue']): void {}
    public setValue(value: T['TValue'], index = this.length) {
        this._growOrRetrieveValues(index);
        this._setValue(this, index, value);
        return super.setValue(value, index);
    }
}

export abstract class FlatListBuilder<T extends Utf8 | Binary = any> extends DataBuilder<T> {

    protected _values?: Map<number, undefined | T['TValue']>;
    protected _setValue(_self: this, _index: number, _value: T['TValue']): void {}

    public setValid(isValid: boolean, index: number) {
        if (!super.setValid(isValid, index)) {
            const valueOffsets = this._growOrRetrieveValueOffsets(index);
            for (let i = this.length; i <= index; i++) {
                valueOffsets[i + 1] = valueOffsets[i];
            }
        }
        return isValid;
    }

    public setValue(value: T['TValue'], index: number) {
        const valueOffsets = this._growOrRetrieveValueOffsets(index);
        valueOffsets[index + 1] = valueOffsets[index] + value.length;
        (this._values || (this._values = new Map())).set(index, value);
        return super.setValue(value, index);
    }

    public flush() {
        this._values && ((xs, n) => {
            let i = -1, x: T['TValue'] | undefined;
            while (++i < n) {
                if ((x = xs.get(i)) !== undefined) {
                    this._setValue(this, i, x);
                }
            }
        })(this._values, this.length);
        this._values = undefined;
        return super.flush();
    }
}

// export abstract class ListTypeBuilder<T extends Utf8 | Binary | List | FixedSizeList = any> extends VectorBuilder<T> {
//     // @ts-ignore
//     protected _setValue(self: this, index: number, value: T['TValue']): void {}
//     public setValue(value: T['TValue'], index = this.length) {
//     }
// }

// export abstract class NestedBuilder<T extends Struct | Map_ | Union = any> extends VectorBuilder<T> {
//     // @ts-ignore
//     protected _setValue(self: this, index: number, value: T['TValue']): void {}
//     public setValue(value: T['TValue'], index = this.length) {
//     }
// }

// export abstract class UnionTypeBuilder<T extends Union = any> extends VectorBuilder<T> {
//     // @ts-ignore
//     protected _setValue(self: this, index: number, value: T['TValue']): void {}
//     public setValue(value: T['TValue'], index = this.length) {
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

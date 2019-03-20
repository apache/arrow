import { FlatBuilder } from './base';
import { BN } from '../util/bn';
import { Int, Uint8, Uint16, Uint32, Uint64, Int8, Int16, Int32, Int64 } from '../type';

export interface IntBuilder<T extends Int> extends FlatBuilder<T> { nullBitmap: Uint8Array; values: T['TArray']; }
export interface Int8Builder extends IntBuilder<Int8> {}
export interface Int16Builder extends IntBuilder<Int16> {}
export interface Int32Builder extends IntBuilder<Int32> {}
export interface Int64Builder extends IntBuilder<Int64> {}
export interface Uint8Builder extends IntBuilder<Uint8> {}
export interface Uint16Builder extends IntBuilder<Uint16> {}
export interface Uint32Builder extends IntBuilder<Uint32> {}
export interface Uint64Builder extends IntBuilder<Uint64> {}

export class IntBuilder<T extends Int> extends FlatBuilder<T> {}

export class Int8Builder extends IntBuilder<Int8> {
    constructor(nullValues?: any[], chunkSize?: number) {
        super(new Int8(), nullValues, chunkSize);
    }
}

export class Int16Builder extends IntBuilder<Int16> {
    constructor(nullValues?: any[], chunkSize?: number) {
        super(new Int16(), nullValues, chunkSize);
    }
}

export class Int32Builder extends IntBuilder<Int32> {
    constructor(nullValues?: any[], chunkSize?: number) {
        super(new Int32(), nullValues, chunkSize);
    }
}

export class Int64Builder extends IntBuilder<Int64> {
    constructor(nullValues?: any[], chunkSize?: number) {
        super(new Int64(), (nullValues || []).map((x) => ArrayBuffer.isView(x) ? (<any> BN.new(x))[Symbol.toPrimitive]('default') : x), chunkSize);
    }
    public set(value: any, index?: number) {
        if (this.nullable) {
            const nullValues = this.nullValues;
            if (ArrayBuffer.isView(value)) {
                if (nullValues.has((<any> BN.new(value))[Symbol.toPrimitive]('default'))) {
                    return this.setValid(index, false);
                }
            } else if (nullValues.has(value)) {
                return this.setValid(index, false);
            }
        }
        return this.setValue(value, index);
    }
}

export class Uint8Builder extends IntBuilder<Uint8> {
    constructor(nullValues?: any[], chunkSize?: number) {
        super(new Uint8(), nullValues, chunkSize);
    }
}

export class Uint16Builder extends IntBuilder<Uint16> {
    constructor(nullValues?: any[], chunkSize?: number) {
        super(new Uint16(), nullValues, chunkSize);
    }
}

export class Uint32Builder extends IntBuilder<Uint32> {
    constructor(nullValues?: any[], chunkSize?: number) {
        super(new Uint32(), nullValues, chunkSize);
    }
}

export class Uint64Builder extends IntBuilder<Uint64> {
    constructor(nullValues?: any[], chunkSize?: number) {
        super(new Uint64(), (nullValues || []).map((x) => ArrayBuffer.isView(x) ? +BN.new(x) : x), chunkSize);
    }
    public set(value: any, index?: number) {
        if (this.nullable) {
            const nullValues = this.nullValues;
            if (ArrayBuffer.isView(value)) {
                if (nullValues.has(+BN.new(value))) {
                    return this.setValid(index, false);
                }
            } else if (nullValues.has(value)) {
                return this.setValid(index, false);
            }
        }
        return this.setValue(value, index);
    }
}

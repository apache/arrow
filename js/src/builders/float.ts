import { FlatBuilder } from './base';
import { Float, Float16, Float32, Float64 } from '../type';

export interface FloatBuilder<T extends Float>extends FlatBuilder<T> { nullBitmap: Uint8Array; values: T['TArray']; }
export interface Float16Builder extends FloatBuilder<Float16> {}
export interface Float32Builder extends FloatBuilder<Float32> {}
export interface Float64Builder extends FloatBuilder<Float64> {}

export class FloatBuilder<T extends Float> extends FlatBuilder<T> {}
export class Float16Builder extends FloatBuilder<Float16> {
    constructor(nullValues?: any[], chunkSize?: number) {
        super(new Float16(), nullValues, chunkSize);
    }
}
export class Float32Builder extends FloatBuilder<Float32> {
    constructor(nullValues?: any[], chunkSize?: number) {
        super(new Float32(), nullValues, chunkSize);
    }
}
export class Float64Builder extends FloatBuilder<Float64> {
    constructor(nullValues?: any[], chunkSize?: number) {
        super(new Float64(), nullValues, chunkSize);
    }
}

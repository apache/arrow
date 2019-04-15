import { FlatBuilder } from './base';
import { Float, Float16, Float32, Float64 } from '../type';

export interface FloatBuilder<T extends Float = Float, TNull = any>extends FlatBuilder<T, TNull> {
    nullBitmap: Uint8Array; values: T['TArray'];
}

export interface Float16Builder<TNull = any> extends FloatBuilder<Float16, TNull> {}
export interface Float32Builder<TNull = any> extends FloatBuilder<Float32, TNull> {}
export interface Float64Builder<TNull = any> extends FloatBuilder<Float64, TNull> {}

export class FloatBuilder<T extends Float = Float, TNull = any> extends FlatBuilder<T, TNull> {}
export class Float16Builder<TNull = any> extends FloatBuilder<Float16, TNull> {}
export class Float32Builder<TNull = any> extends FloatBuilder<Float32, TNull> {}
export class Float64Builder<TNull = any> extends FloatBuilder<Float64, TNull> {}

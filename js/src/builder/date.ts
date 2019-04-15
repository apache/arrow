import { FlatBuilder } from './base';
import { Date_, DateDay, DateMillisecond } from '../type';

export interface DateBuilder<T extends Date_ = Date_, TNull = any> extends FlatBuilder<T, TNull> {
    nullBitmap: Uint8Array;
    values: T['TArray'];
}

export interface DateDayBuilder<TNull = any> extends DateBuilder<DateDay, TNull> {}
export interface DateMillisecondBuilder<TNull = any> extends DateBuilder<DateMillisecond, TNull> {}

export class DateBuilder<T extends Date_ = Date_, TNull = any> extends FlatBuilder<T, TNull> {}
export class DateDayBuilder<TNull = any> extends DateBuilder<DateDay, TNull> {}
export class DateMillisecondBuilder<TNull = any> extends DateBuilder<DateMillisecond, TNull> {}

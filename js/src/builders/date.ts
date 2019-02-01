import { FlatBuilder } from './base';
import { Date_, DateDay, DateMillisecond } from '../type';

export interface DateBuilder<T extends Date_> extends FlatBuilder<T> { nullBitmap: Uint8Array; values: T['TArray']; }
export interface DateDayBuilder extends DateBuilder<DateDay> {}
export interface DateMillisecondBuilder extends DateBuilder<DateMillisecond> {}

export class DateBuilder<T extends Date_> extends FlatBuilder<T> {}
export class DateDayBuilder extends DateBuilder<DateDay> {
    constructor(nullValues?: any[], chunkSize?: number) {
        super(new DateDay(), nullValues, chunkSize);
    }
}
export class DateMillisecondBuilder extends DateBuilder<DateMillisecond> {
    constructor(nullValues?: any[], chunkSize?: number) {
        super(new DateMillisecond(), nullValues, chunkSize);
    }
}

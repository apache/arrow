import { FlatBuilder } from './base';
import { Interval, IntervalDayTime, IntervalYearMonth } from '../type';

export interface IntervalBuilder<T extends Interval> extends FlatBuilder<T> { nullBitmap: Uint8Array; values: T['TArray']; }
export interface IntervalDayTimeBuilder extends IntervalBuilder<IntervalDayTime> {}
export interface IntervalYearMonthBuilder extends IntervalBuilder<IntervalYearMonth> {}

export class IntervalBuilder<T extends Interval> extends FlatBuilder<T> {}
export class IntervalDayTimeBuilder extends IntervalBuilder<IntervalDayTime> {
    constructor(nullValues?: any[], chunkSize?: number) {
        super(new IntervalDayTime(), nullValues, chunkSize);
    }
}
export class IntervalYearMonthBuilder extends IntervalBuilder<IntervalYearMonth> {
    constructor(nullValues?: any[], chunkSize?: number) {
        super(new IntervalYearMonth(), nullValues, chunkSize);
    }
}

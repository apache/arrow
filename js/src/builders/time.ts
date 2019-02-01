import { FlatBuilder } from './base';
import { Time, TimeSecond, TimeMillisecond, TimeMicrosecond, TimeNanosecond } from '../type';

export interface TimeBuilder<T extends Time> extends FlatBuilder<T> { nullBitmap: Uint8Array; values: T['TArray']; }
export interface TimeSecondBuilder extends TimeBuilder<TimeSecond> {}
export interface TimeMillisecondBuilder extends TimeBuilder<TimeMillisecond> {}
export interface TimeMicrosecondBuilder extends TimeBuilder<TimeMicrosecond> {}
export interface TimeNanosecondBuilder extends TimeBuilder<TimeNanosecond> {}

export class TimeBuilder<T extends Time> extends FlatBuilder<T> {}
export class TimeSecondBuilder extends TimeBuilder<TimeSecond> {
    constructor(nullValues?: any[], chunkSize?: number) {
        super(new TimeSecond(), nullValues, chunkSize);
    }
}
export class TimeMillisecondBuilder extends TimeBuilder<TimeMillisecond> {
    constructor(nullValues?: any[], chunkSize?: number) {
        super(new TimeMillisecond(), nullValues, chunkSize);
    }
}
export class TimeMicrosecondBuilder extends TimeBuilder<TimeMicrosecond> {
    constructor(nullValues?: any[], chunkSize?: number) {
        super(new TimeMicrosecond(), nullValues, chunkSize);
    }
}
export class TimeNanosecondBuilder extends TimeBuilder<TimeNanosecond> {
    constructor(nullValues?: any[], chunkSize?: number) {
        super(new TimeNanosecond(), nullValues, chunkSize);
    }
}

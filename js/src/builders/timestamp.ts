import { FlatBuilder } from './base';
import { Timestamp, TimestampSecond, TimestampMillisecond, TimestampMicrosecond, TimestampNanosecond } from '../type';

export interface TimestampBuilder<T extends Timestamp> extends FlatBuilder<T> {
    nullBitmap: Uint8Array;
    values: T['TArray'];
}
export interface TimestampSecondBuilder extends TimestampBuilder<TimestampSecond> {}
export interface TimestampMillisecondBuilder extends TimestampBuilder<TimestampMillisecond> {}
export interface TimestampMicrosecondBuilder extends TimestampBuilder<TimestampMicrosecond> {}
export interface TimestampNanosecondBuilder extends TimestampBuilder<TimestampNanosecond> {}

export class TimestampBuilder<T extends Timestamp> extends FlatBuilder<T> {}
export class TimestampSecondBuilder extends TimestampBuilder<TimestampSecond> {
    constructor(nullValues?: any[], chunkSize?: number) {
        super(new TimestampSecond(), nullValues, chunkSize);
    }
}
export class TimestampMillisecondBuilder extends TimestampBuilder<TimestampMillisecond> {
    constructor(nullValues?: any[], chunkSize?: number) {
        super(new TimestampMillisecond(), nullValues, chunkSize);
    }
}
export class TimestampMicrosecondBuilder extends TimestampBuilder<TimestampMicrosecond> {
    constructor(nullValues?: any[], chunkSize?: number) {
        super(new TimestampMicrosecond(), nullValues, chunkSize);
    }
}
export class TimestampNanosecondBuilder extends TimestampBuilder<TimestampNanosecond> {
    constructor(nullValues?: any[], chunkSize?: number) {
        super(new TimestampNanosecond(), nullValues, chunkSize);
    }
}

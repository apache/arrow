import { FlatBuilder } from './base';
import { FixedSizeBinary } from '../type';

export interface FixedSizeBinaryBuilder<TNull = any> extends FlatBuilder<FixedSizeBinary, TNull> {
    values: Uint8Array;
    nullBitmap: Uint8Array;
}

export class FixedSizeBinaryBuilder<TNull = any> extends FlatBuilder<FixedSizeBinary, TNull> {}

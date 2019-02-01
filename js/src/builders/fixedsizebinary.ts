import { FlatBuilder } from './base';
import { FixedSizeBinary } from '../type';

export interface FixedSizeBinaryBuilder<T extends FixedSizeBinary> extends FlatBuilder<T> {
    nullBitmap: Uint8Array;
    values: T['TArray'];
}

export class FixedSizeBinaryBuilder<T extends FixedSizeBinary> extends FlatBuilder<T> {}

import { Decimal } from '../type';
import { FlatBuilder } from './base';

export interface DecimalBuilder<TNull = any> extends FlatBuilder<Decimal, TNull> {
    nullBitmap: Uint8Array;
    values: Decimal['TArray'];
}

export class DecimalBuilder<TNull = any> extends FlatBuilder<Decimal, TNull> {}

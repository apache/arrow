import { FlatBuilder } from './base';
import { Decimal } from '../type';

export interface DecimalBuilder<T extends Decimal> extends FlatBuilder<T> { nullBitmap: Uint8Array; values: T['TArray']; }

export class DecimalBuilder<T extends Decimal> extends FlatBuilder<T> {}

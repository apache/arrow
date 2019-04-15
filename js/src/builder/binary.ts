import { Binary } from '../type';
import { FlatListBuilder } from './base';

export interface BinaryBuilder<TNull = any> extends FlatListBuilder<Binary, TNull> {
    nullBitmap: Uint8Array;
    valueOffsets: Int32Array;
    values: Uint8Array;
}

export class BinaryBuilder<TNull = any> extends FlatListBuilder<Binary, TNull> {
    public writeValue(value: Uint8Array, index = this.length) {
        return super.writeValue(value, index);
    }
}

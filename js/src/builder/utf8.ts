import { Utf8 } from '../type';
import { FlatListBuilder } from './base';
import { encodeUtf8 } from '../util/utf8';

export interface Utf8Builder<TNull = any> extends FlatListBuilder<Utf8, TNull> {
    nullBitmap: Uint8Array;
    valueOffsets: Int32Array;
    values: Uint8Array;
}

export class Utf8Builder<TNull = any> extends FlatListBuilder<Utf8, TNull> {
    public writeValue(value: string, index = this.length) {
        return super.writeValue(encodeUtf8(value), index);
    }
}

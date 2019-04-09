import { Utf8, Binary } from '../type';
import { FlatListBuilder } from './base';
import { encodeUtf8 } from '../util/utf8';

export interface Utf8Builder extends FlatListBuilder<Utf8 | Binary> {
    nullBitmap: Uint8Array;
    valueOffsets: Int32Array;
    values: Uint8Array;
}

export class Utf8Builder extends FlatListBuilder<Utf8 | Binary> {
    constructor(nullValues?: any[], chunkSize?: number) {
        super(new Utf8(), nullValues, chunkSize);
    }
    public setValue(value: string, index = this.length) {
        return super.setValue(encodeUtf8(value), index);
    }
    public flush() {
        this.values = this._growOrRetrieveValues(this.valueOffsets[this.length] || 0);
        return super.flush();
    }
}

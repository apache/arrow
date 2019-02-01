import { FlatListBuilder } from './base';
import { Utf8 } from '../type';

export interface Utf8Builder extends FlatListBuilder<Utf8> {
    nullBitmap: Uint8Array;
    valueOffsets: Int32Array;
    values: Uint8Array;
}

export class Utf8Builder extends FlatListBuilder<Utf8> {
    constructor(nullValues?: any[], chunkSize?: number) {
        super(new Utf8(), nullValues, chunkSize);
    }
    public setValue(value: string, index = this.length) {
        return super.setValue(Buffer.from(value, 'utf8'), index);
    }
    public flush() {
        this.values = this.getValuesBuffer(this.valueOffsets[this.length] || 0);
        return super.flush();
    }
}

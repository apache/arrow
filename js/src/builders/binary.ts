import { FlatListBuilder } from './base';
import { Binary } from '../type';

export interface BinaryBuilder extends FlatListBuilder<Binary> {
    nullBitmap: Uint8Array;
    valueOffsets: Int32Array;
    values: Uint8Array;
}

export class BinaryBuilder extends FlatListBuilder<Binary> {
    constructor(nullValues?: any[], chunkSize?: number) {
        super(new Binary(), nullValues, chunkSize);
    }
    public flush() {
        this.values = this._growOrRetrieveValues(this.valueOffsets[this.length] || 0);
        return super.flush();
    }
}

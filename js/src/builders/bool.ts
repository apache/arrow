import { Bool } from '../type';
import { FlatBuilder } from './base';
import { memcpy } from '../util/buffer';

export interface BoolBuilder extends FlatBuilder<Bool> {
    nullBitmap: Uint8Array;
    values: Uint8Array;
}

export class BoolBuilder extends FlatBuilder<Bool> {
    constructor(nullValues?: any[], chunkSize?: number) {
        super(new Bool(), nullValues, chunkSize);
    }
    protected _growOrRetrieveValues(length?: number) {
        let buf = this.values;
        let len = !buf ? this.chunkLength : this.length;
        length === undefined || (len = Math.max(len, length));
        if (!buf) {
            this.values = buf = new Uint8Array((((len << 3) + 511) & ~511) >> 3); // pad out to 64 bytes
        } else if ((len >> 3) >= buf.length) {
            this.values = buf = memcpy(new Uint8Array((((len << 3) + 511) & ~511) >> 3 * 2), buf); // pad out to 64 bytes
        }
        return buf;
    }
}

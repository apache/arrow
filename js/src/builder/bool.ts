import { Bool } from '../type';
import { Builder, DataBuilderOptions } from './base';

export class BoolBuilder<TNull = any> extends Builder<Bool, TNull> {
    constructor(options: DataBuilderOptions<Bool, TNull>) {
        super(options);
        this.values = new Uint8Array(0);
    }
    public writeValue(value: boolean, offset: number) {
        this._getValuesBitmap(offset);
        return super.writeValue(value, offset);
    }
    protected _updateBytesUsed(offset: number, length: number) {
        offset % 512 || (this._bytesUsed += 64);
        return super._updateBytesUsed(offset, length);
    }
}

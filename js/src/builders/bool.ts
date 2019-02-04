import { DataBuilder } from './base';
import { Bool } from '../type';
import { SetVisitor } from '../visitor/set';

const setBool = (SetVisitor.prototype as any).visitBool as {
    <T extends Bool>(builder: { values: T['TArray'] }, index: number, value: T['TValue']): void
};

export interface BoolBuilder extends DataBuilder<Bool> {
    nullBitmap: Uint8Array;
    values: Uint8Array;
}

export class BoolBuilder extends DataBuilder<Bool> {
    constructor(nullValues?: any[], chunkSize?: number) {
        super(new Bool(), nullValues, chunkSize);
    }
    protected getValuesBuffer(length?: number) {
        let buf = this.values;
        if (!buf) {
            this.values = buf = new this.ArrayType(Math.max(64, length === undefined ? this.chunkLength : length) >> 3);
        } else if (((length === undefined ? this.length : length) >> 3) >= buf.length) {
            buf = new this.ArrayType(buf.length * 2);
            buf.set(this.values);
            this.values = buf;
        }
        return buf;
    }
    public setValue(value: boolean, index = this.length) {
        const length = super.setValue(value, index);
        this.getValuesBuffer();
        setBool(this, index, value);
        return length;
    }
}

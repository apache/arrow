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
    public setValue(value: boolean, index = this.length) {
        const length = super.setValue(value, index);
        this.getValuesBuffer(length >> 3);
        setBool(this, index, value);
        return length;
    }
}

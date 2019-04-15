import { Null } from '../type';
import { Builder } from './base';

export class NullBuilder<TNull = any> extends Builder<Null, TNull> {
    public writeValue(value: null) { return value; }
    public writeValid(isValid: boolean) { return isValid; }
}

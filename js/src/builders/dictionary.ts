import { IntBuilder } from './int';
import { DataBuilder } from './base';
import { Vector } from '../vector';
import { Dictionary } from '../type';

type HashFunction = (x: any) => string | number;
const metrohash64: HashFunction = require('metrohash').metrohash64;

export class DictionaryBuilder<T extends Dictionary> extends DataBuilder<T> {

    protected hashmap = Object.create(null);
    protected _hash: (x: any) => string | number;
    public readonly indices: IntBuilder<T['indices']>;
    public readonly dictionary: DataBuilder<T['dictionary']>;

    constructor(type: T, nullValues?: any[], chunkSize?: number, hash: HashFunction = metrohash64) {
        super(type, nullValues, chunkSize);
        this._hash = hash;
        this.indices = DataBuilder.new(type.indices, nullValues, chunkSize) as IntBuilder<T['indices']>;
        this.dictionary = DataBuilder.new(type.dictionary, [], chunkSize) as DataBuilder<T['dictionary']>;
    }
    public get values() { return this.indices && this.indices.values; }
    public get nullBitmap() { return this.indices && this.indices.nullBitmap; }
    public set values(values: T['TArray']) { this.indices && (this.indices.values = values); }
    public set nullBitmap(nullBitmap: Uint8Array) { this.indices && (this.indices.nullBitmap = nullBitmap); }
    public setHashFunction(hash: HashFunction) {
        this._hash = hash;
        return this;
    }
    public reset() {
        this.length = 0;
        this.indices.reset();
        this.dictionary.reset();
        return this;
    }
    public flush() {
        const indices = this.indices;
        const data = indices.flush().clone(this.type);
        this.length = indices.length;
        return data;
    }
    public finish() {
        this.type.dictionaryVector = Vector.new(this.dictionary.finish().flush());
        return super.finish();
    }
    public setNull(index = this.length) {
        return (this.length = this.indices.setNull(index));
    }
    public setValue(value: T['TValue'], index = this.length) {
        let id = this._hash(value);
        let key = this.hashmap[id];
        if (key === undefined) {
            this.hashmap[id] = key = -1 + this.dictionary.setValue(value, key);
        }
        return (this.length = this.indices.setValue(key, index));
    }
}

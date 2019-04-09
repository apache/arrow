import { Data } from '../data';
import { Vector } from '../vector';
import { IntBuilder } from './int';
import { DataBuilder } from './base';
import { Dictionary } from '../type';

type HashFunction = (x: string) => string;
const defaultHashFunction = (x: any) => `${x}`;

export class DictionaryBuilder<T extends Dictionary> extends DataBuilder<T> {

    protected _hash: (x: any) => string;
    protected hashmap = Object.create(null);
    public readonly indices: IntBuilder<T['indices']>;
    public readonly dictionary: DataBuilder<T['dictionary']>;

    constructor(type: T, nullValues?: any[], chunkSize?: number, hash: HashFunction = defaultHashFunction) {
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
    public set(value: any, index?: number) {
        return (this.indices.length = super.set(value, index));
    }
    public setValid(isValid: boolean, index: number) {
        return this.indices.setValid(isValid, index);
    }
    public setValue(value: T['TValue'], index: number) {
        let id = this._hash(value);
        let hashmap = this.hashmap;
        if (hashmap[id] === undefined) {
            hashmap[id] = this.dictionary.set(value) - 1;
        }
        this.indices.setValue(hashmap[id], index);
        return value;
    }
    public *fromIterable(source: Iterable<any>, chunkLength = Infinity) {
        const chunks = [] as Data<T>[];
        for (const chunk of super.fromIterable(source, chunkLength)) {
            chunks.push(chunk);
        }
        yield* chunks;
    }
    public async *fromAsyncIterable(source: Iterable<any> | AsyncIterable<any>, chunkLength = Infinity) {
        const chunks = [] as Data<T>[];
        for await (const chunk of super.fromAsyncIterable(source, chunkLength)) {
            chunks.push(chunk);
        }
        yield* chunks;
    }
}

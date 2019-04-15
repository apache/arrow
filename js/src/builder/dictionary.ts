import { Data } from '../data';
import { Vector } from '../vector';
import { IntBuilder } from './int';
import { Dictionary, DataType } from '../type';
import { Builder, DataBuilderOptions } from './base';

type DictionaryHashFunction = (x: any) => string | number;

export interface DictionaryBuilderOptions<T extends DataType = any, TNull = any> extends DataBuilderOptions<T, TNull> {
    dictionaryHashFunction?: DictionaryHashFunction;
}

export class DictionaryBuilder<T extends Dictionary, TNull = any> extends Builder<T, TNull> {

    protected _hash: DictionaryHashFunction;
    protected hashmap = Object.create(null);
    public readonly indices: IntBuilder<T['indices']>;
    public readonly dictionary: Builder<T['dictionary']>;

    constructor(options: DictionaryBuilderOptions<T, TNull>) {
        super(options);
        const { type, nullValues } = options;
        this._hash = options.dictionaryHashFunction || defaultHashFunction;
        this.indices = Builder.new({ type: type.indices, nullValues }) as IntBuilder<T['indices']>;
        this.dictionary = Builder.new({ type: type.dictionary, nullValues: [] }) as Builder<T['dictionary']>;
    }
    public get values() { return this.indices && this.indices.values; }
    public get nullBitmap() { return this.indices && this.indices.nullBitmap; }
    public set values(values: T['TArray']) { this.indices && (this.indices.values = values); }
    public set nullBitmap(nullBitmap: Uint8Array) { this.indices && (this.indices.nullBitmap = nullBitmap); }
    public setHashFunction(hash: DictionaryHashFunction) {
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
    public write(value: any) {
        this.indices.length = super.write(value).length;
        return this;
    }
    public writeValid(isValid: boolean, index: number) {
        return this.indices.writeValid(isValid, index);
    }
    public writeValue(value: T['TValue'], index: number) {
        let id = this._hash(value);
        let hashmap = this.hashmap;
        if (hashmap[id] === undefined) {
            hashmap[id] = this.dictionary.write(value).length - 1;
        }
        return this.indices.writeValue(hashmap[id], index);
    }
    public *readAll(source: Iterable<any>, chunkLength = Infinity) {
        const chunks = [] as Data<T>[];
        for (const chunk of super.readAll(source, chunkLength)) {
            chunks.push(chunk);
        }
        yield* chunks;
    }
    public async *readAllAsync(source: Iterable<any> | AsyncIterable<any>, chunkLength = Infinity) {
        const chunks = [] as Data<T>[];
        for await (const chunk of super.readAllAsync(source, chunkLength)) {
            chunks.push(chunk);
        }
        yield* chunks;
    }
}

function defaultHashFunction(val: any) {
    typeof val === 'string' || (val = `${val}`);
    let h = 6, y = 9 * 9, i = val.length;
    while (i > 0) {
        h = Math.imul(h ^ val.charCodeAt(--i), y);
    }
    return (h ^ h >>> 9) as any;
}

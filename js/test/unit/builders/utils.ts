import { DataBuilder } from '../../Arrow';
import { DataType, Vector, Chunked } from '../../Arrow';

const rand = Math.random.bind(Math);
const randstr = require('randomatic');
const randnulls = <T>(values: T[], n: any = null) => values.map((x) => Math.random() > 0.25 ? x : n);

export const randomBytes = (length: number) => fillRandom(Uint8Array, length);
export const randomString = ((opts) => (length: number) =>
    randstr('?', length, opts)
)({ chars: `abcdefghijklmnopqrstuvwxyz0123456789_` });

export const stringsNoNulls = (length = 20) => Array.from({ length }, (_) => randomString(Math.random() * 20 | 0));
export const timestamp32sNoNulls = (length = 20, now = Date.now() / 86400000 | 0) =>
    Array.from({ length }, (_) => (now + (rand() * 10000 * (rand() > 0.5 ? -1 : 1)) | 0) * 86400000);

export const timestamp64sNoNulls = (length = 20, now = Date.now()) =>
    Array.from({ length }, (_) => now + (rand() * 31557600000 * (rand() > 0.5 ? -1 : 1) | 0));

export const timestamp32sWithNulls = (length = 20) => randnulls(timestamp32sNoNulls(length), null);
export const timestamp64sWithNulls = (length = 20) => randnulls(timestamp64sNoNulls(length), null);

export const boolsNoNulls = (length = 20) => Array.from({ length }, () => rand() > 0.5);
export const date32sNoNulls = (length = 20) => timestamp32sNoNulls(length).map((x) => new Date(x));
export const date64sNoNulls = (length = 20) => timestamp64sNoNulls(length).map((x) => new Date(x));
export const int8sNoNulls = (length = 20) => Array.from(new Int8Array(randomBytes(length * Int8Array.BYTES_PER_ELEMENT).buffer));
export const int16sNoNulls = (length = 20) => Array.from(new Int16Array(randomBytes(length * Int16Array.BYTES_PER_ELEMENT).buffer));
export const int32sNoNulls = (length = 20) => Array.from(new Int32Array(randomBytes(length * Int32Array.BYTES_PER_ELEMENT).buffer));
export const uint8sNoNulls = (length = 20) => Array.from(new Uint8Array(randomBytes(length * Uint8Array.BYTES_PER_ELEMENT).buffer));
export const uint16sNoNulls = (length = 20) => Array.from(new Uint16Array(randomBytes(length * Uint16Array.BYTES_PER_ELEMENT).buffer));
export const uint32sNoNulls = (length = 20) => Array.from(new Uint32Array(randomBytes(length * Uint32Array.BYTES_PER_ELEMENT).buffer));
export const float32sNoNulls = (length = 20) => Array.from(new Float32Array(randomBytes(length * Float32Array.BYTES_PER_ELEMENT).buffer));
export const float64sNoNulls = (length = 20) => Array.from(new Float64Array(randomBytes(length * Float64Array.BYTES_PER_ELEMENT).buffer));

export const date32sWithNulls = (length = 20) => randnulls(date32sNoNulls(length), null);
export const date64sWithNulls = (length = 20) => randnulls(date64sNoNulls(length), null);
export const stringsWithNAs = (length = 20) => randnulls(stringsNoNulls(length), 'n/a');
export const stringsWithNulls = (length = 20) => randnulls(stringsNoNulls(length), null);
export const stringsWithEmpties = (length = 20) => randnulls(stringsNoNulls(length), '\0');

export const int8sWithNulls = (length = 20) => randnulls(int8sNoNulls(length), null);
export const int16sWithNulls = (length = 20) => randnulls(int16sNoNulls(length), null);
export const int32sWithNulls = (length = 20) => randnulls(int32sNoNulls(length), null);
export const uint8sWithNulls = (length = 20) => randnulls(uint8sNoNulls(length), null);
export const uint16sWithNulls = (length = 20) => randnulls(uint16sNoNulls(length), null);
export const uint32sWithNulls = (length = 20) => randnulls(uint32sNoNulls(length), null);
export const float32sWithNulls = (length = 20) => randnulls(float32sNoNulls(length), null);
export const float64sWithNulls = (length = 20) => randnulls(float64sNoNulls(length), null);

export const int8sWithMaxInts = (length = 20) => randnulls(int8sNoNulls(length), 0x7fffffff);
export const int16sWithMaxInts = (length = 20) => randnulls(int16sNoNulls(length), 0x7fffffff);
export const int32sWithMaxInts = (length = 20) => randnulls(int32sNoNulls(length), 0x7fffffff);
export const uint8sWithMaxInts = (length = 20) => randnulls(uint8sNoNulls(length), 0x7fffffff);
export const uint16sWithMaxInts = (length = 20) => randnulls(uint16sNoNulls(length), 0x7fffffff);
export const uint32sWithMaxInts = (length = 20) => randnulls(uint32sNoNulls(length), 0x7fffffff);
export const float64sWithNaNs = (length = 20) => randnulls(float64sNoNulls(length), NaN);
export const float32sWithNaNs = (length = 20) => randnulls(float32sNoNulls(length), NaN);

export const duplicateItems = (n: number, xs: (any | null)[]) => {
    const out = new Array<string | null>(n);
    for (let i = -1, k = xs.length; ++i < n;) {
        out[i] = xs[Math.random() * k | 0];
    }
    return out;
};

export function encodeAll<T extends DataType>(typeFactory: () => T, chunkLen?: number) {
    return function encodeAll<R extends T['TValue']>(values: (R | null)[], nulls?: any[]): Vector<T> {
        const type = typeFactory();
        const builder = DataBuilder.new(type, nulls, chunkLen);
        values.forEach(builder.set.bind(builder));
        return Vector.new(builder.finish().flush()) as Vector<T>;
    }
}

export function encodeEach<T extends DataType>(typeFactory: () => T, chunkLen?: number) {
    return function encodeEach<R extends T['TValue']>(vals: (R | null)[], nulls?: any[]): Vector<T> {
        const type = typeFactory();
        const builder = DataBuilder.new(type, nulls);
        const chunks = [...builder.fromIterable(vals, chunkLen)];
        return Chunked.concat(...chunks.map(Vector.new)) as any;
    }
}

export function validateVector<T extends DataType>(vals: (T['TValue'] | null)[], vec: Vector, nullVals: any[]) {
    let i = 0;
    const nulls = nullVals.reduce((m, x) => m.set(x, true), new Map());
    try {
        for (const x of vec) {
            const y = vals[i];
            expect(x).toEqual(nulls.has(y) ? null : y);
            i++;
        }
    } catch (e) { throw new Error(`${Vector.constructor.name}[${i}]: ${e}`); }
}

function fillRandom<T extends TypedArrayConstructor>(ArrayType: T, length: number) {
    const BPE = ArrayType.BYTES_PER_ELEMENT;
    const array = new ArrayType(length);
    const max = (2 ** (8 * BPE)) - 1;
    for (let i = -1; ++i < length; array[i] = rand() * max * (rand() > 0.5 ? -1 : 1));
    return array as InstanceType<T>;
}

type TypedArrayConstructor =
    (typeof Int8Array) |
    (typeof Int16Array) |
    (typeof Int32Array) |
    (typeof Uint8Array) |
    (typeof Uint16Array) |
    (typeof Uint32Array) |
    (typeof Float32Array) |
    (typeof Float64Array);

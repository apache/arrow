import {
    Vector, DataType,
    Bool, Int8, Int16, Int32, Int64, Uint8, Uint16, Uint32, Uint64, Float16, Float32, Float64
} from '../../Arrow';
import { util } from '../../Arrow';

import {
    validateVector, encodeAll, encodeEach,
    boolsNoNulls, boolsWithNulls,
    int8sNoNulls, int8sWithNulls, int8sWithMaxInts,
    int16sNoNulls, int16sWithNulls, int16sWithMaxInts,
    int32sNoNulls, int32sWithNulls, int32sWithMaxInts,
    int64sNoNulls, int64sWithNulls, int64sWithMaxInts,
    uint8sNoNulls, uint8sWithNulls, uint8sWithMaxInts,
    uint16sNoNulls, uint16sWithNulls, uint16sWithMaxInts,
    uint32sNoNulls, uint32sWithNulls, uint32sWithMaxInts,
    uint64sNoNulls, uint64sWithNulls, uint64sWithMaxInts,
    float16sNoNulls, float16sWithNulls, float16sWithNaNs,
    float32sNoNulls, float32sWithNulls, float64sWithNaNs,
    float64sNoNulls, float64sWithNulls, float32sWithNaNs,
} from './utils';

describe('BoolBuilder', () => {

    runTestsWithEncoder('encodeAll: 5', encodeAll(() => new Bool()));
    runTestsWithEncoder('encodeEach: 5', encodeEach(() => new Bool(), 5));
    runTestsWithEncoder('encodeEach: 25', encodeEach(() => new Bool(), 25));
    runTestsWithEncoder('encodeEach: undefined', encodeEach(() => new Bool()));

    function runTestsWithEncoder<T extends DataType>(name: string, encode: (vals: (T['TValue'] | null)[], nullVals?: any[]) => Vector<T>) {
        describe(`${encode.name} ${name}`, () => {
            it(`encodes bools no nulls`, () => {
                const vals = boolsNoNulls(20);
                validateVector(vals, encode(vals, []), []);
            });
            it(`encodes bools with nulls`, () => {
                const vals = boolsWithNulls(20);
                validateVector(vals, encode(vals, [null]), [null]);
            });
        });
    }
});

type PrimitiveTypeOpts<T extends DataType> = [
    new (...args: any[]) => T,
    (count: number) => (T['TValue'] | null)[],
    (count: number) => (T['TValue'] | null)[],
    (count: number) => (T['TValue'] | null)[]
];

[
    [Int8, int8sNoNulls, int8sWithNulls, int8sWithMaxInts] as PrimitiveTypeOpts<Int8>,
    [Int16, int16sNoNulls, int16sWithNulls, int16sWithMaxInts] as PrimitiveTypeOpts<Int16>,
    [Int32, int32sNoNulls, int32sWithNulls, int32sWithMaxInts] as PrimitiveTypeOpts<Int32>,
    [Int64, int64sNoNulls, int64sWithNulls, int64sWithMaxInts] as PrimitiveTypeOpts<Int64>,
    [Uint8, uint8sNoNulls, uint8sWithNulls, uint8sWithMaxInts] as PrimitiveTypeOpts<Uint8>,
    [Uint16, uint16sNoNulls, uint16sWithNulls, uint16sWithMaxInts] as PrimitiveTypeOpts<Uint16>,
    [Uint32, uint32sNoNulls, uint32sWithNulls, uint32sWithMaxInts] as PrimitiveTypeOpts<Uint32>,
    [Uint64, uint64sNoNulls, uint64sWithNulls, uint64sWithMaxInts] as PrimitiveTypeOpts<Uint64>,
].forEach(([TypeCtor, noNulls, withNulls, withNaNs]) => {

    describe(`${TypeCtor.name}Builder`, () => {

        const typeFactory = () => new TypeCtor();
        const valueName = TypeCtor.name.toLowerCase();

        runTestsWithEncoder('encodeAll', encodeAll(typeFactory));
        runTestsWithEncoder('encodeEach: 5', encodeEach(typeFactory, 5));
        runTestsWithEncoder('encodeEach: 25', encodeEach(typeFactory, 25));
        runTestsWithEncoder('encodeEach: undefined', encodeEach(typeFactory));
    
        function runTestsWithEncoder<T extends DataType>(name: string, encode: (vals: (T['TValue'] | null)[], nullVals?: any[]) => Vector<T>) {
            describe(`${encode.name} ${name}`, () => {
                it(`encodes ${valueName} no nulls`, () => {
                    const vals = noNulls(20);
                    validateVector(vals, encode(vals, []), []);
                });
                it(`encodes ${valueName} with nulls`, () => {
                    const vals = withNulls(20);
                    validateVector(vals, encode(vals, [null]), [null]);
                });
                it(`encodes ${valueName} with MAX_INT`, () => {
                    const vals = withNaNs(20);
                    const nullVals0: any[] = [0x7fffffff];
                    const nullVals1: any[] = [0x7fffffff];
                    switch (TypeCtor) {
                        case Int64:
                        case Uint64:
                            nullVals0[0] = new Uint32Array([0x7fffffff, 0x7fffffff]);
                            nullVals1[0] = (util.BN.new(nullVals0[0]) as any)[Symbol.toPrimitive]('default');
                    }
                    validateVector(vals, encode(vals, nullVals0), nullVals1);
                });
            });
        }
    });
});

[
    [Float16, float16sNoNulls, float16sWithNulls, float16sWithNaNs] as PrimitiveTypeOpts<Float16>,
    [Float32, float32sNoNulls, float32sWithNulls, float32sWithNaNs] as PrimitiveTypeOpts<Float32>,
    [Float64, float64sNoNulls, float64sWithNulls, float64sWithNaNs] as PrimitiveTypeOpts<Float64>,
].forEach(([TypeCtor, noNulls, withNulls, withNaNs]) => {

    describe(`${TypeCtor.name}Builder`, () => {

        const typeFactory = () => new TypeCtor();
        const valueName = TypeCtor.name.toLowerCase();

        runTestsWithEncoder('encodeAll', encodeAll(typeFactory));
        runTestsWithEncoder('encodeEach: 5', encodeEach(typeFactory, 5));
        runTestsWithEncoder('encodeEach: 25', encodeEach(typeFactory, 25));
        runTestsWithEncoder('encodeEach: undefined', encodeEach(typeFactory));

        function runTestsWithEncoder<T extends DataType>(name: string, encode: (vals: (T['TValue'] | null)[], nullVals?: any[]) => Vector<T>) {
            describe(`${encode.name} ${name}`, () => {
                it(`encodes ${valueName} no nulls`, () => {
                    const vals = noNulls(20);
                    validateVector(vals, encode(vals, []), []);
                });
                it(`encodes ${valueName} with nulls`, () => {
                    const vals = withNulls(20);
                    validateVector(vals, encode(vals, [null]), [null]);
                });
                it(`encodes ${valueName} with NaNs`, () => {
                    const vals = withNaNs(20);
                    validateVector(vals, encode(vals, [NaN]), [NaN]);
                });
            });
        }
    });
});

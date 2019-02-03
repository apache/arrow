import { validateVector } from './utils';
import { Vector, DateDay, DateMillisecond } from '../../Arrow';
import {
    encodeAll,
    encodeEach,
    date32sNoNulls,
    date64sNoNulls,
    date32sWithNulls,
    date64sWithNulls
} from './utils';

describe('DateDayBuilder', () => {
    runTestsWithEncoder('5', encodeAll(() => new DateDay(), 5));
    runTestsWithEncoder('25', encodeAll(() => new DateDay(), 25));
    runTestsWithEncoder('none', encodeAll(() => new DateDay()));
    runTestsWithEncoder('5', encodeEach(() => new DateDay(), 5));
    runTestsWithEncoder('25', encodeEach(() => new DateDay(), 25));
    runTestsWithEncoder('none', encodeEach(() => new DateDay()));
    
    function runTestsWithEncoder(name: string, encode: (vals: (Date | null)[], nullVals?: any[]) => Vector<DateDay>) {
        describe(`${encode.name} ${name}`, () => {
            it(`encodes dates no nulls`, () => {
                const vals = date32sNoNulls(20);
                validateVector(vals, encode(vals, []), []);
            });
            it(`encodes dates with nulls`, () => {
                const vals = date32sWithNulls(20);
                validateVector(vals, encode(vals, [null]), [null]);
            });
        });
    }
});

describe('DateMillisecondBuilder', () => {
    runTestsWithEncoder('5', encodeAll(() => new DateMillisecond(), 5));
    runTestsWithEncoder('25', encodeAll(() => new DateMillisecond(), 25));
    runTestsWithEncoder('none', encodeAll(() => new DateMillisecond()));
    runTestsWithEncoder('5', encodeEach(() => new DateMillisecond(), 5));
    runTestsWithEncoder('25', encodeEach(() => new DateMillisecond(), 25));
    runTestsWithEncoder('none', encodeEach(() => new DateMillisecond()));
    
    function runTestsWithEncoder(name: string, encode: (vals: (Date | null)[], nullVals?: any[]) => Vector<DateMillisecond>) {
        describe(`${encode.name} ${name}`, () => {
            it(`encodes dates no nulls`, () => {
                const vals = date64sNoNulls(20);
                validateVector(vals, encode(vals, []), []);
            });
            it(`encodes dates with nulls`, () => {
                const vals = date64sWithNulls(20);
                validateVector(vals, encode(vals, [null]), [null]);
            });
        });
    }
});

import { validateVector } from './utils';
import { Vector, DateMillisecond } from '../../Arrow';
import {
    encodeAll,
    encodeEach,
    datesNoNulls,
    datesWithNulls
} from './utils';

describe('Date64Builder', () => {
    runTestsWithEncoder(encodeAll(() => new DateMillisecond(), 5));
    runTestsWithEncoder(encodeAll(() => new DateMillisecond(), 25));
    runTestsWithEncoder(encodeAll(() => new DateMillisecond(), void 0));
    runTestsWithEncoder(encodeEach(() => new DateMillisecond(), 5));
    runTestsWithEncoder(encodeEach(() => new DateMillisecond(), 25));
    runTestsWithEncoder(encodeEach(() => new DateMillisecond(), void 0));
});

function runTestsWithEncoder(encode: (vals: (Date | null)[], nullVals?: any[]) => Vector<DateMillisecond>) {
    describe(encode.name, () => {
        it(`encodes dates no nulls`, () => {
            const vals = datesNoNulls(20);
            validateVector(vals, encode(vals, []), []);
        });
        it(`encodes dates with nulls`, () => {
            const vals = datesWithNulls(20);
            validateVector(vals, encode(vals, [null]), [null]);
        });
    });
}

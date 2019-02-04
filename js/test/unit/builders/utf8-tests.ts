import { validateVector } from './utils';
import { Vector, Utf8 } from '../../Arrow';
import {
    encodeAll,
    encodeEach,
    stringsNoNulls,
    stringsWithNAs,
    stringsWithNulls,
    stringsWithEmpties
} from './utils';

describe('Utf8Builder', () => {
    runTestsWithEncoder('chunkLength: 5', encodeAll(() => new Utf8(), 5));
    runTestsWithEncoder('chunkLength: 25', encodeAll(() => new Utf8(), 25));
    runTestsWithEncoder('chunkLength: undefined', encodeAll(() => new Utf8(), void 0));
    runTestsWithEncoder('chunkLength: 5', encodeEach(() => new Utf8(), 5));
    runTestsWithEncoder('chunkLength: 25', encodeEach(() => new Utf8(), 25));
    runTestsWithEncoder('chunkLength: undefined', encodeEach(() => new Utf8(), void 0));
});

function runTestsWithEncoder(name: string, encode: (vals: (string | null)[], nullVals?: any[]) => Vector<Utf8>) {
    describe(`${encode.name} ${name}`, () => {
        it(`encodes strings no nulls`, () => {
            const vals = stringsNoNulls(20);
            validateVector(vals, encode(vals, []), []);
        });
        it(`encodes strings with nulls`, () => {
            const vals = stringsWithNulls(20);
            validateVector(vals, encode(vals, [null]), [null]);
        });
        it(`encodes strings using n/a as the null value rep`, () => {
            const vals = stringsWithNAs(20);
            validateVector(vals, encode(vals, ['n/a']), ['n/a']);
        });
        it(`encodes strings using \\0 as the null value rep`, () => {
            const vals = stringsWithEmpties(20);
            validateVector(vals, encode(vals, ['\0']), ['\0']);
        });
    });
}

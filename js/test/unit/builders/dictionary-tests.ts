import { validateVector } from './utils';
import { Dictionary, Utf8, Int32, Vector } from '../../Arrow';
import {
    encodeAll,
    encodeEach,
    duplicateItems,
    stringsNoNulls,
    stringsWithNAs,
    stringsWithNulls,
    stringsWithEmpties
} from './utils';

describe('DictionaryBuilder', () => {
    describe('<Utf8, Int32>', () => {
        runTestsWithEncoder('chunkLength: 5', encodeAll(() => new Dictionary(new Utf8(), new Int32()), 5));
        runTestsWithEncoder('chunkLength: 25', encodeAll(() => new Dictionary(new Utf8(), new Int32()), 25));
        runTestsWithEncoder('chunkLength: undefined', encodeAll(() => new Dictionary(new Utf8(), new Int32()), void 0));
        runTestsWithEncoder('chunkLength: 5', encodeEach(() => new Dictionary(new Utf8(), new Int32()), 5));
        runTestsWithEncoder('chunkLength: 25', encodeEach(() => new Dictionary(new Utf8(), new Int32()), 25));
        runTestsWithEncoder('chunkLength: undefined', encodeEach(() => new Dictionary(new Utf8(), new Int32()), void 0));
    });
});

function runTestsWithEncoder(name: string, encode: (vals: (string | null)[], nullVals?: any[]) => Vector<Dictionary<Utf8, Int32>>) {
    describe(`${encode.name} ${name}`, () => {
        it(`dictionary-encodes strings no nulls`, () => {
            const vals = duplicateItems(20, stringsNoNulls(10));
            validateVector(vals, encode(vals, []), []);
        });
        it(`dictionary-encodes strings with nulls`, () => {
            const vals = duplicateItems(20, stringsWithNulls(10));
            validateVector(vals, encode(vals, [null]), [null]);
        });
        it(`dictionary-encodes strings using n/a as the null value rep`, () => {
            const vals = duplicateItems(20, stringsWithNAs(10));
            validateVector(vals, encode(vals, ['n/a']), ['n/a']);
        });
        it(`dictionary-encodes strings using \\0 as the null value rep`, () => {
            const vals = duplicateItems(20, stringsWithEmpties(10));
            validateVector(vals, encode(vals, ['\0']), ['\0']);
        });
    });
}

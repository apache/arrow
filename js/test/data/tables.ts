// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import { vecs } from '../generate-test-data';
import * as generate from '../generate-test-data';
import { Schema, Field, Dictionary } from '../Arrow';

const listVectorGeneratorNames = ['list', 'fixedSizeList'];
const nestedVectorGeneratorNames = [ 'struct', 'denseUnion', 'sparseUnion', 'map' ];
const dictionaryKeyGeneratorNames = ['int8' ,'int16' ,'int32' ,'uint8' ,'uint16' ,'uint32'];
const valueVectorGeneratorNames = [
    'null_', 'bool', 'int8', 'int16', 'int32', 'int64', 'uint8', 'uint16', 'uint32', 'uint64',
    'float16', 'float32', 'float64', 'utf8', 'binary', 'fixedSizeBinary', 'dateDay', 'dateMillisecond',
    'timestampSecond', 'timestampMillisecond', 'timestampMicrosecond', 'timestampNanosecond',
    'timeSecond', 'timeMillisecond', 'timeMicrosecond', 'timeNanosecond', 'decimal',
    'dictionary', 'intervalDayTime', 'intervalYearMonth'
];

const vectorGeneratorNames = [...valueVectorGeneratorNames, ...listVectorGeneratorNames, ...nestedVectorGeneratorNames];

export function* generateRandomTables(batchLengths = [1000, 2000, 3000], minCols = 1, maxCols = 5) {

    let numCols = 0;
    let allNames = shuffle(vectorGeneratorNames);

    do {
        numCols = Math.max(Math.min(
            Math.random() * maxCols | 0, allNames.length), minCols);

        let names = allNames.slice(0, numCols);
        let types = names.map((fn) => vecs[fn](0).vector.type);
        let schema = new Schema(names.map((name, i) => new Field(name, types[i])));

        yield generate.table(batchLengths, schema).table;

    } while ((allNames = allNames.slice(numCols)).length > 0);
}

/**
 * Yields a series of tables containing a single Dictionary-encoded column.
 * Each yielded table will be a unique combination of dictionary and indexType,
 * such that consuming all tables ensures all Arrow types dictionary-encode.
 *
 * @param batchLengths number[] Number and length of recordbatches to generate
 */
export function* generateDictionaryTables(batchLengths = [100, 200, 300]) {
    for (const dictName of valueVectorGeneratorNames) {
        if (dictName === 'dictionary') { continue; }
        const dictionary = vecs[dictName](100).vector;
        for (const keys of dictionaryKeyGeneratorNames) {
            const valsType = dictionary.type;
            const keysType = vecs[keys](0).vector.type;
            const dictType = new Dictionary(valsType, keysType);
            const schema = new Schema([new Field(`dict[${keys}]`, dictType, true)]);
            yield generate.table(batchLengths, schema).table;
        }
    }
}

function shuffle(input: any[]) {
    const result = input.slice();
    let j, tmp, i = result.length;
    while (--i > 0) {
        j = (Math.random() * (i + 1)) | 0;
        tmp = result[i];
        result[i] = result[j];
        result[j] = tmp;
    }
    return result;
}

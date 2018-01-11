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

import { Vector } from './vector/vector';
import { read, readAsync } from './reader/arrow';
import { Predicate } from './predicate';

export type NextFunc = (idx: number, cols: Vector[]) => void;

export class TableRow {
    constructor (readonly batch: Vector[], readonly idx: number) {}
    toArray() {
        return this.batch.map((vec) => vec.get(this.idx));
    }
    toString() {
        return this.toArray().map((x) => JSON.stringify(x)).join(', ');
    }
    *[Symbol.iterator]() {
        for (const vec of this.batch) {
            yield vec.get(this.idx);
        }
    }
}

export interface DataFrame {
    readonly batches: Vector[][];
    readonly lengths: Uint32Array;
    filter(predicate: Predicate): DataFrame;
    scan(next: NextFunc): void;
    count(): number;
}

function columnsFromBatches(batches: Vector[][]) {
    const remaining = batches.slice(1);
    return batches[0].map((vec, colidx) =>
        vec.concat(...remaining.map((batch) => batch[colidx]))
    );
}

export class Table implements DataFrame {
    static from(sources?: Iterable<Uint8Array | Buffer | string> | object | string) {
        let batches: Vector<any>[][] = [[]];
        if (sources) {
            batches = Array.from(read(sources));
        }
        return new Table({ batches });
    }
    static async fromAsync(sources?: AsyncIterable<Uint8Array | Buffer | string>) {
        let batches: Vector<any>[][] = [[]];
        if (sources) {
            batches = [];
            for await (let batch of readAsync(sources)) {
                batches.push(batch);
            }
        }
        return new Table({ batches });
    }

    // VirtualVector of each column, spanning batches
    readonly columns: Vector<any>[];

    // List of batches, where each batch is a list of Vectors
    readonly batches: Vector<any>[][];
    readonly lengths: Uint32Array;
    readonly length: number;
    constructor(argv: { batches: Vector<any>[][] }) {
        this.batches = argv.batches;
        this.columns = columnsFromBatches(this.batches);
        this.lengths = new Uint32Array(this.batches.map((batch) => batch[0].length));

        this.length = this.lengths.reduce((acc, length) => acc + length);
    }
    get(idx: number): TableRow {
        let batch = 0;
        while (idx > this.lengths[batch] && batch < this.lengths.length) {
            idx -= this.lengths[batch++];
        }

        if (batch === this.lengths.length) { throw new Error('Overflow'); }

        return new TableRow(this.batches[batch], idx);
    }
    filter(predicate: Predicate): DataFrame {
        return new FilteredDataFrame(this, predicate);
    }
    scan(next: NextFunc) {
        for (let batch = -1; ++batch < this.lengths.length;) {
            const length = this.lengths[batch];

            // load batches
            const columns = this.batches[batch];

            // yield all indices
            for (let idx = -1; ++idx < length;) {
                next(idx, columns);
            }
        }
    }
    count(): number {
        return this.lengths.reduce((acc, val) => acc + val);
    }
    *[Symbol.iterator]() {
        for (let batch = -1; ++batch < this.lengths.length;) {
            const length = this.lengths[batch];

            // load batches
            const columns = this.batches[batch];

            // yield all indices
            for (let idx = -1; ++idx < length;) {
                yield new TableRow(columns, idx);
            }
        }
    }
}

class FilteredDataFrame implements DataFrame {
    readonly lengths: Uint32Array;
    readonly batches: Vector[][];
    constructor (readonly parent: DataFrame, private predicate: Predicate) {
        this.batches = parent.batches;
        this.lengths = parent.lengths;
    }

    scan(next: NextFunc) {
        // inlined version of this:
        // this.parent.scan((idx, columns) => {
        //     if (this.predicate(idx, columns)) next(idx, columns);
        // });
        for (let batch = -1; ++batch < this.lengths.length;) {
            const length = this.lengths[batch];

            // load batches
            const columns = this.batches[batch];
            const predicate = this.predicate.bind(columns);

            // yield all indices
            for (let idx = -1; ++idx < length;) {
                if (predicate(idx, columns)) { next(idx, columns); }
            }
        }
    }

    count(): number {
        // inlined version of this:
        // let sum = 0;
        // this.parent.scan((idx, columns) => {
        //     if (this.predicate(idx, columns)) ++sum;
        // });
        // return sum;
        let sum = 0;
        for (let batch = -1; ++batch < this.lengths.length;) {
            const length = this.lengths[batch];

            // load batches
            const columns = this.batches[batch];
            const predicate = this.predicate.bind(columns);

            // yield all indices
            for (let idx = -1; ++idx < length;) {
                if (predicate(idx, columns)) { ++sum; }
            }
        }
        return sum;
    }

    filter(predicate: Predicate): DataFrame {
        return new FilteredDataFrame(
            this.parent,
            this.predicate.and(predicate)
        );
    }
}

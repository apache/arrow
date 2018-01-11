import { Vector } from "../vector/vector";
import { StructVector, StructRow } from "../vector/struct";
import { VirtualVector } from "../vector/virtual";

import { Predicate } from "./predicate"

export type NextFunc = (idx: number, cols: Vector[]) => void;

export class DataFrameRow extends StructRow<any> {
    constructor (batches: Vector[], idx: number) {
        super(new StructVector({columns: batches}), idx);
    }
}

export interface DataFrameOps {
    readonly batches: Vector[][];
    readonly lengths: Uint32Array;
    filter(predicate: Predicate): DataFrameOps;
    scan(next: NextFunc): void;
    count(): number;
}

export class DataFrame extends Vector<DataFrameRow> implements DataFrameOps {
    readonly lengths: Uint32Array;
    constructor(readonly batches: Vector[][]) {
        super();
        // for each batch
        this.lengths = new Uint32Array(batches.map((batch)=>{
            // verify that every vector has the same length, and return that
            // length
            // throw an error if the lengths don't match
            return batch.reduce((length, col) => {
                if (col.length !== length)
                    throw new Error("Attempted to create a DataFrame with un-aligned vectors");
                return length;
            }, batch[0].length);
        }));
    }

    get(idx: number): DataFrameRow|null {
        let batch = 0;
        while (idx > this.lengths[batch] && batch < this.lengths.length)
            idx -= this.lengths[batch++];

        if (batch === this.lengths.length) return null;

        else return new DataFrameRow(this.batches[batch], idx);
    }

    filter(predicate: Predicate): DataFrameOps {
        return new FilteredDataFrame(this, predicate);
    }

    scan(next: NextFunc) {
        for (let batch = -1; ++batch < this.lengths.length;) {
            const length = this.lengths[batch];

            // load batches
            const columns = this.batches[batch];

            // yield all indices
            for (let idx = -1; ++idx < length;) {
                next(idx, columns)
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
                yield new DataFrameRow(columns, idx);
            }
        }
    }

    static from(table: Vector<any>): DataFrame {
        if (table instanceof StructVector) {
            const columns = table.columns;
            if (isAligned(columns)) {
                // StructVector of aligned VirtualVectors
                // break up VirtualVectors into batches
                const batches = columns[0].vectors.map((_,i) => {
                    return columns.map((vec: VirtualVector<any>) => {
                            return vec.vectors[i];
                        });
                });
                return new DataFrame(batches);
            } else {
                return new DataFrame([columns]);
            }
        } else if (table instanceof VirtualVector &&
                   table.vectors.every((vec) => vec instanceof StructVector)) {
            return new DataFrame(table.vectors.map((vec) => {
                return (vec as StructVector<any>).columns;
            }));
        } else {
            return new DataFrame([[table]]);
        }
    }
}

class FilteredDataFrame implements DataFrameOps {
    readonly lengths: Uint32Array;
    readonly batches: Vector[][];
    constructor (readonly parent: DataFrameOps, private predicate: Predicate) {
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
                if (predicate(idx, columns)) next(idx, columns);
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
                if (predicate(idx, columns)) ++sum;
            }
        }
        return sum;
    }

    filter(predicate: Predicate): DataFrameOps {
        return new FilteredDataFrame(
            this.parent,
            this.predicate.and(predicate)
        );
    }
}

function isAligned(columns: Vector[]): columns is VirtualVector<any>[] {
    if (columns.every((col) => col instanceof VirtualVector)) {
        const virtuals = columns as VirtualVector<any>[]

        return virtuals.slice(1).every((col) => {
            return col.aligned(virtuals[0]);
        });
    }
    return false;
}

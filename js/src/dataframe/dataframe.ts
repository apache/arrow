import { Vector } from "../vector/vector";
import { StructVector } from "../vector/struct";
import { VirtualVector } from "../vector/virtual";

export type NextFunc = (idx: number, cols: Vector[]) => void;
export type PredicateFunc = (idx: number, cols: Vector[]) => boolean;

export abstract class DataFrame {
    constructor(readonly lengths: Uint32Array) {}
    public abstract columns: Vector<any>[];
    public abstract getBatch(batch: number): Vector[];
    public abstract scan(next: NextFunc): void;
    public filter(predicate: PredicateFunc): DataFrame {
        return new FilteredDataFrame(this, predicate);
    }

    static from(table: Vector<any>): DataFrame {
        // There are two types of Vectors we might want to make into
        // a ChunkedDataFrame:
        //   1) a StructVector of all VirtualVectors
        //   2) a VirtualVector of all StructVectors
        if (table instanceof StructVector) {
            if (table.columns.every((col) => col instanceof VirtualVector)) {
                // ChunkedDataFrame case (1)
                return new ChunkedDataFrame(table.columns as VirtualVector<any>[]);
            } else {
                return new SimpleDataFrame(table.columns)
            }
        } else if (table instanceof VirtualVector &&
                   table.vectors.every((vec) => vec instanceof StructVector)) {
            const structs = table.vectors as StructVector<any>[];
            const rest: StructVector<any>[] = structs.slice(1);
            const virtuals: VirtualVector<any>[] = structs[0].columns.map((vec, col_idx) => {
                return vec.concat(...rest.map((vec) => vec.columns[col_idx]));
            }) as VirtualVector<any>[];
            // ChunkedDataFrame case (2)
            return new ChunkedDataFrame(virtuals);
        } else {
            return new SimpleDataFrame([table]);
        }
    }

    count(): number {
        return this.lengths.reduce((acc, val) => acc + val);
    }
}

class SimpleDataFrame extends DataFrame {
    readonly lengths: Uint32Array;
    constructor(public columns: Vector<any>[]) {
        super(new Uint32Array([0, columns[0].length]));
        if (!this.columns.slice(1).every((v) => v.length === this.columns[0].length)) {
            throw new Error("Attempted to create a DataFrame with un-aligned vectors");
        }
    }

    public getBatch() {
        return this.columns;
    }

    public scan(next: NextFunc) {
        for (let idx = -1; ++idx < this.lengths[1];) {
            next(idx, this.columns)
        }
    }

    *[Symbol.iterator]() {
        for (let idx = -1; ++idx < this.lengths[1];) {
            yield idx;
        }
    }
}

class ChunkedDataFrame extends DataFrame {
    public columns: Vector<any>[];
    constructor(private virtuals: VirtualVector<any>[]) {
        super(ChunkedDataFrame.getLengths(virtuals));
        this.virtuals = virtuals;
    }

    getBatch(batch: number): Vector[] {
        return this.virtuals.map((virt) => virt.vectors[batch]);
    }

    scan(next: NextFunc) {
        for (let batch = -1; ++batch < this.lengths.length;) {
            const length = this.lengths[batch];

            // load batches
            const columns = this.getBatch(batch);

            // yield all indices
            for (let idx = -1; ++idx < length;) {
                next(idx, columns)
            }
        }
    }

    *[Symbol.iterator]() {
        for (let batch = -1; ++batch < this.lengths.length;) {
            const length = this.lengths[batch];

            // load batches
            this.columns = this.getBatch(batch);

            // yield all indices
            for (let idx = -1; ++idx < length;) {
                yield idx;
            }
        }
    }

    private static getLengths(virtuals: VirtualVector<any>[]): Uint32Array {
        if (!virtuals.slice(1).every((v) => v.aligned(virtuals[0]))) {
            throw new Error("Attempted to create a DataFrame with un-aligned vectors");
        }
        return new Uint32Array(virtuals[0].vectors.map((v)=>v.length));
    }
}

class FilteredDataFrame extends DataFrame {
    public columns: Vector<any>[];
    constructor (readonly parent: DataFrame, private predicate: PredicateFunc) {
        super(parent.lengths);
    }

    getBatch(batch: number): Vector[] {
        return this.parent.getBatch(batch);
    };

    scan(next: NextFunc) {
        // inlined version of this:
        // this.parent.scan((idx, columns) => {
        //     if (this.predicate(idx, columns)) next(idx, columns);
        // });
        for (let batch = -1; ++batch < this.parent.lengths.length;) {
            const length = this.parent.lengths[batch];

            // load batches
            const columns = this.parent.getBatch(batch);

            // yield all indices
            for (let idx = -1; ++idx < length;) {
                if (this.predicate(idx, columns)) next(idx, columns);
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
        for (let batch = -1; ++batch < this.parent.lengths.length;) {
            const length = this.parent.lengths[batch];

            // load batches
            const columns = this.parent.getBatch(batch);

            // yield all indices
            for (let idx = -1; ++idx < length;) {
                if (this.predicate(idx, columns)) ++sum;
            }
        }
        return sum;
    }

    filter(predicate: PredicateFunc): DataFrame {
        return new FilteredDataFrame(
            this.parent,
            (idx, cols) => this.predicate(idx, cols) && predicate(idx, cols)
        );
    }
}

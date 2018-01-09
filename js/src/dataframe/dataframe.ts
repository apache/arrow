import { Vector } from "../vector/vector";
import { StructVector } from "../vector/struct";
import { VirtualVector } from "../vector/virtual";

export abstract class DataFrame {
    public abstract columns: Vector<any>[];
    public abstract getBatch(batch: number): Vector[];
    public abstract scan(next: (idx: number, cols: Vector[])=>void): void;
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
}

class SimpleDataFrame extends DataFrame {
    readonly lengths: Uint32Array;
    constructor(public columns: Vector<any>[]) {
        super();
        if (!this.columns.slice(1).every((v) => v.length === this.columns[0].length)) {
            throw new Error("Attempted to create a DataFrame with un-aligned vectors");
        }
        this.lengths = new Uint32Array([0, this.columns[0].length]);
    }

    public getBatch() {
        return this.columns;
    }

    public scan(next: (idx: number, cols: Vector[])=>void) {
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
    readonly lengths: Uint32Array;
    constructor(private virtuals: VirtualVector<any>[]) {
        super();
        const offsets = virtuals[0].offsets;
        if (!this.virtuals.slice(1).every((v) => v.aligned(virtuals[0]))) {
            throw new Error("Attempted to create a DataFrame with un-aligned vectors");
        }
        this.lengths = new Uint32Array(offsets.length);
        offsets.forEach((offset, i) => {
            this.lengths[i] = offsets[i+1] - offset;;
        });
    }

    getBatch(batch: number): Vector[] {
        return this.virtuals.map((virt) => virt.vectors[batch]);
    }

    scan(next: (idx: number, cols: Vector[])=>void) {
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
}

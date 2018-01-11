import { Vector } from "./vector/vector";
import { DictionaryVector } from "./vector/dictionary";

export type ValueFunc<T> = (idx: number, cols: Vector[]) => T|null;
export type PredicateFunc = (idx: number, cols: Vector[]) => boolean;

export abstract class Value<T> {
    eq(other: Value<T>|T): Predicate {
        if (!(other instanceof Value)) other = new Literal(other);
        return new Equals(this, other);
    }
    lteq(other: Value<T>|T): Predicate {
        if (!(other instanceof Value)) other = new Literal(other);
        return new LTeq(this, other);
    }
    gteq(other: Value<T>|T): Predicate {
        if (!(other instanceof Value)) other = new Literal(other);
        return new GTeq(this, other);
    }
}

class Literal<T=any> extends Value<T> {
    constructor(public v: T) { super(); }
}

class Col<T=any> extends Value<T> {
    vector: Vector<T>;
    colidx: number;

    constructor(public name: string) { super(); }
    bind(cols: Vector[]) {
        if (!this.colidx) {
            // Assume column index doesn't change between calls to bind
            //this.colidx = cols.findIndex(v => v.name.indexOf(this.name) != -1);
            this.colidx = -1;
            for (let idx = -1; ++idx < cols.length;) {
                if (cols[idx].name === this.name) {
                    this.colidx = idx;
                    break;
                }
            }
            if (this.colidx < 0) throw new Error(`Failed to bind Col "${this.name}"`)
        }
        this.vector = cols[this.colidx]
        return this.vector.get.bind(this.vector);
    }

    emitString() { return `cols[${this.colidx}].get(idx)`; }
}

export abstract class Predicate {
    abstract bind(cols: Vector[]): PredicateFunc;
    and(expr: Predicate): Predicate { return new And(this, expr); }
    or(expr: Predicate): Predicate { return new Or(this, expr); }
    ands(): Predicate[] { return [this]; }
}

abstract class ComparisonPredicate<T=any> extends Predicate {
    constructor(public readonly left: Value<T>, public readonly right: Value<T>) {
        super();
    }

    bind(cols: Vector<any>[]) {
        if (this.left instanceof Literal) {
            if (this.right instanceof Literal) {
                return this._bindLitLit(cols, this.left, this.right);
            } else { // right is a Col

                return this._bindColLit(cols, this.right as Col, this.left);
            }
        } else { // left is a Col
            if (this.right instanceof Literal) {
                return this._bindColLit(cols, this.left as Col, this.right);
            } else { // right is a Col
                return this._bindColCol(cols, this.left as Col, this.right as Col);
            }
        }
    }

    protected abstract _bindLitLit(cols: Vector<any>[], left: Literal, right: Literal): PredicateFunc;
    protected abstract _bindColCol(cols: Vector<any>[], left: Col    , right: Col    ): PredicateFunc;
    protected abstract _bindColLit(cols: Vector<any>[], col: Col     , lit: Literal  ): PredicateFunc;
}

abstract class CombinationPredicate extends Predicate {
    constructor(public readonly left: Predicate, public readonly right: Predicate) {
        super();
    }
}

class And extends CombinationPredicate {
    bind(cols: Vector[]) {
        const left = this.left.bind(cols);
        const right = this.right.bind(cols);
        return (idx: number, cols: Vector[]) => left(idx, cols) && right(idx, cols);
    }
    ands() : Predicate[] { return this.left.ands().concat(this.right.ands()); }
}

class Or extends CombinationPredicate {
    bind(cols: Vector[]) {
        const left = this.left.bind(cols);
        const right = this.right.bind(cols);
        return (idx: number, cols: Vector[]) => left(idx, cols) || right(idx, cols);
    }
}

class Equals extends ComparisonPredicate {
    protected _bindLitLit(_: Vector<any>[], left: Literal, right: Literal): PredicateFunc {
        const rtrn: boolean = left.v == right.v;
        return () => rtrn;
    }

    protected _bindColCol(cols: Vector<any>[], left: Col    , right: Col    ): PredicateFunc {
        const left_func = left.bind(cols);
        const right_func = right.bind(cols);
        return (idx: number, cols: Vector[]) => left_func(idx, cols) == right_func(idx, cols);
    }

    protected _bindColLit(cols: Vector<any>[], col: Col     , lit: Literal  ): PredicateFunc {
        const col_func = col.bind(cols);
        if (col.vector instanceof DictionaryVector) {
            // Assume that there is only one key with the value `lit.v`
            let key = -1
            for (; ++key < col.vector.data.length;) {
                if (col.vector.data.get(key) === lit.v) {
                    break;
                }
            }

            if (key == col.vector.data.length) {
                // the value doesn't exist in the dictionary - always return
                // false
                // TODO: special-case of PredicateFunc that encapsulates this
                // "always false" behavior. That way filtering operations don't
                // have to bother checking
                return () => false;
            } else {
                return (idx: number) => {
                    return (col.vector as DictionaryVector<any>).getKey(idx) === key;
                }
            }
        } else {
            return (idx: number, cols: Vector[]) => col_func(idx, cols) == lit.v;
        }
    }
}

class LTeq extends ComparisonPredicate {
    protected _bindLitLit(_: Vector<any>[], left: Literal, right: Literal): PredicateFunc {
        const rtrn: boolean = left.v <= right.v;
        return () => rtrn;
    }

    protected _bindColCol(cols: Vector<any>[], left: Col    , right: Col    ): PredicateFunc {
        const left_func = left.bind(cols);
        const right_func = right.bind(cols);
        return (idx: number, cols: Vector[]) => left_func(idx, cols) <= right_func(idx, cols);
    }

    protected _bindColLit(cols: Vector<any>[], col: Col     , lit: Literal  ): PredicateFunc {
        const col_func = col.bind(cols);
        return (idx: number, cols: Vector[]) => col_func(idx, cols) <= lit.v;
    }
}

class GTeq extends ComparisonPredicate {
    protected _bindLitLit(_: Vector<any>[], left: Literal, right: Literal): PredicateFunc {
        const rtrn: boolean = left.v >= right.v;
        return () => rtrn;
    }

    protected _bindColCol(cols: Vector<any>[], left: Col, right: Col): PredicateFunc {
        const left_func = left.bind(cols);
        const right_func = right.bind(cols);
        return (idx: number, cols: Vector[]) => left_func(idx, cols) >= right_func(idx, cols);
    }

    protected _bindColLit(cols: Vector<any>[], col: Col, lit: Literal): PredicateFunc {
        const col_func = col.bind(cols);
        return (idx: number, cols: Vector[]) => col_func(idx, cols) >= lit.v;
    }
    //eval(idx: number, cols: Vector[]) {
    //    return this.left.eval(idx, cols) >= this.right.eval(idx, cols);
    //}
    //emitString() {
    //    return `${this.left.emitString()} >= ${this.right.emitString()}`
    //}
    //createDictionaryEval(schema, lit: Literal, col: Col): (idx: number, cols: Vector[]) => boolean {
    //    return this.eval;
    //}
}

export function lit(n: number): Value<any> { return new Literal(n); }
export function col(n: string): Value<any> { return new Col(n); }

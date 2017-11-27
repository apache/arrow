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

export class Int128 {
    constructor (private buffer: Int32Array) {
        // buffer[0] MSB (high)
        // buffer[1]
        // buffer[2]
        // buffer[3] LSB (low)
    }

    high(): Int64 {
        return new Int64(new Int32Array(this.buffer.buffer, this.buffer.byteOffset, 2));
    }

    low(): Int64 {
        return new Int64(new Int32Array(this.buffer.buffer, this.buffer.byteOffset + 8, 2));
    }

    negate(): Int128 {
        this.buffer[3] = ~this.buffer[3] + 1;
        this.buffer[2] = ~this.buffer[2];
        this.buffer[1] = ~this.buffer[1];
        this.buffer[0] = ~this.buffer[0];

        if (this.buffer[3] == 0) ++this.buffer[2];
        if (this.buffer[2] == 0) ++this.buffer[1];
        if (this.buffer[1] == 0) ++this.buffer[0];
        return this;
    }

    times(other: Int128): Int128 {
        // Break the left and right numbers into 16 bit chunks
        // so that we can multiply them without overflow.
        const L0 = new Uint64(new Uint32Array([0, this.buffer[0]]))
        const L1 = new Uint64(new Uint32Array([0, this.buffer[1]]))
        const L2 = new Uint64(new Uint32Array([0, this.buffer[2]]))
        const L3 = new Uint64(new Uint32Array([0, this.buffer[3]]))

        const R0 = new Uint64(new Uint32Array([0, other.buffer[0]]))
        const R1 = new Uint64(new Uint32Array([0, other.buffer[1]]))
        const R2 = new Uint64(new Uint32Array([0, other.buffer[2]]))
        const R3 = new Uint64(new Uint32Array([0, other.buffer[3]]))
        //console.log(`L0: ${L0.hex()}`);
        //console.log(`L1: ${L1.hex()}`);
        //console.log(`L2: ${L2.hex()}`);
        //console.log(`L3: ${L3.hex()}`);
        //console.log(`R0: ${R0.hex()}`);
        //console.log(`R1: ${R1.hex()}`);
        //console.log(`R2: ${R2.hex()}`);
        //console.log(`R3: ${R3.hex()}`);
        //console.log(`${this.hex()}`);

        let product = Uint64.multiply(L3, R3);
        //console.log(`product: ${product.hex()}`);
        this.buffer[3] = product.low();
        //console.log(`${this.hex()}`);

        let sum = new Uint64(new Uint32Array([0, product.high()]));
        //console.log(`sum: ${sum.hex()}`);

        product = Uint64.multiply(L2, R3);
        //console.log(`product: ${product.hex()}`);
        sum.plus(product);
        //console.log(`sum: ${sum.hex()}`);

        product = Uint64.multiply(L3, R2);
        //console.log(`product: ${product.hex()}`);
        sum.plus(product);
        //console.log(`sum: ${sum.hex()}`);

        this.buffer[2] = sum.low()
        //console.log(`${this.hex()}`);

        this.buffer[0] = (sum.lessThan(product) ? 1 : 0);
        //if (sum.lessThan(product)) {
        //    this.buffer[0] += 1;
        //}
        //console.log(`${this.hex()}`);

        //high.plus(new Int64(new Int32Array([0, sum.high()])));
        this.buffer[1] = sum.high();
        //console.log(`${this.hex()}`);
        let high = new Uint64(new Uint32Array(this.buffer.buffer, this.buffer.byteOffset, 2));
        high.plus(Uint64.multiply(L1, R3))
            .plus(Uint64.multiply(L2, R2))
            .plus(Uint64.multiply(L3, R1));
        //console.log(`${this.hex()}`);
        this.buffer[0] += Uint64.multiply(L0, R3)
                        .plus(Uint64.multiply(L1, R2))
                        .plus(Uint64.multiply(L2, R1))
                        .plus(Uint64.multiply(L3, R0)).low();
        //console.log(`${this.hex()}`);

        return this;
    }

    plus(other: Int128): Int128 {
        let sums = new Uint32Array(4);
        sums[3] = (this.buffer[3] + other.buffer[3]) >>> 0;
        sums[2] = (this.buffer[2] + other.buffer[2]) >>> 0;
        sums[1] = (this.buffer[1] + other.buffer[1]) >>> 0;
        sums[0] = (this.buffer[0] + other.buffer[0]) >>> 0;

        if (sums[3] < (this.buffer[3] >>> 0)) {
            ++sums[2];
        }
        if (sums[2] < (this.buffer[2] >>> 0)) {
            ++sums[1];
        }
        if (sums[1] < (this.buffer[1] >>> 0)) {
            ++sums[0];
        }

        this.buffer[3] = sums[3];
        this.buffer[2] = sums[2];
        this.buffer[1] = sums[1];
        this.buffer[0] = sums[0];

        return this;
    }

    hex(): string {
        return `${intAsHex(this.buffer[0])} ${intAsHex(this.buffer[1])} ${intAsHex(this.buffer[2])} ${intAsHex(this.buffer[3])}`;
    }

    static multiply(left: Int128, right: Int128): Int128 {
        let rtrn = new Int128(new Int32Array(left.buffer));
        return rtrn.times(right);
    }

    static add(left: Int128, right: Int128): Int128 {
        let rtrn = new Int128(new Int32Array(left.buffer));
        return rtrn.plus(right);
    }

    static fromString(str: string): Int128 {
        //DCHECK_NE(out, NULLPTR) << "Decimal128 output variable cannot be NULLPTR";
        //DCHECK_EQ(*out, 0)
            //<< "When converting a string to Decimal128 the initial output must be 0";


        //DCHECK_GT(length, 0) << "length of parsed decimal string should be greater than 0";
        const negate = str.startsWith("-");
        const length = str.length;

        let out = new Int128(new Int32Array([0, 0, 0, 0]));
        1002111867823618826746863804903129070
        for (let posn = negate ? 1 : 0; posn < length;) {
            console.log(posn);
            const group = kInt32DecimalDigits < length - posn ?
                          kInt32DecimalDigits : length - posn;
            console.log(group);
            const chunk = new Int128(new Int32Array([0, 0, 0, parseInt(str.substr(posn, group), 10)]));
            console.log(chunk);
            const multiple = new Int128(new Int32Array([0, 0, 0, kPowersOfTen[group]]));
            console.log(multiple);

            out.times(multiple);
            console.log(out);
            out.plus(chunk);
            console.log(out);

            posn += group;
        }

        return negate ? out.negate() : out;
    }
}

const carryBit16 = 1 << 16;

//function PRINT(name: string, value: number) {
//    if (value < 0) {
//        value = 0xFFFFFFFF + value + 1
//    }
//    console.log(`${name}: 0x${value.toString(16)}`);
//}


function intAsHex(value: number): string {
    if (value < 0) {
        value = 0xFFFFFFFF + value + 1
    }
    return `0x${value.toString(16)}`;
}

export class BaseInt64<T extends (Int32Array|Uint32Array)> {
    constructor (protected buffer: T) {}

    high(): number { return this.buffer[0]; }
    low (): number { return this.buffer[1]; }

    protected _times(other: BaseInt64<T>) {
        // Break the left and right numbers into 16 bit chunks
        // so that we can multiply them without overflow.
        const L = new Uint32Array([
            this.buffer[0] >>> 16,
            this.buffer[0] & 0xFFFF,
            this.buffer[1] >>> 16,
            this.buffer[1] & 0xFFFF
        ]);

        const R = new Uint32Array([
            other.buffer[0] >>> 16,
            other.buffer[0] & 0xFFFF,
            other.buffer[1] >>> 16,
            other.buffer[1] & 0xFFFF
        ]);

        // -5 * -5
        // L0: 0x0000ffff
        // L1: 0x0000ffff
        // L2: 0x0000ffff
        // L3: 0x0000fffb

        // R0: 0x0000ffff
        // R1: 0x0000ffff
        // R2: 0x0000ffff
        // R3: 0x0000fffb

        //     0xfff60019
        let product = L[3] * R[3];
        //PRINT("product", product);
        //     0x00000019
        this.buffer[1] = product & 0xFFFF;
        //PRINT("this.buffer[1]", this.buffer[1]);

        //     0x0000fff6
        let sum = product >>> 16;
        //PRINT("sum", sum);

        //     0xfffa0005
        product = L[2] * R[3];
        //PRINT("product", product);
        //     0xfffafffb
        sum += product;
        //PRINT("sum", sum);

        //     0xfffa0005
        product = (L[3] * R[2]) >>> 0;
        //PRINT("product", product);
        //     0xfff50000
        sum += product;
        //PRINT("sum", sum);

        //     0x00000019
        this.buffer[1] += sum << 16;
        //PRINT("this.buffer[1]", this.buffer[1]);

        //     0x00010000
        this.buffer[0] = (sum >>> 0 < product ? carryBit16 : 0);
        //PRINT("this.buffer[0]", this.buffer[0]);
        if (sum < product) {
        //     0x00020000
            //this.buffer[0] += carryBit16;
        }

        //     0x0002fff5
        this.buffer[0] += sum >>> 16;
        //PRINT("this.buffer[0]", this.buffer[0]);
        //               0xfffa0005   + 0xfffe0001    + 0xfffa0005 = 0xfff2000b
        //     0xfff50000
        this.buffer[0] += L[1] * R[3] + L[2] * R[2] + L[3] * R[1];
        //PRINT("this.buffer[0]", this.buffer[0]);
        //                0xfffa0005   + 0xfffe0001  + 0xfffe0001  + 0xfffa0005 = 0xfff0000c
        //
        this.buffer[0] += (L[0] * R[3] + L[1] * R[2] + L[2] * R[1] + L[3] * R[0]) << 16;
        //PRINT("this.buffer[0]", this.buffer[0]);

        return this;
      }

    protected _plus(other: BaseInt64<T>) {
        const sum = (this.buffer[1] + other.buffer[1]) >>> 0;
        this.buffer[0] += other.buffer[0];
        if (sum < (this.buffer[1] >>> 0)) {
          ++this.buffer[0];
        }
        this.buffer[1] = sum;
    }

    lessThan(other: BaseInt64<T>): boolean {
        return this.buffer[0] < other.buffer[0] ||
            (this.buffer[0] === other.buffer[0] && this.buffer[1] < other.buffer[1]);
    }

    equals(other: BaseInt64<T>): boolean {
        return this.buffer[0] === other.buffer[0] && this.buffer[1] == other.buffer[1];
    }

    greaterThan(other: BaseInt64<T>): boolean {
        return other.lessThan(this);
    }

    hex(): string {
        return `${intAsHex(this.buffer[0])} ${intAsHex(this.buffer[1])}`;
    }
}

export class Uint64 extends BaseInt64<Uint32Array> {
    times(other: Uint64): Uint64 {
        this._times(other);
        return this;
    }

    plus(other: Uint64): Uint64 {
        this._plus(other);
        return this;
    }

    static multiply(left: Uint64, right: Uint64): Uint64 {
        let rtrn = new Uint64(new Uint32Array(left.buffer));
        return rtrn.times(right);
    }

    static add(left: Uint64, right: Uint64): Uint64 {
        let rtrn = new Uint64(new Uint32Array(left.buffer));
        return rtrn.plus(right);
    }
}

const kInt32DecimalDigits = 8;
const kPowersOfTen = [1,
                      10,
                      100,
                      1000,
                      10000,
                      100000,
                      1000000,
                      10000000,
                      100000000]


export class Int64 extends BaseInt64<Int32Array> {
    negate(): Int64 {
        this.buffer[1] = ~this.buffer[1] + 1;
        this.buffer[0] = ~this.buffer[0];

        if (this.buffer[1] == 0) ++this.buffer[0];
        return this;
    }

    times(other: Int64): Int64 {
        this._times(other);
        return this;
    }

    plus(other: Int64): Int64 {
        this._plus(other);
        return this;
    }

    static fromString(str: string): Int64 {
        //DCHECK_NE(out, NULLPTR) << "Decimal128 output variable cannot be NULLPTR";
        //DCHECK_EQ(*out, 0)
            //<< "When converting a string to Decimal128 the initial output must be 0";


        //DCHECK_GT(length, 0) << "length of parsed decimal string should be greater than 0";
        const negate = str.startsWith("-")
        if (negate) str = str.substr(1);
        const length = str.length;

        let out = new Int64(new Int32Array([0, 0]));
        for (let posn = 0; posn < length;) {
            const group = kInt32DecimalDigits < length - posn ?
                          kInt32DecimalDigits : length - posn;
            const chunk = new Int64(new Int32Array([0, parseInt(str.substr(posn, group), 10)]));
            const multiple = new Int64(new Int32Array([0, kPowersOfTen[group]]));

            out.times(multiple);
            out.plus(chunk);

            posn += group;
        }

        return negate ? out.negate() : out;
    }

    static multiply(left: Int64, right: Int64): Int64 {
        let rtrn = new Int64(new Int32Array(left.buffer));
        return rtrn.times(right);
    }

    static add(left: Int64, right: Int64): Int64 {
        let rtrn = new Int64(new Int32Array(left.buffer));
        return rtrn.plus(right);
    }
}

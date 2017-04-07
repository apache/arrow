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

import { BitArray } from './bitarray';
import { TextDecoder } from 'text-encoding';
import { org } from './Arrow_generated';

var Field = org.apache.arrow.flatbuf.Field;
var Type = org.apache.arrow.flatbuf.Type;

interface ArrayView {
    slice(start: number, end: number) : ArrayView
    toString() : string
}

export abstract class Vector {
    field: any;
    name: string;
    length: number;
    null_count: number;

    constructor(field) {
        this.field = field;
        this.name = field.name();
    }
    /* Access datum at index i */
    abstract get(i);
    /* Return array representing data in the range [start, end) */
    abstract slice(start: number, end: number);
    /* Return array of internal vectors */
    abstract getChildVectors();

    /**
      * Use recordBatch fieldNodes and Buffers to construct this Vector
      *   node: org.apache.arrow.flatbuf.FieldNode
      *   buffers: org.apache.arrow.flatbuf.Buffer[]
      */
    public loadData(node, buffers) {
        this.length = node.length().low;
        this.null_count = node.nullCount().low;
        this.loadBuffers(node, buffers);
    }

    protected abstract loadBuffers(node, buffers);


    /*
    static loadValidityBuffer(recordBatch, buffer, bufReader, baseOffset) : BitArray {
        var buf_meta = recordBatch.buffers(bufReader.index);
        var offset = baseOffset + buf_meta.offset().low;
        var length = buf_meta.length().low;
        bufReader.index += 1|0;
        return new BitArray(buffer, offset, length*8);
    }

    static loadOffsetBuffer(recordBatch, buffer, bufReader, baseOffset) : Int32Array {
        var buf_meta = recordBatch.buffers(bufReader.index);
        var offset = baseOffset + buf_meta.offset().low;
        var length = buf_meta.length().low/Int32Array.BYTES_PER_ELEMENT;
        bufReader.index += 1|0;
        return new Int32Array(buffer, offset, length);
    }
    */

    /**
      * Helper function for loading a VALIDITY buffer (for Nullable types)
      *   buffer: org.apache.arrow.flatbuf.Buffer
      */
    static loadValidityBuffer(buffer) : BitArray {
console.log("validity buffer " + buffer.bb.bytes().length + " " + buffer.bb_pos + " " + buffer.offset().low + " " + buffer.length().low);
        var v = new BitArray(buffer.bb.bytes(), buffer.bb_pos /*+ buffer.offset().low*/, buffer.length().low * 8);
console.log(v.get(0) + " " + v.get(1) + " " + v.get(2) + " " + v.get(3) + " " + v.get(4) + " " + v.get(5) + " ")
        return v;
    }

    /**
      * Helper function for loading an OFFSET buffer
      *   buffer: org.apache.arrow.flatbuf.Buffer
      */
    static loadOffsetBuffer(buffer) : Int32Array {
        return new Int32Array(buffer.bb.bytes(), buffer.bb_pos + buffer.offset().low, buffer.length().low / Int32Array.BYTES_PER_ELEMENT);
    }

}

class SimpleVector<T extends ArrayView> extends Vector {
    protected dataView: T;
    private TypedArray: { new(buffer: any, offset: number, length: number): T, BYTES_PER_ELEMENT: number };

    constructor (field, TypedArray: { new(buffer: any, offset: number, length: number): T, BYTES_PER_ELEMENT: number }) {
        super(field);
        this.TypedArray = TypedArray;
    }

    getChildVectors() {
        return [];
    }

    get(i) {
        return this.dataView[i];
    }

    loadBuffers(node, buffers) {
        this.loadDataBuffer(buffers[0]);
    }

    /**
      * buffer: org.apache.arrow.flatbuf.Buffer
      */
    protected loadDataBuffer(buffer) {
        this.dataView = new this.TypedArray(buffer, buffer.offset().low, buffer.length().low / this.TypedArray.BYTES_PER_ELEMENT);
    }

    getDataView() {
        return this.dataView;
    }

    toString() {
        return this.dataView.toString();
    }

    slice(start, end) {
        return this.dataView.slice(start, end);
    }
}

class NullableSimpleVector<T extends ArrayView> extends SimpleVector<T> {
    private validityView: BitArray;

    get(i: number) {
        if (this.validityView.get(i)) {
            return this.dataView[i];
        } else {
          return null;
        }
    }

    loadBuffers(node, buffers) {
        this.validityView = Vector.loadValidityBuffer(buffers[0]);
        this.loadDataBuffer(buffers[1]);
    }
}

class Uint8Vector   extends SimpleVector<Uint8Array>   { constructor(field) { super(field, Uint8Array);   }; }
class Uint16Vector  extends SimpleVector<Uint16Array>  { constructor(field) { super(field, Uint16Array);  }; }
class Uint32Vector  extends SimpleVector<Uint32Array>  { constructor(field) { super(field, Uint32Array);  }; }
class Int8Vector    extends SimpleVector<Uint8Array>   { constructor(field) { super(field, Uint8Array);   }; }
class Int16Vector   extends SimpleVector<Uint16Array>  { constructor(field) { super(field, Uint16Array);  }; }
class Int32Vector   extends SimpleVector<Uint32Array>  { constructor(field) { super(field, Uint32Array);  }; }
class Float32Vector extends SimpleVector<Float32Array> { constructor(field) { super(field, Float32Array); }; }
class Float64Vector extends SimpleVector<Float64Array> { constructor(field) { super(field, Float64Array); }; }

class NullableUint8Vector   extends NullableSimpleVector<Uint8Array>   { constructor(field) { super(field, Uint8Array);   }; }
class NullableUint16Vector  extends NullableSimpleVector<Uint16Array>  { constructor(field) { super(field, Uint16Array);  }; }
class NullableUint32Vector  extends NullableSimpleVector<Uint32Array>  { constructor(field) { super(field, Uint32Array);  }; }
class NullableInt8Vector    extends NullableSimpleVector<Uint8Array>   { constructor(field) { super(field, Uint8Array);   }; }
class NullableInt16Vector   extends NullableSimpleVector<Uint16Array>  { constructor(field) { super(field, Uint16Array);  }; }
class NullableInt32Vector   extends NullableSimpleVector<Uint32Array>  { constructor(field) { super(field, Uint32Array);  }; }
class NullableFloat32Vector extends NullableSimpleVector<Float32Array> { constructor(field) { super(field, Float32Array); }; }
class NullableFloat64Vector extends NullableSimpleVector<Float64Array> { constructor(field) { super(field, Float64Array); }; }

class Utf8Vector extends SimpleVector<Uint8Array> {
    protected offsetView: Int32Array;
    static decoder: TextDecoder = new TextDecoder('utf8');

    constructor(field) {
        super(field, Uint8Array);
    }

    loadBuffers(node, buffers) {
        this.offsetView = Vector.loadOffsetBuffer(buffers[0]);
        this.loadDataBuffer(buffers[1]);
    }

    get(i) {
        return Utf8Vector.decoder.decode
            (this.dataView.slice(this.offsetView[i], this.offsetView[i + 1]));
    }

    slice(start: number, end: number) {
        var rtrn: string[] = [];
        for (var i: number = start; i < end; i += 1|0) {
            rtrn.push(this.get(i));
        }
        return rtrn;
    }
}

class NullableUtf8Vector extends Utf8Vector {
    private validityView: BitArray;

    loadBuffers(node, buffers) {
        this.validityView = Vector.loadValidityBuffer(buffers[0]);
        this.offsetView = Vector.loadOffsetBuffer(buffers[1]);
        this.loadDataBuffer(buffers[2]);
    }

    get(i) {
        if (this.validityView.get(i)) {
            return super.get(i);
        } else {
            return null;
        }
    }
}

// Nested Types
class ListVector extends Uint32Vector {
    private dataVector: Vector;

    constructor(field, dataVector: Vector) {
        super(field);
        this.dataVector = dataVector;
    }

    getChildVectors() {
        return [this.dataVector];
    }

    loadBuffers(node, buffers) {
        super.loadBuffers(node, buffers);
        this.length -= 1;
    }

    get(i) {
        var offset = super.get(i)
        if (offset === null) {
            return null;
        }
        var next_offset = super.get(i + 1)
        return this.dataVector.slice(offset, next_offset)
    }

    toString() {
        return "length: " + (this.length);
    }

    slice(start : number, end : number) { return []; };
}

class NullableListVector extends ListVector {
    private validityView: BitArray;

    loadBuffers(node, buffers) {
        this.validityView = Vector.loadValidityBuffer(buffers[0]);
        this.loadDataBuffer(buffers[1]);
        this.length -= 1;
    }

    get(i) {
        if (this.validityView.get(i)) {
            return super.get(i);
        } else {
            return null;
        }
    }
}

class FixedSizeListVector extends Vector {
    private dataVector: Vector;
    private size: number

    constructor(field, size: number, dataVector: Vector) {
        super(field);
        this.dataVector = dataVector;
        this.size = size;
    }

    getChildVectors() {
        return [this.dataVector];
    }

    loadBuffers(node, buffers) {
        // no buffers to load
    }

    get(i: number) {
        return this.dataVector.slice(i * this.size, (i + 1) * this.size);
    }

    // TODO
    slice(start : number, end : number) { return []; };
}

class NullableFixedSizeListVector extends FixedSizeListVector {
    private validityView: BitArray;

    loadBuffers(node, buffers) {
        this.validityView = Vector.loadValidityBuffer(buffers[0]);
    }

    get(i: number) {
        if (this.validityView.get(i)) {
            return super.get(i);
        } else {
            return null;
        }
    }
}

class StructVector extends Vector {
    private validityView: BitArray;
    private vectors: Vector[];

    constructor(field, vectors: Vector[]) {
        super(field);
        this.vectors = vectors;
    }

    getChildVectors() {
        return this.vectors;
    }

    loadBuffers(node, buffers) {
        this.validityView = Vector.loadValidityBuffer(buffers[0]);
    }

    get(i : number) {
        if (this.validityView.get(i)) {
          return this.vectors.map((v: Vector) => v.get(i));
        } else {
            return null;
        }
    }

    slice(start : number, end : number) {
        var rtrn = [];
        for (var i: number = start; i < end; i += 1|0) {
            rtrn.push(this.get(i));
        }
        return rtrn;
    }
}

class DateVector extends SimpleVector<Uint32Array> {
    constructor(field) {
        super(field, Uint32Array);
    }

    get (i) {
        return new Date(super.get(2*i+1)*Math.pow(2,32) + super.get(2*i));
    }
}

class NullableDateVector extends DateVector {
    private validityView: BitArray;

    loadBuffers(node, buffers) {
        this.validityView = Vector.loadValidityBuffer(buffers[0]);
        this.loadDataBuffer(buffers[1]);
    }

    get (i) {
        if (this.validityView.get(i)) {
            return super.get(i);
        } else {
            return null;
        }
    }
}

var BASIC_TYPES = [Type.Int, Type.FloatingPoint, Type.Utf8, Type.Date];

export function vectorFromField(field) : Vector {
    var typeType = field.typeType();
    if (BASIC_TYPES.indexOf(typeType) >= 0) {
        var type = field.typeType();
        if (type === Type.Int) {
            type = field.type(new org.apache.arrow.flatbuf.Int());
            var VectorConstructor : {new(Field): Vector};
            if (type.isSigned()) {
                if (type.bitWidth() == 32)
                    VectorConstructor = field.nullable() ? NullableInt32Vector : Int32Vector;
                else if (type.bitWidth() == 16)
                    VectorConstructor = field.nullable() ? NullableInt16Vector : Int16Vector;
                else if (type.bitWidth() == 8)
                    VectorConstructor = field.nullable() ? NullableInt8Vector : Int8Vector;
            } else {
                if (type.bitWidth() == 32)
                    VectorConstructor = field.nullable() ? NullableUint32Vector : Uint32Vector;
                else if (type.bitWidth() == 16)
                    VectorConstructor = field.nullable() ? NullableUint16Vector : Uint16Vector;
                else if (type.bitWidth() == 8)
                    VectorConstructor = field.nullable() ? NullableUint8Vector : Uint8Vector;
            }
        } else if (type === Type.FloatingPoint) {
            type = field.type(new org.apache.arrow.flatbuf.FloatingPoint());
            if (type.precision() == org.apache.arrow.flatbuf.Precision.SINGLE)
                VectorConstructor = field.nullable() ? NullableFloat32Vector : Float32Vector;
            else if (type.precision() == org.apache.arrow.flatbuf.Precision.DOUBLE)
                VectorConstructor = field.nullable() ? NullableFloat64Vector : Float64Vector;
        } else if (type === Type.Utf8) {
            VectorConstructor = field.nullable() ? NullableUtf8Vector : Utf8Vector;
        } else if (type === Type.Date) {
            VectorConstructor = field.nullable() ? NullableDateVector : DateVector;
        }

        return new VectorConstructor(field);
    } else if (typeType === Type.List) {
        var dataVector = vectorFromField(field.children(0));
        return field.nullable() ? new NullableListVector(field, dataVector) : new ListVector(field, dataVector);
    } else if (typeType === Type.FixedSizeList) {
        var dataVector = vectorFromField(field.children(0));
        var type = field.type(new org.apache.arrow.flatbuf.FixedSizeList());
        if (field.nullable()) {
          return new NullableFixedSizeListVector(field, type.size, dataVector);
        } else {
          return new FixedSizeListVector(field, type.size, dataVector);
        }
     } else if (typeType === Type.Struct_) {
        var vectors : Vector[] = [];
        for (var i : number = 0; i < field.childrenLength(); i += 1|0) {
            vectors.push(vectorFromField(field.children(i)));
        }
        return new StructVector(field, vectors);
    }
}

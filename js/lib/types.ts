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
    /* Return array of child vectors, for container types */
    abstract getChildVectors();

    /**
     * Use recordBatch fieldNodes and Buffers to construct this Vector
     *   bb: flatbuffers.ByteBuffer
     *   node: org.apache.arrow.flatbuf.FieldNode
     *   buffers: { offset: number, length: number }[]
     */
    public loadData(bb, node, buffers) {
        this.length = node.length().low;
        this.null_count = node.nullCount().low;
        this.loadBuffers(bb, node, buffers);
    }

    protected abstract loadBuffers(bb, node, buffers);

    /**
     * Helper function for loading a VALIDITY buffer (for Nullable types)
     *   bb: flatbuffers.ByteBuffer
     *   buffer: org.apache.arrow.flatbuf.Buffer
     */
    static loadValidityBuffer(bb, buffer) : BitArray {
        var arrayBuffer = bb.bytes_.buffer;
        var offset = bb.bytes_.byteOffset + buffer.offset;
        return new BitArray(arrayBuffer, offset, buffer.length * 8);
    }

    /**
     * Helper function for loading an OFFSET buffer
     *   buffer: org.apache.arrow.flatbuf.Buffer
     */
    static loadOffsetBuffer(bb, buffer) : Int32Array {
        var arrayBuffer = bb.bytes_.buffer;
        var offset  = bb.bytes_.byteOffset + buffer.offset;
        var length = buffer.length / Int32Array.BYTES_PER_ELEMENT;
        return new Int32Array(arrayBuffer, offset, length);
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

    loadBuffers(bb, node, buffers) {
        this.loadDataBuffer(bb, buffers[0]);
    }

    /**
      * buffer: org.apache.arrow.flatbuf.Buffer
      */
    protected loadDataBuffer(bb, buffer) {
        var arrayBuffer = bb.bytes_.buffer;
        var offset  = bb.bytes_.byteOffset + buffer.offset;
        var length = buffer.length / this.TypedArray.BYTES_PER_ELEMENT;
        this.dataView = new this.TypedArray(arrayBuffer, offset, length);
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

    protected validityView: BitArray;

    get(i: number) {
        if (this.validityView.get(i)) {
            return this.dataView[i];
        } else {
          return null;
        }
    }

    loadBuffers(bb, node, buffers) {
        this.validityView = Vector.loadValidityBuffer(bb, buffers[0]);
        this.loadDataBuffer(bb, buffers[1]);
    }

    getValidityVector() {
        return this.validityView;
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

class Uint64Vector extends SimpleVector<Uint32Array>  {
    constructor(field) {
        super(field, Uint32Array);
    }

    get(i: number) {
        return { low: this.dataView[i * 2], high: this.dataView[(i * 2) + 1] };
    }
}

class NullableUint64Vector extends NullableSimpleVector<Uint32Array>  {
    constructor(field) {
        super(field, Uint32Array);
    }

    get(i: number) {
        if (this.validityView.get(i)) {
            return { low: this.dataView[i * 2], high: this.dataView[(i * 2) + 1] };
        } else {
          return null;
        }
    }
}

class Int64Vector extends NullableSimpleVector<Uint32Array>  {
    constructor(field) {
        super(field, Uint32Array);
    }

    get(i: number) {
        return { low: this.dataView[i * 2], high: this.dataView[(i * 2) + 1] };
    }
}

class NullableInt64Vector extends NullableSimpleVector<Uint32Array>  {
    constructor(field) {
        super(field, Uint32Array);
    }

    get(i: number) {
        if (this.validityView.get(i)) {
            return { low: this.dataView[i * 2], high: this.dataView[(i * 2) + 1] };
        } else {
          return null;
        }
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

    loadBuffers(bb, node, buffers) {
        this.validityView = Vector.loadValidityBuffer(bb, buffers[0]);
        this.loadDataBuffer(bb, buffers[1]);
    }

    get (i) {
        if (this.validityView.get(i)) {
            return super.get(i);
        } else {
            return null;
        }
    }

    getValidityVector() {
        return this.validityView;
    }
}

class Utf8Vector extends SimpleVector<Uint8Array> {
    protected offsetView: Int32Array;
    static decoder: TextDecoder = new TextDecoder('utf8');

    constructor(field) {
        super(field, Uint8Array);
    }

    loadBuffers(bb, node, buffers) {
        this.offsetView = Vector.loadOffsetBuffer(bb, buffers[0]);
        this.loadDataBuffer(bb, buffers[1]);
    }

    get(i) {
        return Utf8Vector.decoder.decode(this.dataView.slice(this.offsetView[i], this.offsetView[i + 1]));
    }

    slice(start: number, end: number) {
        var result: string[] = [];
        for (var i: number = start; i < end; i += 1|0) {
            result.push(this.get(i));
        }
        return result;
    }

    getOffsetView() {
        return this.offsetView;
    }
}

class NullableUtf8Vector extends Utf8Vector {
    private validityView: BitArray;

    loadBuffers(bb, node, buffers) {
        this.validityView = Vector.loadValidityBuffer(bb, buffers[0]);
        this.offsetView = Vector.loadOffsetBuffer(bb, buffers[1]);
        this.loadDataBuffer(bb, buffers[2]);
    }

    get(i) {
        if (this.validityView.get(i)) {
            return super.get(i);
        } else {
            return null;
        }
    }

    getValidityVector() {
        return this.validityView;
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

    loadBuffers(bb, node, buffers) {
        super.loadBuffers(bb, node, buffers);
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

    slice(start: number, end: number) {
        var result = [];
        for (var i = start; i < end; i += 1|0) {
            result.push(this.get(i));
        }
        return result;
    }
}

class NullableListVector extends ListVector {
    private validityView: BitArray;

    loadBuffers(bb, node, buffers) {
        this.validityView = Vector.loadValidityBuffer(bb, buffers[0]);
        this.loadDataBuffer(bb, buffers[1]);
        this.length -= 1;
    }

    get(i) {
        if (this.validityView.get(i)) {
            return super.get(i);
        } else {
            return null;
        }
    }

    getValidityVector() {
        return this.validityView;
    }
}

class FixedSizeListVector extends Vector {
    private size: number
    private dataVector: Vector;

    constructor(field, size: number, dataVector: Vector) {
        super(field);
        this.size = size;
        this.dataVector = dataVector;
    }

    getChildVectors() {
        return [this.dataVector];
    }

    loadBuffers(bb, node, buffers) {
        // no buffers to load
    }

    get(i: number) {
        return this.dataVector.slice(i * this.size, (i + 1) * this.size);
    }

    slice(start : number, end : number) {
        var result = [];
        for (var i = start; i < end; i += 1|0) {
            result.push(this.get(i));
        }
        return result;
    }

    getListSize() {
        return this.size;
    }
}

class NullableFixedSizeListVector extends FixedSizeListVector {
    private validityView: BitArray;

    loadBuffers(bb, node, buffers) {
        this.validityView = Vector.loadValidityBuffer(bb, buffers[0]);
    }

    get(i: number) {
        if (this.validityView.get(i)) {
            return super.get(i);
        } else {
            return null;
        }
    }

    getValidityVector() {
        return this.validityView;
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

    loadBuffers(bb, node, buffers) {
        this.validityView = Vector.loadValidityBuffer(bb, buffers[0]);
    }

    get(i : number) {
        if (this.validityView.get(i)) {
          return this.vectors.map((v: Vector) => v.get(i));
        } else {
            return null;
        }
    }

    slice(start : number, end : number) {
        var result = [];
        for (var i = start; i < end; i += 1|0) {
            result.push(this.get(i));
        }
        return result;
    }

    getValidityVector() {
        return this.validityView;
    }
}

class DictionaryVector extends Vector {

    private indices: Vector;
    private dictionary: Vector;

    constructor (field, indices: Vector, dictionary: Vector) {
        super(field);
        this.indices = indices;
        this.dictionary = dictionary;
    }

    get(i) {
        var encoded = this.indices.get(i);
        if (encoded == null) {
            return null;
        } else {
            return this.dictionary.get(encoded);
        }
    }

    /** Get the dictionary encoded value */
    public getEncoded(i) {
        return this.indices.get(i);
    }

    slice(start, end) {
        return this.indices.slice(start, end); // TODO decode
    }

    getChildVectors() {
        return this.indices.getChildVectors();
    }

    loadBuffers(bb, node, buffers) {
        this.indices.loadData(bb, node, buffers);
    }

    /** Get the index (encoded) vector */
    public getIndexVector() {
        return this.indices;
    }

    /** Get the dictionary vector */
    public getDictionaryVector() {
        return this.dictionary;
    }

    toString() {
        return this.indices.toString();
    }
}

export function vectorFromField(field, dictionaries) : Vector {
    var dictionary = field.dictionary(), nullable = field.nullable();
    if (dictionary == null) {
        var typeType = field.typeType();
        if (typeType === Type.List) {
            var dataVector = vectorFromField(field.children(0), dictionaries);
            return nullable ? new NullableListVector(field, dataVector) : new ListVector(field, dataVector);
        } else if (typeType === Type.FixedSizeList) {
            var dataVector = vectorFromField(field.children(0), dictionaries);
            var size = field.type(new org.apache.arrow.flatbuf.FixedSizeList()).listSize();
            if (nullable) {
              return new NullableFixedSizeListVector(field, size, dataVector);
            } else {
              return new FixedSizeListVector(field, size, dataVector);
            }
         } else if (typeType === Type.Struct_) {
            var vectors : Vector[] = [];
            for (var i : number = 0; i < field.childrenLength(); i += 1|0) {
                vectors.push(vectorFromField(field.children(i), dictionaries));
            }
            return new StructVector(field, vectors);
        } else {
            if (typeType === Type.Int) {
                var type = field.type(new org.apache.arrow.flatbuf.Int());
                return _createIntVector(field, type.bitWidth(), type.isSigned(), nullable)
            } else if (typeType === Type.FloatingPoint) {
                var precision = field.type(new org.apache.arrow.flatbuf.FloatingPoint()).precision();
                if (precision == org.apache.arrow.flatbuf.Precision.SINGLE) {
                    return nullable ? new NullableFloat32Vector(field) : new Float32Vector(field);
                } else if (precision == org.apache.arrow.flatbuf.Precision.DOUBLE) {
                    return nullable ? new NullableFloat64Vector(field) : new Float64Vector(field);
                } else {
                    throw "Unimplemented FloatingPoint precision " + precision;
                }
            } else if (typeType === Type.Utf8) {
                return nullable ? new NullableUtf8Vector(field) : new Utf8Vector(field);
            } else if (typeType === Type.Date) {
                return nullable ? new NullableDateVector(field) : new DateVector(field);
            } else {
                throw "Unimplemented type " + typeType;
            }
        }
    } else {
        // determine arrow type - default is signed 32 bit int
        var type = dictionary.indexType(), bitWidth = 32, signed = true;
        if (type != null) {
            bitWidth = type.bitWidth();
            signed = type.isSigned();
        }
        var indices = _createIntVector(field, bitWidth, signed, nullable);
        return new DictionaryVector(field, indices, dictionaries[dictionary.id().toFloat64().toString()]);
    }
}

function _createIntVector(field, bitWidth, signed, nullable) {
    if (bitWidth == 64) {
        if (signed) {
            return nullable ? new NullableInt64Vector(field) : new Int64Vector(field);
        } else {
            return nullable ? new NullableUint64Vector(field) : new Uint64Vector(field);
        }
    } else if (bitWidth == 32) {
        if (signed) {
            return nullable ? new NullableInt32Vector(field) : new Int32Vector(field);
        } else {
            return nullable ? new NullableUint32Vector(field) : new Uint32Vector(field);
        }
    } else if (bitWidth == 16) {
        if (signed) {
            return nullable ? new NullableInt16Vector(field) : new Int16Vector(field);
        } else {
            return nullable ? new NullableUint16Vector(field) : new Uint16Vector(field);
        }
    } else if (bitWidth == 8) {
        if (signed) {
            return nullable ? new NullableInt8Vector(field) : new Int8Vector(field);
        } else {
            return nullable ? new NullableUint8Vector(field) : new Uint8Vector(field);
        }
    } else {
         throw "Unimplemented Int bit width " + bitWidth;
    }
}

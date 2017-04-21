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
var arrow = org.apache.arrow;

interface ArrayView {
    slice(start: number, end: number) : ArrayView
    toString() : string
}

export abstract class Vector {
    name: string;
    length: number;
    null_count: number;
    constructor(name: string) {
        this.name = name;
    }
    /* Access datum at index i */
    abstract get(i);
    /* Return array representing data in the range [start, end) */
    abstract slice(start: number, end: number);

    /* Use recordBatch fieldNodes and Buffers to construct this Vector */
    public loadData(recordBatch: any, buffer: any, bufReader: any, baseOffset: any) {
        var fieldNode = recordBatch.nodes(bufReader.node_index);
        this.length = fieldNode.length();
        this.null_count = fieldNode.length();
        bufReader.node_index += 1|0;

        this.loadBuffers(recordBatch, buffer, bufReader, baseOffset);
    }

    protected abstract loadBuffers(recordBatch: any, buffer: any, bufReader: any, baseOffset: any);

    /* Helper function for loading a VALIDITY buffer (for Nullable types) */
    static loadValidityBuffer(recordBatch, buffer, bufReader, baseOffset) : BitArray {
        var buf_meta = recordBatch.buffers(bufReader.index);
        var offset = baseOffset + buf_meta.offset().low;
        var length = buf_meta.length().low;
        bufReader.index += 1|0;
        return new BitArray(buffer, offset, length*8);
    }

    /* Helper function for loading an OFFSET buffer */
    static loadOffsetBuffer(recordBatch, buffer, bufReader, baseOffset) : Int32Array {
        var buf_meta = recordBatch.buffers(bufReader.index);
        var offset = baseOffset + buf_meta.offset().low;
        var length = buf_meta.length().low/Int32Array.BYTES_PER_ELEMENT;
        bufReader.index += 1|0;
        return new Int32Array(buffer, offset, length);
    }

}

class SimpleVector<T extends ArrayView> extends Vector {
    protected dataView: T;
    private TypedArray: {new(buffer: any, offset: number, length: number) : T, BYTES_PER_ELEMENT: number};

    constructor (TypedArray: {new(buffer: any, offset: number, length: number): T, BYTES_PER_ELEMENT: number}, name: string) {
        super(name);
        this.TypedArray = TypedArray;
    }

    get(i) {
        return this.dataView[i];
    }

    loadBuffers(recordBatch, buffer, bufReader, baseOffset) {
        this.dataView = this.loadDataBuffer(recordBatch, buffer, bufReader, baseOffset);
    }

    loadDataBuffer(recordBatch, buffer, bufReader, baseOffset) : T {
        var buf_meta = recordBatch.buffers(bufReader.index);
        var offset = baseOffset + buf_meta.offset().low;
        var length = buf_meta.length().low/this.TypedArray.BYTES_PER_ELEMENT;
        bufReader.index += 1|0;
        return new this.TypedArray(buffer, offset, length);
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
        if (this.validityView.get(i)) return this.dataView[i];
        else                          return null
    }

    loadBuffers(recordBatch, buffer, bufReader, baseOffset) {
        this.validityView = Vector.loadValidityBuffer(recordBatch, buffer, bufReader, baseOffset);
        this.dataView = this.loadDataBuffer(recordBatch, buffer, bufReader, baseOffset);
    }

}

class Uint8Vector   extends SimpleVector<Uint8Array>   { constructor(name: string) { super(Uint8Array,  name);  }; }
class Uint16Vector  extends SimpleVector<Uint16Array>  { constructor(name: string) { super(Uint16Array, name);  }; }
class Uint32Vector  extends SimpleVector<Uint32Array>  { constructor(name: string) { super(Uint32Array, name);  }; }
class Int8Vector    extends SimpleVector<Uint8Array>   { constructor(name: string) { super(Uint8Array,  name);  }; }
class Int16Vector   extends SimpleVector<Uint16Array>  { constructor(name: string) { super(Uint16Array, name);  }; }
class Int32Vector   extends SimpleVector<Uint32Array>  { constructor(name: string) { super(Uint32Array, name);  }; }
class Float32Vector extends SimpleVector<Float32Array> { constructor(name: string) { super(Float32Array, name); }; }
class Float64Vector extends SimpleVector<Float64Array> { constructor(name: string) { super(Float64Array, name); }; }

class NullableUint8Vector   extends NullableSimpleVector<Uint8Array>   { constructor(name: string) { super(Uint8Array,  name);  }; }
class NullableUint16Vector  extends NullableSimpleVector<Uint16Array>  { constructor(name: string) { super(Uint16Array, name);  }; }
class NullableUint32Vector  extends NullableSimpleVector<Uint32Array>  { constructor(name: string) { super(Uint32Array, name);  }; }
class NullableInt8Vector    extends NullableSimpleVector<Uint8Array>   { constructor(name: string) { super(Uint8Array,  name);  }; }
class NullableInt16Vector   extends NullableSimpleVector<Uint16Array>  { constructor(name: string) { super(Uint16Array, name);  }; }
class NullableInt32Vector   extends NullableSimpleVector<Uint32Array>  { constructor(name: string) { super(Uint32Array, name);  }; }
class NullableFloat32Vector extends NullableSimpleVector<Float32Array> { constructor(name: string) { super(Float32Array, name); }; }
class NullableFloat64Vector extends NullableSimpleVector<Float64Array> { constructor(name: string) { super(Float64Array, name); }; }

class Utf8Vector extends SimpleVector<Uint8Array> {
    protected offsetView: Int32Array;
    static decoder: TextDecoder = new TextDecoder('utf8');

    constructor(name: string) {
        super(Uint8Array, name);
    }

    loadBuffers(recordBatch, buffer, bufReader, baseOffset) {
        this.offsetView = Vector.loadOffsetBuffer(recordBatch, buffer, bufReader, baseOffset);
        this.dataView = this.loadDataBuffer(recordBatch, buffer, bufReader, baseOffset);
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

    loadBuffers(recordBatch, buffer, bufReader, baseOffset) {
        this.validityView = Vector.loadValidityBuffer(recordBatch, buffer, bufReader, baseOffset);
        this.offsetView = Vector.loadOffsetBuffer(recordBatch, buffer, bufReader, baseOffset);
        this.dataView = this.loadDataBuffer(recordBatch, buffer, bufReader, baseOffset);
    }

    get(i) {
        if (!this.validityView.get(i)) return null;
        return super.get(i);
    }
}

// Nested Types
class ListVector extends Uint32Vector {
    private dataVector: Vector;

    constructor(name, dataVector : Vector) {
        super(name);
        this.dataVector = dataVector;
    }

    loadBuffers(recordBatch, buffer, bufReader, baseOffset) {
        super.loadBuffers(recordBatch, buffer, bufReader, baseOffset);
        this.dataVector.loadData(recordBatch, buffer, bufReader, baseOffset);
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

    loadBuffers(recordBatch, buffer, bufReader, baseOffset) {
        this.validityView = Vector.loadValidityBuffer(recordBatch, buffer, bufReader, baseOffset);
        super.loadBuffers(recordBatch, buffer, bufReader, baseOffset);
    }

    get(i) {
        if (!this.validityView.get(i)) return null;
        return super.get(i);
    }
}

class StructVector extends Vector {
    private validityView: BitArray;
    private vectors : Vector[];
    constructor(name: string, vectors: Vector[]) {
        super(name);
        this.vectors = vectors;
    }

    loadBuffers(recordBatch, buffer, bufReader, baseOffset) {
        this.validityView = Vector.loadValidityBuffer(recordBatch, buffer, bufReader, baseOffset);
        this.vectors.forEach((v: Vector) => v.loadData(recordBatch, buffer, bufReader, baseOffset));
    }

    get(i : number) {
        if (!this.validityView.get(i)) return null;
        return this.vectors.map((v: Vector) => v.get(i));
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
    constructor (name: string) {
        super(Uint32Array, name);
    }

    get (i) {
        return new Date(super.get(2*i+1)*Math.pow(2,32) + super.get(2*i));
    }
}

class NullableDateVector extends DateVector {
    private validityView: BitArray;

    loadBuffers(recordBatch, buffer, bufReader, baseOffset) {
        this.validityView = Vector.loadValidityBuffer(recordBatch, buffer, bufReader, baseOffset);
        super.loadBuffers(recordBatch, buffer, bufReader, baseOffset);
    }

    get (i) {
        if (!this.validityView.get(i)) return null;
        return super.get(i);
    }
}

var BASIC_TYPES = [arrow.flatbuf.Type.Int, arrow.flatbuf.Type.FloatingPoint, arrow.flatbuf.Type.Utf8, arrow.flatbuf.Type.Date];

export function vectorFromField(field) : Vector {
    var typeType = field.typeType();
    if (BASIC_TYPES.indexOf(typeType) >= 0) {
        var type = field.typeType();
        if (type === arrow.flatbuf.Type.Int) {
            type = field.type(new arrow.flatbuf.Int());
            var VectorConstructor : {new(string) : Vector};
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
        } else if (type === arrow.flatbuf.Type.FloatingPoint) {
            type = field.type(new arrow.flatbuf.FloatingPoint());
            if (type.precision() == arrow.flatbuf.Precision.SINGLE)
                VectorConstructor = field.nullable() ? NullableFloat32Vector : Float32Vector;
            else if (type.precision() == arrow.flatbuf.Precision.DOUBLE)
                VectorConstructor = field.nullable() ? NullableFloat64Vector : Float64Vector;
        } else if (type === arrow.flatbuf.Type.Utf8) {
            VectorConstructor = field.nullable() ? NullableUtf8Vector : Utf8Vector;
        } else if (type === arrow.flatbuf.Type.Date) {
            VectorConstructor = field.nullable() ? NullableDateVector : DateVector;
        }

        return new VectorConstructor(field.name());
    } else if (typeType === arrow.flatbuf.Type.List) {
        var dataVector = vectorFromField(field.children(0));
        return field.nullable() ? new NullableListVector(field.name(), dataVector) : new ListVector(field.name(), dataVector);
    } else if (typeType === arrow.flatbuf.Type.Struct_) {
        var vectors : Vector[] = [];
        for (var i : number = 0; i < field.childrenLength(); i += 1|0) {
            vectors.push(vectorFromField(field.children(i)));
        }
        return new StructVector(field.name(), vectors);
    }
}

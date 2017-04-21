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

export class BitArray {
    private view: Uint8Array;
    constructor(buffer: ArrayBuffer, offset: number, length: number) {
        //if (ArrayBuffer.isView(buffer)) {
        //    var og_view = buffer;
        //    buffer = buffer.buffer;
        //    offset = og_view.offset;
        //    length = og_view.length/og_view.BYTES_PER_ELEMENT*8;
        //} else if (buffer instanceof ArrayBuffer) {
        var offset = offset || 0;
        var length = length;// || buffer.length*8;
        //} else if (buffer instanceof Number) {
        //    length = buffer;
        //    buffer = new ArrayBuffer(Math.ceil(length/8));
        //    offset = 0;
        //}

        this.view = new Uint8Array(buffer, offset, Math.ceil(length/8));
    }

    get(i) {
        var index = (i >> 3) | 0; // | 0 converts to an int. Math.floor works too.
        var bit = i % 8;  // i % 8 is just as fast as i & 7
        return (this.view[index] & (1 << bit)) !== 0;
    }

    set(i) {
        var index = (i >> 3) | 0;
        var bit = i % 8;
        this.view[index] |= 1 << bit;
    }

    unset(i) {
        var index = (i >> 3) | 0;
        var bit = i % 8;
        this.view[index] &= ~(1 << bit);
    }
}

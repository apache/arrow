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

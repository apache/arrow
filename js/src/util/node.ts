
export class PipeIterator<T> implements IterableIterator<T> {
    constructor(protected iterator: IterableIterator<T>, protected encoding?: any) {}
    [Symbol.iterator]() { return this.iterator; }
    next(value?: any) { return this.iterator.next(value); }
    throw(error?: any) {
        if (typeof this.iterator.throw === 'function') {
            return this.iterator.throw(error);
        }
        return { done: true, value: null as any };
    }
    return(value?: any) {
        if (typeof this.iterator.return === 'function') {
            return this.iterator.return(value);
        }
        return { done: true, value: null as any };
    }
    pipe(stream: NodeJS.WritableStream) {
        let { encoding } = this;
        let res: IteratorResult<T>;
        let write = (err?: any) => {
            stream['removeListener']('error', write);
            stream['removeListener']('drain', write);
            if (err) return this.throw(err);
            if (stream['writable']) {
                do {
                    if ((res = this.next()).done) break;
                } while (emit(stream, encoding, res.value));
            }
            return wait(stream, res && res.done, write);
        };
        write();
        return stream;
    }
}

export class AsyncPipeIterator<T> implements AsyncIterableIterator<T> {
    constructor(protected iterator: AsyncIterableIterator<T>, protected encoding?: any) {}
    [Symbol.asyncIterator]() { return this.iterator; }
    next(value?: any) { return this.iterator.next(value); }
    async throw(error?: any) {
        if (typeof this.iterator.throw === 'function') {
            return this.iterator.throw(error);
        }
        return { done: true, value: null as any };
    }
    async return(value?: any) {
        if (typeof this.iterator.return === 'function') {
            return this.iterator.return(value);
        }
        return { done: true, value: null as any };
    }
    pipe(stream: NodeJS.WritableStream) {
        let { encoding } = this;
        let res: IteratorResult<T>;
        let write = async (err?: any) => {
            stream['removeListener']('error', write);
            stream['removeListener']('drain', write);
            if (err) return this.throw(err);
            if (stream['writable']) {
                do {
                    if ((res = await this.next()).done) break;
                } while (emit(stream, encoding, res.value));
            }
            return wait(stream, res && res.done, write);
        };
        write();
        return stream;
    }
}

const toBufferOrUint8Array = (() => {
    // If in node, convert Uint8Arrays to Buffer instances. This is necessary
    // because some node APIs ('http' etc.) don't work unless you give them Buffers.
    // This eval also defeats closure-compiler, which doesn't recognize the Buffer constructor.
    const BufferCtor = eval('typeof Buffer !== "undefined" ? Buffer : null');
    return !BufferCtor ? (arr: Uint8Array) => arr :
        (arr: Uint8Array) => BufferCtor.from(arr.buffer, arr.byteOffset, arr.byteLength);
})();

function emit(stream: NodeJS.WritableStream, encoding: string, value: any) {
    return stream['write']((encoding === 'utf8' ? value + '\n' : toBufferOrUint8Array(value)) as any, encoding);
}

function wait(stream: NodeJS.WritableStream, done: boolean, write: (x?: any) => void) {
    const p = eval('process'); // defeat closure compiler
    if (!done) {
        stream['once']('error', write);
        stream['once']('drain', write);
    } else if (!(!p || stream === p.stdout) && !(stream as any)['isTTY']) {
        stream['end'](<any> null);
    }
}

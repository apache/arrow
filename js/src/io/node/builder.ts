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

import { DataType } from '../../type';
import { Duplex, DuplexOptions } from 'stream';
import { Builder, BuilderOptions } from '../../builder/index';

/** @ignore */
export function builderThroughNodeStream<T extends DataType = any, TNull = any>(
    options: DuplexOptions & BuilderOptions<T, TNull>
) {
    return new BuilderDuplex(Builder.new(options), options);
}

/** @ignore */
type CB = (error?: Error | null | undefined) => void;

class BuilderDuplex<T extends DataType = any, TNull = any> extends Duplex {
    private _builder: Builder<T, TNull> | null;
    constructor(builder: Builder<T, TNull>, options?: DuplexOptions) {
        super({ allowHalfOpen: true, ...options, writableObjectMode: true, readableObjectMode: true });
        this._builder = builder;
    }
    _final(cb?: CB) {
        const builder = this._builder;
        if (builder) { flush(builder.finish(), this); }
        cb && cb();
    }
    _write(x: any, _: string, cb: CB) {
        const builder = this._builder;
        if (builder) { flush(builder.write(x), this); }
        cb && cb();
        return true;
    }
    _read(size: number) {
        const builder = this._builder;
        if (builder) { flush(builder, this, size); }
    }
    _destroy(_err: Error | null, cb: (error: Error | null) => void) {
        const builder = this._builder;
        if (builder) { builder.reset(); }
        cb(this._builder = null);
    }
}

function flush<T extends DataType = any, TNull = any>(builder: Builder<T, TNull>, sink: BuilderDuplex<T, TNull>, size = sink.readableHighWaterMark) {
    if (size === null || builder.length >= size) {
        sink.push(builder.flush());
    }
    if (builder.finished) {
        if (builder.length > 0) {
            sink.push(builder.flush());
        }
        sink.push(null);
    }
}

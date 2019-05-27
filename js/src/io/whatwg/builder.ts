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

import { Data } from '../../data';
import { DataType } from '../../type';
import { Builder, BuilderOptions } from '../../builder/index';

/** @ignore */
export interface BuilderTransformOptions<T extends DataType = any, TNull = any> extends BuilderOptions<T, TNull> {
    queueingStrategy?: 'bytes' | 'count';
    dictionaryHashFunction?: (value: any) => string | number;
    readableStrategy?: { highWaterMark?: number, size?: any, type?: 'bytes'; }
    writableStrategy?: { highWaterMark?: number, size?: any, type?: 'bytes'; }
    valueToChildTypeId?: (builder: Builder<T, TNull>, value: any, offset: number) => number;
}

/** @ignore */
export function builderThroughDOMStream<T extends DataType = any, TNull = any>(options: BuilderTransformOptions<T, TNull>) {
    return new BuilderTransform(options);
}

/** @ignore */
class BuilderTransform<T extends DataType = any, TNull = any> {

    public readable: ReadableStream<Data<T>>;
    public writable: WritableStream<T['TValue'] | TNull>;
    public _controller: ReadableStreamDefaultController<Data<T>> | null;

    private _finished = false;
    private _bufferedSize = 0;
    private _builder: Builder<T, TNull>;
    private _getSize: (builder: Builder<T, TNull>) => number;

    constructor(options: BuilderTransformOptions<T, TNull>) {

        // Access properties by string indexers to defeat closure compiler

        const {
            ['readableStrategy']: readableStrategy,
            ['writableStrategy']: writableStrategy,
            ['queueingStrategy']: queueingStrategy = 'count',
            ...builderOptions
        } = options;

        this._controller = null;
        this._builder = Builder.new<T, TNull>(builderOptions);
        this._getSize = queueingStrategy !== 'bytes' ? builderLength : builderByteLength;

        const { ['highWaterMark']: readableHighWaterMark = queueingStrategy === 'bytes' ? 2 ** 14 : 1000 } = { ...readableStrategy };
        const { ['highWaterMark']: writableHighWaterMark = queueingStrategy === 'bytes' ? 2 ** 14 : 1000 } = { ...writableStrategy };

        this['readable'] = new ReadableStream<Data<T>>({
            ['cancel']: ()  => { this._builder.reset(); },
            ['pull']: (c) => { this._maybeFlush(this._builder, this._controller = c); },
            ['start']: (c) => { this._maybeFlush(this._builder, this._controller = c); },
        }, {
            'highWaterMark': readableHighWaterMark,
            'size': queueingStrategy === 'bytes' ? dataByteLength : dataLength,
        });

        this['writable'] = new WritableStream({
            ['abort']: () => { this._builder.reset(); },
            ['write']: () => { this._maybeFlush(this._builder, this._controller); },
            ['close']: () => { this._maybeFlush(this._builder.finish(), this._controller); },
        }, {
            'highWaterMark': writableHighWaterMark,
            'size': (value: T['TValue'] | TNull) => this._writeValueAndReturnChunkSize(value),
        });

        if (DataType.isDictionary(builderOptions.type)) {
            let chunks: any[] = [];
            this._enqueue = (controller: ReadableStreamDefaultController<Data<T>>, chunk: Data<T> | null) => {
                this._bufferedSize = 0;
                if (chunk !== null) {
                    chunks.push(chunk);
                } else {
                    const chunks_ = chunks;
                    chunks = [];
                    chunks_.forEach((x) => controller.enqueue(x));
                    controller.close();
                    this._controller = null;
                }
            };
        }
    }

    private _writeValueAndReturnChunkSize(x: T['TValue'] | TNull) {
        const builder = this._builder.write(x);
        const bufferedSize = this._bufferedSize;
        this._bufferedSize = this._getSize(builder);
        return this._bufferedSize - bufferedSize;
    }

    private _maybeFlush(builder: Builder<T, TNull>, controller: ReadableStreamDefaultController<Data<T>> | null) {
        if (controller === null) { return; }
        if (this._bufferedSize >= controller.desiredSize!) {
            this._enqueue(controller, builder.flush());
        }
        if (builder.finished) {
            if (builder.length > 0) {
                this._enqueue(controller, builder.flush());
            }
            if (!this._finished && (this._finished = true)) {
                this._enqueue(controller, null);
            }
        }
    }

    private _enqueue(controller: ReadableStreamDefaultController<Data<T>>, chunk: Data<T> | null) {
        this._bufferedSize = 0;
        this._controller = null;
        chunk === null ? controller.close() : controller.enqueue(chunk);
    }
}

/** @ignore */ const dataLength = <T extends DataType = any>(data: Data<T>) => data.length;
/** @ignore */ const dataByteLength = <T extends DataType = any>(data: Data<T>) => data.byteLength;
/** @ignore */ const builderLength = <T extends DataType = any>(builder: Builder<T>) => builder.length;
/** @ignore */ const builderByteLength = <T extends DataType = any>(builder: Builder<T>) => builder.bytesUsed;

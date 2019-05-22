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
export function builderThroughDOMStream<T extends DataType = any, TNull = any>(
    writableStrategy: QueuingStrategy<T['TValue'] | TNull> & BuilderOptions<T, TNull>,
    readableStrategy?: { highWaterMark?: number, size?: any }
) {

    const builder = Builder.new<T, TNull>(writableStrategy);
    let controller_: ReadableStreamDefaultController<Data<T>> | null = null;

    // Access these properties by string indexers to defeat closure compiler
    const { ['highWaterMark']: highWaterMark, ['size']: size } = writableStrategy;

    return {
        readable: new ReadableStream<Data<T>>({
            ['cancel']() { builder.reset(); },
            ['start'](c) { flush(builder, controller_ = c); },
            ['pull'](c) { flush(builder, controller_ = c); },
        }, readableStrategy),
        writable: new WritableStream({
            ['abort']() { builder.reset(); },
            ['close']() { flush(builder.finish(), controller_); },
            ['write'](value: T['TValue'] | TNull) {
                flush(builder.write(value), controller_);
            },
        }, { 'highWaterMark': highWaterMark, 'size': size })
    };

    function flush(builder: Builder<T, TNull>, controller: ReadableStreamDefaultController<Data<T>> | null) {
        if (controller === null) { return; }
        const size = controller.desiredSize;
        if (size === null || builder.length >= size) {
            controller_ = null;
            controller.enqueue(builder.flush());
        }
        if (builder.finished) {
            controller_ = null;
            if (builder.length > 0) {
                controller.enqueue(builder.flush());
            } else {
                controller.close();
            }
        }
    }
}

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

    const readable = new ReadableStream<Data<T>>({
        cancel() { builder.reset(); },
        pull(controller) {
            const size = controller.desiredSize;
            if (size === null || builder.length >= size) {
                controller.enqueue(builder.flush());
            }
            if (builder.finished) {
                if (builder.length > 0) {
                    controller.enqueue(builder.flush());
                }
                controller.close();
            }
        },
    }, readableStrategy);

    return {
        readable,
        writable: new WritableStream({
            abort() { builder.reset(); },
            close() { builder.finish(); },
            write(value: T['TValue'] | TNull) {
                builder.write(value);
            },
        }, writableStrategy)
    };
}

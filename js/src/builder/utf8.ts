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

import { Utf8 } from '../type';
import { encodeUtf8 } from '../util/utf8';
import { BinaryBufferBuilder } from './buffer';
import { VariableWidthBuilder, BuilderOptions } from './base';
import { BinaryBuilder } from './binary';

export class Utf8Builder<TNull = any> extends VariableWidthBuilder<Utf8, TNull> {
    constructor(opts: BuilderOptions<Utf8, TNull>) {
        super(opts);
        this._values = new BinaryBufferBuilder();
    }
    public setValue(index: number, value: string) {
        return super.setValue(index, encodeUtf8(value) as any);
    }
    // @ts-ignore
    protected _flushPending(pending: Map<number, Uint8Array | undefined>, pendingLength: number): void {}
}

(Utf8Builder.prototype as any)._flushPending = (BinaryBuilder.prototype as any)._flushPending;

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

import { CompressionType } from '../fb/compression-type.js';

export interface Codec {
    encode?(data: Uint8Array): Uint8Array;
    decode?(data: Uint8Array): Uint8Array;
}

class _CompressionRegistry {
    protected declare registry: { [key in CompressionType]?: Codec };

    constructor() {
        this.registry = {};
    }

    set(compression: CompressionType, codec: Codec) {
        this.registry[compression] = codec;
    }

    get(compression?: CompressionType): Codec | null {
        if (compression !== undefined) {
            return this.registry?.[compression] || null;
        }
        return null;
    }
}

export const compressionRegistry = new _CompressionRegistry();

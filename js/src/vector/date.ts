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

import { Chunked } from './chunked';
import { BaseVector } from './base';
import { VectorType as V } from '../interfaces';
import { VectorBuilderOptions } from './index';
import { vectorFromValuesWithType } from './index';
import { VectorBuilderOptionsAsync } from './index';
import { Date_, DateDay, DateMillisecond  } from '../type';

/** @ignore */
export class DateVector<T extends Date_ = Date_> extends BaseVector<T> {
    public static from<T extends Date_ = DateMillisecond, TNull = any>(input: Iterable<Date | TNull>): V<T>;
    public static from<T extends Date_ = DateMillisecond, TNull = any>(input: AsyncIterable<Date | TNull>): Promise<V<T>>;
    public static from<T extends Date_ = DateMillisecond, TNull = any>(input: VectorBuilderOptions<T, TNull>): Chunked<T>;
    public static from<T extends Date_ = DateMillisecond, TNull = any>(input: VectorBuilderOptionsAsync<T, TNull>): Promise<Chunked<T>>;
    /** @nocollapse */
    public static from<T extends Date_ = DateMillisecond, TNull = any>(input: Iterable<Date | TNull> | AsyncIterable<Date | TNull> | VectorBuilderOptions<T, TNull> | VectorBuilderOptionsAsync<T, TNull>) {
        return vectorFromValuesWithType(() => new DateMillisecond() as T, input);
    }
}

/** @ignore */
export class DateDayVector extends DateVector<DateDay> {
    public static from<TNull = any>(input: Iterable<Date | TNull>): DateDayVector;
    public static from<TNull = any>(input: AsyncIterable<Date | TNull>): Promise<DateDayVector>;
    public static from<TNull = any>(input: VectorBuilderOptions<DateDay, TNull>): Chunked<DateDay>;
    public static from<TNull = any>(input: VectorBuilderOptionsAsync<DateDay, TNull>): Promise<Chunked<DateDay>>;
    /** @nocollapse */
    public static from<TNull = any>(input: Iterable<Date | TNull> | AsyncIterable<Date | TNull> | VectorBuilderOptions<DateDay, TNull> | VectorBuilderOptionsAsync<DateDay, TNull>) {
        return vectorFromValuesWithType(() => new DateDay(), input);
    }
}

/** @ignore */
export class DateMillisecondVector extends DateVector<DateMillisecond> {
    public static from<TNull = any>(input: Iterable<Date | TNull>): DateMillisecondVector;
    public static from<TNull = any>(input: AsyncIterable<Date | TNull>): Promise<DateMillisecondVector>;
    public static from<TNull = any>(input: VectorBuilderOptions<DateMillisecond, TNull>): Chunked<DateMillisecond>;
    public static from<TNull = any>(input: VectorBuilderOptionsAsync<DateMillisecond, TNull>): Promise<Chunked<DateMillisecond>>;
    /** @nocollapse */
    public static from<TNull = any>(input: Iterable<Date | TNull> | AsyncIterable<Date | TNull> | VectorBuilderOptions<DateMillisecond, TNull> | VectorBuilderOptionsAsync<DateMillisecond, TNull>) {
        return vectorFromValuesWithType(() => new DateMillisecond(), input);
    }
}

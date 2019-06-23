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

import { DateUnit } from '../enum';
import { Chunked } from './chunked';
import { BaseVector } from './base';
import { VectorType as V } from '../interfaces';
import { VectorBuilderOptions } from './index';
import { vectorFromValuesWithType } from './index';
import { VectorBuilderOptionsAsync } from './index';
import { Date_, DateDay, DateMillisecond  } from '../type';

/** @ignore */
type FromArgs<T extends Date_> = [Iterable<Date>, T['unit']];

/** @ignore */
export class DateVector<T extends Date_ = Date_> extends BaseVector<T> {
    public static from<T extends DateUnit.DAY>(...args: FromArgs<DateDay>): V<DateDay>;
    public static from<T extends DateUnit.MILLISECOND>(...args: FromArgs<DateMillisecond>): V<DateMillisecond>;
    public static from<T extends Date_, TNull = any>(input: Iterable<Date | TNull>): V<T>;
    public static from<T extends Date_, TNull = any>(input: AsyncIterable<Date | TNull>): Promise<V<T>>;
    public static from<T extends Date_, TNull = any>(input: VectorBuilderOptions<T, TNull>): Chunked<T>;
    public static from<T extends Date_, TNull = any>(input: VectorBuilderOptionsAsync<T, TNull>): Promise<Chunked<T>>;
    /** @nocollapse */
    public static from<T extends Date_, TNull = any>(...args: FromArgs<T> | [Iterable<Date | TNull> | AsyncIterable<Date | TNull> | VectorBuilderOptions<T, TNull> | VectorBuilderOptionsAsync<T, TNull>]) {
        if (args.length === 2) {
            return vectorFromValuesWithType(() => args[1] === DateUnit.DAY ? new DateDay() : new DateMillisecond() as T, args[0]);
        }
        return vectorFromValuesWithType(() => new DateMillisecond() as T, args[0]);
    }
}

/** @ignore */
export class DateDayVector extends DateVector<DateDay> {}

/** @ignore */
export class DateMillisecondVector extends DateVector<DateMillisecond> {}

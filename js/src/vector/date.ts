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

import { Data } from '../data';
import { Vector } from '../vector';
import { DateUnit } from '../enum';
import { BaseVector } from './base';
import * as IntUtil from '../util/int';
import { Date_, DateDay, DateMillisecond  } from '../type';

export class DateVector<T extends Date_ = Date_> extends BaseVector<T> {
    /** @nocollapse */
    public static from<T extends Date_ = DateMillisecond>(data: Date[], unit: T['unit'] = DateUnit.MILLISECOND) {
        switch (unit) {
            case DateUnit.DAY: {
                const values = Int32Array.from(data.map((d) => d.valueOf() / 86400000));
                return Vector.new(Data.Date(new DateDay(), 0, data.length, 0, null, values));
            }
            case DateUnit.MILLISECOND: {
                const values = IntUtil.Int64.convertArray(data.map((d) => d.valueOf()));
                return Vector.new(Data.Date(new DateMillisecond(), 0, data.length, 0, null, values));
            }
        }
        throw new TypeError(`Unrecognized date unit "${DateUnit[unit]}"`);
    }
}

export class DateDayVector extends DateVector<DateDay> {}
export class DateMillisecondVector extends DateVector<DateMillisecond> {}

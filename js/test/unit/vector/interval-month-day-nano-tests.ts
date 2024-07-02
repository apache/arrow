import { IntervalMonthDayNano, Vector, makeData } from "apache-arrow";

type IntervalValue = {
    months?: number;
    days?: number;
    nanoseconds?: bigint
};

function formatIntervalValue(value: IntervalValue): IntervalValue {
    return { months: 0, days: 0, nanoseconds: BigInt(0), ...value };
}

function convertIntervalValueToIntArray(value: IntervalValue): Int32Array {
    const int64s = new BigInt64Array(2);
    int64s[0] = BigInt(value.months ?? 0) + BigInt(value.days ?? 0) * (BigInt(1) << BigInt(32));
    if (value.nanoseconds) {
        int64s[1] = value.nanoseconds;
    } else {
        int64s[1] = BigInt(0);
    }
    return new Int32Array(int64s.buffer);
}

function convertIntArrayToIntervalValue(value: Int32Array | null): IntervalValue | null {
    if (!value) return null;
    const intervalValue: IntervalValue = {};
    intervalValue.months = value[0];
    const negative = value[1] & (1 << 31)
    intervalValue.days = value[1] + (negative ? 1 : 0);
    const secondWords = new BigInt64Array(value.buffer);
    intervalValue.nanoseconds = secondWords[1];
    return intervalValue;
}

function makeIntervalMonthDayNanoVector(intervalArray: IntervalValue[]) {
    const type = new IntervalMonthDayNano();
    const length = intervalArray.length;
    const data = new Int32Array(length * 4);
    for (let i = 0; i < intervalArray.length; i++) {
        const intValue = convertIntervalValueToIntArray(intervalArray[i]);
        for (let j = 0; j < intValue.length; j++) {
            data[i * 4 + j] = intValue[j];
        }
    }
    const vec = new Vector([makeData({ type, length, data })]);
    return vec;
}

describe(`MonthDayNanoInteralVector`, () => {
    test(`Intervals with months are stored in IntervalMonthDayNano`, () => {
        const value: IntervalValue = formatIntervalValue({ months: 5 });
        const vec = makeIntervalMonthDayNanoVector([value]);
        expect(vec.type).toBeInstanceOf(IntervalMonthDayNano);
        expect(convertIntArrayToIntervalValue(vec.get(0))).toStrictEqual(value);
    });

    test(`Intervals with days are stored in IntervalMonthDayNano`, () => {
        const value: IntervalValue = formatIntervalValue({ days: 1000 });
        const vec = makeIntervalMonthDayNanoVector([value]);
        expect(vec.type).toBeInstanceOf(IntervalMonthDayNano);
        expect(convertIntArrayToIntervalValue(vec.get(0))).toStrictEqual(value);
    });

    test(`Intervals with nanoseconds are stored in IntervalMonthDayNano`, () => {
        const value: IntervalValue = formatIntervalValue({ nanoseconds: 100000000000000000n });
        const vec = makeIntervalMonthDayNanoVector([value]);
        expect(vec.type).toBeInstanceOf(IntervalMonthDayNano);
        expect(convertIntArrayToIntervalValue(vec.get(0))).toStrictEqual(value);
    });

    test(`Intervals with months, days, nanoseconds are stored in IntervalMonthDayNano`, () => {
        const value: IntervalValue = formatIntervalValue({ months: 1000, days: 10000, nanoseconds: 100000000000000000n });
        const vec = makeIntervalMonthDayNanoVector([value]);
        expect(vec.type).toBeInstanceOf(IntervalMonthDayNano);
        expect(convertIntArrayToIntervalValue(vec.get(0))).toStrictEqual(value);
    });

    test(`Negative Intervals with months, days, nanoseconds are stored in IntervalMonthDayNano`, () => {
        const value: IntervalValue = formatIntervalValue({ months: -1000, days: -10000, nanoseconds: -100000000000000000n });
        const vec = makeIntervalMonthDayNanoVector([value]);
        expect(vec.type).toBeInstanceOf(IntervalMonthDayNano);
        expect(convertIntArrayToIntervalValue(vec.get(0))).toStrictEqual(value);
    });
});
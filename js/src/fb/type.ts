// automatically generated by the FlatBuffers compiler, do not modify

import { Binary } from './binary.js';
import { Bool } from './bool.js';
import { Date } from './date.js';
import { Decimal } from './decimal.js';
import { Duration } from './duration.js';
import { FixedSizeBinary } from './fixed-size-binary.js';
import { FixedSizeList } from './fixed-size-list.js';
import { FloatingPoint } from './floating-point.js';
import { Int } from './int.js';
import { Interval } from './interval.js';
import { LargeBinary } from './large-binary.js';
import { LargeList } from './large-list.js';
import { LargeUtf8 } from './large-utf8.js';
import { List } from './list.js';
import { Map } from './map.js';
import { Null } from './null.js';
import { Struct_ } from './struct_.js';
import { Time } from './time.js';
import { Timestamp } from './timestamp.js';
import { Union } from './union.js';
import { Utf8 } from './utf8.js';


/**
 * ----------------------------------------------------------------------
 * Top-level Type value, enabling extensible type-specific metadata. We can
 * add new logical types to Type without breaking backwards compatibility
 */
export enum Type{
  NONE = 0,
  Null = 1,
  Int = 2,
  FloatingPoint = 3,
  Binary = 4,
  Utf8 = 5,
  Bool = 6,
  Decimal = 7,
  Date = 8,
  Time = 9,
  Timestamp = 10,
  Interval = 11,
  List = 12,
  Struct_ = 13,
  Union = 14,
  FixedSizeBinary = 15,
  FixedSizeList = 16,
  Map = 17,
  Duration = 18,
  LargeBinary = 19,
  LargeUtf8 = 20,
  LargeList = 21
}

export function unionToType(
  type: Type,
  accessor: (obj:Binary|Bool|Date|Decimal|Duration|FixedSizeBinary|FixedSizeList|FloatingPoint|Int|Interval|LargeBinary|LargeList|LargeUtf8|List|Map|Null|Struct_|Time|Timestamp|Union|Utf8) => Binary|Bool|Date|Decimal|Duration|FixedSizeBinary|FixedSizeList|FloatingPoint|Int|Interval|LargeBinary|LargeList|LargeUtf8|List|Map|Null|Struct_|Time|Timestamp|Union|Utf8|null
): Binary|Bool|Date|Decimal|Duration|FixedSizeBinary|FixedSizeList|FloatingPoint|Int|Interval|LargeBinary|LargeList|LargeUtf8|List|Map|Null|Struct_|Time|Timestamp|Union|Utf8|null {
  switch(Type[type]) {
    case 'NONE': return null;
    case 'Null': return accessor(new Null())! as Null;
    case 'Int': return accessor(new Int())! as Int;
    case 'FloatingPoint': return accessor(new FloatingPoint())! as FloatingPoint;
    case 'Binary': return accessor(new Binary())! as Binary;
    case 'Utf8': return accessor(new Utf8())! as Utf8;
    case 'Bool': return accessor(new Bool())! as Bool;
    case 'Decimal': return accessor(new Decimal())! as Decimal;
    case 'Date': return accessor(new Date())! as Date;
    case 'Time': return accessor(new Time())! as Time;
    case 'Timestamp': return accessor(new Timestamp())! as Timestamp;
    case 'Interval': return accessor(new Interval())! as Interval;
    case 'List': return accessor(new List())! as List;
    case 'Struct_': return accessor(new Struct_())! as Struct_;
    case 'Union': return accessor(new Union())! as Union;
    case 'FixedSizeBinary': return accessor(new FixedSizeBinary())! as FixedSizeBinary;
    case 'FixedSizeList': return accessor(new FixedSizeList())! as FixedSizeList;
    case 'Map': return accessor(new Map())! as Map;
    case 'Duration': return accessor(new Duration())! as Duration;
    case 'LargeBinary': return accessor(new LargeBinary())! as LargeBinary;
    case 'LargeUtf8': return accessor(new LargeUtf8())! as LargeUtf8;
    case 'LargeList': return accessor(new LargeList())! as LargeList;
    default: return null;
  }
}

export function unionListToType(
  type: Type,
  accessor: (index: number, obj:Binary|Bool|Date|Decimal|Duration|FixedSizeBinary|FixedSizeList|FloatingPoint|Int|Interval|LargeBinary|LargeList|LargeUtf8|List|Map|Null|Struct_|Time|Timestamp|Union|Utf8) => Binary|Bool|Date|Decimal|Duration|FixedSizeBinary|FixedSizeList|FloatingPoint|Int|Interval|LargeBinary|LargeList|LargeUtf8|List|Map|Null|Struct_|Time|Timestamp|Union|Utf8|null,
  index: number
): Binary|Bool|Date|Decimal|Duration|FixedSizeBinary|FixedSizeList|FloatingPoint|Int|Interval|LargeBinary|LargeList|LargeUtf8|List|Map|Null|Struct_|Time|Timestamp|Union|Utf8|null {
  switch(Type[type]) {
    case 'NONE': return null;
    case 'Null': return accessor(index, new Null())! as Null;
    case 'Int': return accessor(index, new Int())! as Int;
    case 'FloatingPoint': return accessor(index, new FloatingPoint())! as FloatingPoint;
    case 'Binary': return accessor(index, new Binary())! as Binary;
    case 'Utf8': return accessor(index, new Utf8())! as Utf8;
    case 'Bool': return accessor(index, new Bool())! as Bool;
    case 'Decimal': return accessor(index, new Decimal())! as Decimal;
    case 'Date': return accessor(index, new Date())! as Date;
    case 'Time': return accessor(index, new Time())! as Time;
    case 'Timestamp': return accessor(index, new Timestamp())! as Timestamp;
    case 'Interval': return accessor(index, new Interval())! as Interval;
    case 'List': return accessor(index, new List())! as List;
    case 'Struct_': return accessor(index, new Struct_())! as Struct_;
    case 'Union': return accessor(index, new Union())! as Union;
    case 'FixedSizeBinary': return accessor(index, new FixedSizeBinary())! as FixedSizeBinary;
    case 'FixedSizeList': return accessor(index, new FixedSizeList())! as FixedSizeList;
    case 'Map': return accessor(index, new Map())! as Map;
    case 'Duration': return accessor(index, new Duration())! as Duration;
    case 'LargeBinary': return accessor(index, new LargeBinary())! as LargeBinary;
    case 'LargeUtf8': return accessor(index, new LargeUtf8())! as LargeUtf8;
    case 'LargeList': return accessor(index, new LargeList())! as LargeList;
    default: return null;
  }
}


﻿using System;
using System.Collections.Generic;
using Apache.Arrow.Reflection;
using Apache.Arrow.Types;

namespace Apache.Arrow.Builder
{
    public abstract class ProxyArrayBuilder<TPrimitive, TLogical> : FixedBinaryArrayBuilder<TPrimitive>
        where TPrimitive : struct
        where TLogical : struct
    {
        private readonly Func<TLogical, TPrimitive> ToPrimitive;

        public ProxyArrayBuilder(Func<TLogical, TPrimitive> convert, int capacity = 32)
            : this(TypeReflection<TLogical>.ArrowType as FixedWidthType, convert, capacity)
        {
        }

        public ProxyArrayBuilder(FixedWidthType dataType, Func<TLogical, TPrimitive> convert, int capacity = 32)
            : base(dataType, capacity)
        {
            ToPrimitive = convert;
        }

        public Status AppendValue(TLogical value) => AppendValue(ToPrimitive(value));

        public Status AppendValue(TLogical? value)
            => value.HasValue ? AppendValue(ToPrimitive(value.Value)) : AppendNull();

        public Status AppendValues(ReadOnlySpan<TLogical> values)
        {
            Span<TPrimitive> buffer = new TPrimitive[values.Length];

            for (int i = 0; i < values.Length; i++){
                buffer[i] = ToPrimitive(values[i]);
            }
            return AppendValues(buffer);
        }

        public Status AppendValues(ICollection<TLogical?> values)
        {
            int length = values.Count;
            Span<bool> validity = new bool[length];
            Span<TPrimitive> destination = new TPrimitive[length];
            int i = 0;

            // Transform the source ReadOnlySpan<T?> into the destination ReadOnlySpan<T>, filling any null values with default(T)
            foreach (TLogical? value in values)
            {
                if (value.HasValue)
                {
                    destination[i] = ToPrimitive(value.Value);
                    validity[i] = true;
                }
                else
                {
                    destination[i] = default;
                    // default is already false
                    // validity[i] = false;
                }
                i++;
            }

            return AppendValues(destination, validity);
        }
    }

    // Timestamp / DateTime
    public class TimestampArrayBuilder : ProxyArrayBuilder<long, DateTimeOffset>
    {
        private static Func<DateTimeOffset, long> ToPrimitive(TimeUnit unit)
        {
            return unit switch
            {
                TimeUnit.Second => DateTimeOffsetExtensions.ToUnixTimeSeconds,
                TimeUnit.Millisecond => DateTimeOffsetExtensions.ToUnixTimeMilliseconds,
                TimeUnit.Microsecond => DateTimeOffsetExtensions.ToUnixTimeMicroseconds,
                TimeUnit.Nanosecond => DateTimeOffsetExtensions.ToUnixTimeNanoseconds,
                _ => throw new ArgumentException($"Unknwown arrow TimeUnit {unit}"),
            };
        }

        public TimestampArrayBuilder(int capacity = 32) : this(TimestampType.SystemDefault, capacity)
        {
        }

        public TimestampArrayBuilder(TimestampType dtype, int capacity = 32) : base(dtype, ToPrimitive(dtype.Unit), capacity)
        {
        }
    }

    // Time
    public class Time32ArrayBuilder : ProxyArrayBuilder<int, TimeSpan>
    {
        private static Func<TimeSpan, int> ToPrimitive(TimeUnit unit)
        {
            return unit switch
            {
                TimeUnit.Second => TimeSpanExtensions.TotalSeconds,
                TimeUnit.Millisecond => TimeSpanExtensions.TotalMilliseconds,
                _ => throw new ArgumentException($"Unknwown arrow TimeUnit {unit}"),
            };
        }

        public Time32ArrayBuilder(int capacity = 32) : this(Time32Type.Default, capacity)
        {
        }

        public Time32ArrayBuilder(TimeType dtype, int capacity = 32) : base(dtype, ToPrimitive(dtype.Unit), capacity)
        {
        }
    }

    public class Time64ArrayBuilder : ProxyArrayBuilder<long, TimeSpan>
    {
        private static Func<TimeSpan, long> ToPrimitive(TimeUnit unit)
        {
            return unit switch
            {
                TimeUnit.Microsecond => TimeSpanExtensions.TotalMicroseconds,
                TimeUnit.Nanosecond => TimeSpanExtensions.TotalNanoseconds,
                _ => throw new ArgumentException($"Unknwown arrow TimeUnit {unit}"),
            };
        }

        public Time64ArrayBuilder(int capacity = 32) : this(TimeType.SystemDefault, capacity)
        {
        }

        public Time64ArrayBuilder(TimeType dtype, int capacity = 32) : base(dtype, ToPrimitive(dtype.Unit), capacity)
        {
        }
    }

    // Date
    public class Date32ArrayBuilder : ProxyArrayBuilder<int, DateTimeOffset>
    {
        public Date32ArrayBuilder(DateType dtype, int capacity = 32) : base(dtype, DateTimeOffsetExtensions.ToUnixDays, capacity)
        {
        }
    }

    public class Date64ArrayBuilder : ProxyArrayBuilder<long, DateTimeOffset>
    {
        public Date64ArrayBuilder(DateType dtype, int capacity = 32) : base(dtype, DateTimeOffsetExtensions.ToUnixTimeMilliseconds, capacity)
        {
        }
    }
}

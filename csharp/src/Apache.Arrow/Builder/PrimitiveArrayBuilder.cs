using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Arrow.Types;
using Apache.Arrow.Values;

namespace Apache.Arrow.Builder
{
    public class PrimitiveArrayBuilder<T>
        : ArrayBuilder where T : struct
    {
        public IPrimitiveBufferBuilder<T> ValuesBuffer { get; }

        public PrimitiveArrayBuilder(int capacity = 64)
            : this(new Field.Builder().DataType(typeof(T))._type, capacity)
        {
        }

        public PrimitiveArrayBuilder(IArrowType dataType, int capacity = 64)
            : this(dataType , new ValueBufferBuilder<bool>(capacity), new ValueBufferBuilder<T>(capacity))
        {
        }

        public PrimitiveArrayBuilder(
            IArrowType dataType,
            IPrimitiveBufferBuilder<bool> validity, IPrimitiveBufferBuilder<T> values
            ) : base(dataType, new IValueBufferBuilder[] { validity, values })
        {
            ValuesBuffer = values;
        }

        public override IArrayBuilder AppendNull() => AppendNull(default);
        public virtual PrimitiveArrayBuilder<T> AppendNull(T nullValue = default)
        {
            base.AppendNull();
            ValuesBuffer.AppendValue(nullValue);
            return this;
        }

        public override IArrayBuilder AppendNulls(int count) => AppendNulls(count, new T[count]);
        public virtual PrimitiveArrayBuilder<T> AppendNulls(int count, ReadOnlySpan<T> nullValues)
        {
            base.AppendNulls(count);
            ValuesBuffer.AppendValues(nullValues);
            return this;
        }

        public virtual PrimitiveArrayBuilder<T> AppendValue(T value, bool isValid = true)
        {
            base.AppendValidity(isValid);
            ValuesBuffer.AppendValue(value);
            return this;
        }

        public virtual PrimitiveArrayBuilder<T> AppendValue(T? value)
        {
            base.AppendValidity(value.HasValue);
            ValuesBuffer.AppendValue(value);
            return this;
        }

        public virtual PrimitiveArrayBuilder<T> AppendValues(ReadOnlySpan<T> values)
            => AppendValues(values, ValidityMask(values.Length, true));

        public virtual PrimitiveArrayBuilder<T> AppendValues(ICollection<T?> values)
        {
            int length = values.Count;
            Span<bool> validity = new bool[length];
            Span<T> destination = new T[length];
            int i = 0;

            // Transform the source ReadOnlySpan<T?> into the destination ReadOnlySpan<T>, filling any null values with default(T)
            foreach (T? value in values)
            {
                if (value.HasValue)
                {
                    destination[i] = value.Value;
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

        public virtual PrimitiveArrayBuilder<T> AppendValues(ReadOnlySpan<T> values, ReadOnlySpan<bool> mask)
        {
            AppendValidity(mask);
            ValuesBuffer.AppendValues(values);
            return this;
        }

        public override IArrayBuilder AppendValue(Scalar value)
            => value.IsValid ? AppendValue((value as IPrimitiveScalar<T>).Value) : AppendNull();

        // Optimize bulk insert since default iterates over each values
        public override IArrayBuilder AppendValues(ICollection<Scalar> values)
        {
            int length = values.Count;
            T[] buffer = new T[length];
            bool[] validity = new bool[length];
            int i = 0;

            foreach (Scalar obj in values)
            {
                if (obj.IsValid)
                {
                    buffer[i] = (obj as IPrimitiveScalar<T>).Value;
                    validity[i] = true;
                }
                else
                {
                    buffer[i] = default;
                }
                i++;
            }

            return AppendValues(buffer, validity);
        }
    }

    public class VariablePrimitiveArrayBuilder<T>
        : ArrayBuilder where T : struct
    {
        public IPrimitiveBufferBuilder<T> ValuesBuffer { get; }

        // From the docs:
        //
        // The offsets buffer contains length + 1 signed integers (either 32-bit or 64-bit, depending on the
        // logical type), which encode the start position of each slot in the data buffer. The length of the
        // value in each slot is computed using the difference between the offset at that slot’s index and the
        // subsequent offset.
        //
        // In this builder, we choose to append the first offset (zero) upon construction, and each trailing
        // offset is then added after each individual item has been appended.
        public IPrimitiveBufferBuilder<int> OffsetsBuffer { get; }

        private int CurrentOffset
        {
            get
            {
                int length = OffsetsBuffer.ValueLength;
                return length == 0 ? 0 : OffsetsBuffer.Span[length - 1];
            }
        }

        public VariablePrimitiveArrayBuilder(int capacity = 64)
            : this(new Field.Builder().DataType(typeof(T))._type, capacity)
        {
        }

        public VariablePrimitiveArrayBuilder(IArrowType dataType, int capacity = 64)
            : this(dataType, new ValueBufferBuilder<bool>(capacity), new ValueBufferBuilder<int>(capacity), new ValueBufferBuilder<T>(capacity))
        {
        }

        public VariablePrimitiveArrayBuilder(
            IArrowType dataType,
            IPrimitiveBufferBuilder<bool> validity, IPrimitiveBufferBuilder<int> offsets, IPrimitiveBufferBuilder<T> values
            ) : base(dataType, new IValueBufferBuilder[] { validity, offsets, values })
        {
            ValuesBuffer = values;
            OffsetsBuffer = offsets;

            OffsetsBuffer.AppendValue(0);
        }

        public override IArrayBuilder AppendNull() => AppendNull(default);
        public virtual VariablePrimitiveArrayBuilder<T> AppendNull(T nullValue = default)
        {
            base.AppendNull();

            // Append Offset
            OffsetsBuffer.AppendValue(CurrentOffset);

            // Append Values
            ValuesBuffer.AppendValue(nullValue);
            return this;
        }

        public override IArrayBuilder AppendNulls(int count) => AppendNulls(count, new T[count]);
        public new VariablePrimitiveArrayBuilder<T> AppendNulls(int count, ReadOnlySpan<T> nullValues)
        {
            base.AppendNulls(count);

            // Append Offset
            OffsetsBuffer.AppendValues(CurrentOffset, count);

            // Append Values
            ValuesBuffer.AppendValues(nullValues);
            return this;
        }

        public virtual VariablePrimitiveArrayBuilder<T> AppendValue(T value)
        {
            AppendValidity(true);

            // Append Offset
            OffsetsBuffer.AppendValue(CurrentOffset + 1);

            // Append Values
            ValuesBuffer.AppendValue(value);
            return this;
        }

        public virtual VariablePrimitiveArrayBuilder<T> AppendValue(T? value)
            => value.HasValue ? AppendValue(value.Value) : AppendNull();

        public virtual VariablePrimitiveArrayBuilder<T> AppendValue(ReadOnlySpan<T> value)
        {
            AppendValidity(true);

            // Append Offset
            OffsetsBuffer.AppendValue(CurrentOffset + 1);

            // Append Values
            ValuesBuffer.AppendValues(value);
            return this;
        }

        public virtual VariablePrimitiveArrayBuilder<T> AppendValue(ReadOnlySpan<T> value, bool isValid = true)
            => isValid ? AppendValue(value) : AppendNull();

        public virtual VariablePrimitiveArrayBuilder<T> AppendValues(ICollection<T[]> values)
        {
            Span<T> memory = new T[values.Sum(row => row.Length)];
            Span<int> offsets = new int[values.Count];
            Span<bool> mask = new bool[offsets.Length];
            int offset = 0;
            int currentOffset = CurrentOffset;
            int i = 0;

            foreach (T[] value in values)
            {
                if (value == null)
                {
                    offsets[i] = currentOffset;
                    // default is already false
                    // mask[i] = false;
                }
                else
                {
                    // Copy to memory
                    value.CopyTo(memory.Slice(offset, value.Length));

                    offset += value.Length;
                    currentOffset += value.Length;

                    // Fill other buffers
                    offsets[i] = currentOffset;
                    mask[i] = true;
                }
                i++;
            }

            return AppendValues(memory, offsets, mask);
        }

        public virtual VariablePrimitiveArrayBuilder<T> AppendValues(
            ReadOnlySpan<T> values, ReadOnlySpan<int> offsets, ReadOnlySpan<bool> mask
            )
        {
            AppendValidity(mask);

            // Append Offset
            OffsetsBuffer.AppendValues(offsets);

            // Append Values
            ValuesBuffer.AppendValues(values);
            return this;
        }

        public override IArrayBuilder AppendValue(Scalar value)
            => value.IsValid ? AppendValue((value as IPrimitiveScalar<T>).Value) : AppendNull();
        public override IArrayBuilder AppendValue(IEnumerable<Scalar> value)
            => throw new NotImplementedException("");
    }
}

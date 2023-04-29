using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Arrow.Types;

namespace Apache.Arrow.Builder
{
    public class ValueArrayBuilder<T>
        : ArrayBuilder where T : struct
    {
        public IValueBufferBuilder<T> ValuesBuffer { get; }

        public ValueArrayBuilder(int capacity = 64)
            : this(new Field.Builder().DataType(typeof(T))._type, capacity)
        {
        }

        public ValueArrayBuilder(IArrowType dataType, int capacity = 64)
            : this(dataType , new ValueBufferBuilder<bool>(capacity), new ValueBufferBuilder<T>(capacity))
        {
        }

        public ValueArrayBuilder(
            IArrowType dataType,
            IValueBufferBuilder<bool> validity, IValueBufferBuilder<T> values
            ) : base(dataType, new IValueBufferBuilder[] { validity, values })
        {
            ValuesBuffer = values;
        }

        public override IArrayBuilder AppendNull()
        {
            base.AppendNull();
            ValuesBuffer.AppendValue(default);
            return this;
        }

        public override IArrayBuilder AppendNulls(int count)
        {
            base.AppendNulls(count);
            ValuesBuffer.AppendValues(new T[count]);
            return this;
        }

        public virtual ValueArrayBuilder<T> AppendValue(T value, bool isValid = true)
        {
            base.AppendValidity(isValid);
            ValuesBuffer.AppendValue(value);
            return this;
        }

        public virtual ValueArrayBuilder<T> AppendValue(T? value)
        {
            base.AppendValidity(value.HasValue);
            ValuesBuffer.AppendValue(value);
            return this;
        }

        public virtual ValueArrayBuilder<T> AppendValues(ReadOnlySpan<T> values)
            => AppendValues(values, ValidityMask(values.Length, true));

        public virtual ValueArrayBuilder<T> AppendValues(ICollection<T?> values)
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

        public virtual ValueArrayBuilder<T> AppendValues(ReadOnlySpan<T> values, ReadOnlySpan<bool> mask)
        {
            AppendValidity(mask);
            ValuesBuffer.AppendValues(values);
            return this;
        }

        public override IArrayBuilder AppendValue(object value, bool isValid)
            => value == null ? AppendNull() : AppendValue((T)value, true);
    }

    public class VariableValueArrayBuilder<T>
        : ArrayBuilder where T : struct
    {
        public IValueBufferBuilder<T> ValuesBuffer { get; }

        // From the docs:
        //
        // The offsets buffer contains length + 1 signed integers (either 32-bit or 64-bit, depending on the
        // logical type), which encode the start position of each slot in the data buffer. The length of the
        // value in each slot is computed using the difference between the offset at that slot’s index and the
        // subsequent offset.
        //
        // In this builder, we choose to append the first offset (zero) upon construction, and each trailing
        // offset is then added after each individual item has been appended.
        public IValueBufferBuilder<int> OffsetsBuffer { get; }

        private int CurrentOffset
        {
            get
            {
                int length = OffsetsBuffer.ValueLength;
                return length == 0 ? 0 : OffsetsBuffer.Span[length - 1];
            }
        }

        public VariableValueArrayBuilder(int capacity = 64)
            : this(new Field.Builder().DataType(typeof(T))._type, capacity)
        {
        }

        public VariableValueArrayBuilder(IArrowType dataType, int capacity = 64)
            : this(dataType, new ValueBufferBuilder<bool>(capacity), new ValueBufferBuilder<int>(capacity), new ValueBufferBuilder<T>(capacity))
        {
        }

        public VariableValueArrayBuilder(
            IArrowType dataType,
            IValueBufferBuilder<bool> validity, IValueBufferBuilder<int> offsets, IValueBufferBuilder<T> values
            ) : base(dataType, new IValueBufferBuilder[] { validity, offsets, values })
        {
            ValuesBuffer = values;
            OffsetsBuffer = offsets;

            OffsetsBuffer.AppendValue(0);
        }

        public new VariableValueArrayBuilder<T> AppendNull()
        {
            base.AppendNull();

            // Append Offset
            OffsetsBuffer.AppendValue(CurrentOffset);

            // Append Values
            ValuesBuffer.AppendValue(default);
            return this;
        }

        public new VariableValueArrayBuilder<T> AppendNulls(int count)
        {
            base.AppendNulls(count);

            // Append Offset
            OffsetsBuffer.AppendValues(CurrentOffset, count);

            // Append Values
            ValuesBuffer.AppendValues(new T[count]);
            return this;
        }

        public virtual VariableValueArrayBuilder<T> AppendValue(T value)
        {
            AppendValidity(true);

            // Append Offset
            OffsetsBuffer.AppendValue(CurrentOffset + 1);

            // Append Values
            ValuesBuffer.AppendValue(value);
            return this;
        }

        public virtual VariableValueArrayBuilder<T> AppendValue(T? value)
            => value.HasValue ? AppendValue(value.Value) : AppendNull() as VariableValueArrayBuilder<T>;

        public virtual VariableValueArrayBuilder<T> AppendValue(ReadOnlySpan<T> value)
        {
            AppendValidity(true);

            // Append Offset
            OffsetsBuffer.AppendValue(CurrentOffset + 1);

            // Append Values
            ValuesBuffer.AppendValues(value);
            return this;
        }

        public virtual VariableValueArrayBuilder<T> AppendValue(ReadOnlySpan<T> value, bool isValid = true)
            => isValid ? AppendValue(value) : AppendNull() as VariableValueArrayBuilder<T>;

        public virtual VariableValueArrayBuilder<T> AppendValues(ICollection<T[]> values)
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

        public virtual VariableValueArrayBuilder<T> AppendValues(
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

        public override IArrayBuilder AppendValue(object value, bool isValid)
            => AppendValue((T)value);
        public override IArrayBuilder AppendValue(IEnumerable<object> value, bool isValid)
            => AppendValue((IEnumerable<T>)value, isValid);
    }
}

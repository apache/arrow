using System;
using System.Linq;
using System.Runtime.CompilerServices;
using Apache.Arrow.Types;

namespace Apache.Arrow.Builder
{
    public class ValueArrayBuilder<T>
        : BaseArrayBuilder where T : struct
    {
        public IValueBufferBuilder<T> ValuesBuffer { get; }

        public ValueArrayBuilder(IArrowType dataType)
            : this(dataType, new ValueBufferBuilder<bool>(), new ValueBufferBuilder<T>())
        {
        }

        public ValueArrayBuilder(
            IArrowType dataType,
            IValueBufferBuilder<bool> validity, IValueBufferBuilder<T> values
            ) : base(dataType, validity, new IValueBufferBuilder[] { validity, values })
        {
            ValuesBuffer = values;
        }

        public override IArrayBuilder AppendNull()
        {
            ValuesBuffer.AppendValue(default);
            return base.AppendNull();
        }

        public override IArrayBuilder AppendNulls(int count)
        {
            ValuesBuffer.AppendValues(new T[count]);
            return base.AppendNulls(count);
        }

        public virtual ValueArrayBuilder<T> AppendValue(T value, bool isValid = true)
        {
            ValuesBuffer.AppendValue(value);
            ValidityBuffer.AppendBit(isValid);
            Length++;
            return this;
        }

        public virtual ValueArrayBuilder<T> AppendValue(T? value)
        {
            ValuesBuffer.AppendValue(value);
            ValidityBuffer.AppendBit(value.HasValue);
            Length++;
            return this;
        }

        public virtual ValueArrayBuilder<T> AppendValues(ReadOnlySpan<T> values)
            => AppendValues(values, ValidityMask(values.Length, true));

        public virtual ValueArrayBuilder<T> AppendValues(ReadOnlySpan<T?> values)
        {
            Span<bool> validity = new bool[values.Length];
            Span<T> destination = new T[values.Length];

            // Transform the source ReadOnlySpan<T?> into the destination ReadOnlySpan<T>, filling any null values with default(T)
            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] == null)
                {
                    destination[i] = default;
                    // default is already false
                    // validity[i] = false;
                }
                else
                {
                    destination[i] = values[i].Value;
                    validity[i] = true;
                }
            }

            return AppendValues(destination, validity);
        }

        public virtual ValueArrayBuilder<T> AppendValues(ReadOnlySpan<T> values, ReadOnlySpan<bool> mask)
        {
            ValuesBuffer.AppendValues(values);
            ValidityBuffer.AppendBits(mask);
            Length += values.Length;
            return this;
        }
    }

    public class VariableValueArrayBuilder<T>
        : BaseArrayBuilder where T : struct
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

        private int CurrentOffset;

        public VariableValueArrayBuilder(IArrowType dataType)
            : this(dataType, new ValueBufferBuilder<bool>(), new ValueBufferBuilder<int>(), new ValueBufferBuilder<T>())
        {
        }

        public VariableValueArrayBuilder(
            IArrowType dataType,
            IValueBufferBuilder<bool> validity, IValueBufferBuilder<int> offsets, IValueBufferBuilder<T> values
            ) : base(dataType, validity, new IValueBufferBuilder[] { validity, offsets, values })
        {
            ValuesBuffer = values;
            OffsetsBuffer = offsets;

            CurrentOffset = 0;
            OffsetsBuffer.AppendStruct(CurrentOffset);
        }

        public override IArrayBuilder AppendNull()
        {
            // Append Offset
            OffsetsBuffer.AppendStruct(CurrentOffset);

            // Append Values
            ValuesBuffer.AppendValue(default);
            return base.AppendNull();
        }

        public override IArrayBuilder AppendNulls(int count)
        {
            // Append Offset
            OffsetsBuffer.AppendStruct(CurrentOffset);

            // Append Values
            ValuesBuffer.AppendValues(new T[count]);
            return base.AppendNulls(count);
        }

        public virtual VariableValueArrayBuilder<T> AppendValue(T value)
        {
            // Append Offset
            CurrentOffset++;
            OffsetsBuffer.AppendStruct(CurrentOffset);

            // Append Values
            ValuesBuffer.AppendValue(value);
            ValidityBuffer.AppendBit(true);
            Length++;
            return this;
        }

        public virtual VariableValueArrayBuilder<T> AppendValue(T? value)
            => value.HasValue ? AppendValue(value.Value) : AppendNull() as VariableValueArrayBuilder<T>;

        public virtual VariableValueArrayBuilder<T> AppendValue(ReadOnlySpan<T> value)
        {
            // Append Offset
            CurrentOffset += value.Length;
            OffsetsBuffer.AppendStruct(CurrentOffset);

            // Append Values
            ValuesBuffer.AppendValues(value);
            ValidityBuffer.AppendBit(true);
            Length++;

            return this;
        }

        public virtual VariableValueArrayBuilder<T> AppendValue(ReadOnlySpan<T> value, bool isValid = true)
            => isValid ? AppendValue(value) : AppendNull() as VariableValueArrayBuilder<T>;

        public virtual VariableValueArrayBuilder<T> AppendValues(T[][] values)
        {
            Span<T> memory = new T[values.Sum(row => row.Length)];
            Span<int> offsets = new int[values.Count()];
            Span<bool> mask = new bool[offsets.Length];
            int offset = 0;

            for (int i = 0; i < values.Length; i++)
            {
                T[] value = values[i];

                value.CopyTo(memory.Slice(offset, value.Length));
                offset += value.Length;

                mask[i] = value == null;
            }

            return AppendValues(memory, offsets, mask);
        }

        public virtual VariableValueArrayBuilder<T> AppendValues(
            ReadOnlySpan<T> values, ReadOnlySpan<int> offsets, ReadOnlySpan<bool> mask
            )
        {
            // Append Offset
            OffsetsBuffer.AppendStructs(offsets);

            // Append Values
            ValuesBuffer.AppendValues(values);
            ValidityBuffer.AppendBits(mask);
            Length += mask.Length;

            return this;
        }
    }
}

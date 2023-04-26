using System;
using Apache.Arrow.Types;

namespace Apache.Arrow.Builder
{
    public class ValueArrayBuilder<T>
        : BaseArrayBuilder where T : struct
    {
        public IValueBufferBuilder<T> ValuesBuffer { get; internal set; }

        public ValueArrayBuilder(IArrowType dataType)
            : this(dataType, new ValueBufferBuilder<bool>(), new ValueBufferBuilder<T>())
        {
        }

        public ValueArrayBuilder(
            IArrowType dataType,
            IValueBufferBuilder<bool> validity, IValueBufferBuilder<T> values
            ) : this(dataType, validity, values, new IValueBufferBuilder[] { validity, values })
        {
        }

        internal ValueArrayBuilder(
            IArrowType dataType,
            IValueBufferBuilder<bool> validity,
            IValueBufferBuilder<T> values,
            IValueBufferBuilder[] buffers,
            IArrayBuilder[] children = null, IArrayBuilder dictionary = null
            ) : base(dataType, validity, buffers, children, dictionary)
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
        {
            ValuesBuffer.AppendValues(values);
            ValidityBuffer.AppendBits(ValidityMask(values.Length, true));
            Length += values.Length;
            return this;
        }

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

    public class ValueOffsetArrayBuilder<T>
        : ValueArrayBuilder<T> where T : struct
    {
        public IValueBufferBuilder<int> OffsetsBuffer { get; }

        public ValueOffsetArrayBuilder(IArrowType dataType)
            : this(dataType, new ValueBufferBuilder<bool>(), new ValueBufferBuilder<int>(), new ValueBufferBuilder<T>())
        {
        }

        public ValueOffsetArrayBuilder(
            IArrowType dataType,
            IValueBufferBuilder<bool> validity, IValueBufferBuilder<int> offset, IValueBufferBuilder<T> values
            ) : base(dataType, validity, values, new IValueBufferBuilder[] { validity, offset, values })
        {
            OffsetsBuffer = offset;
        }

        public ValueOffsetArrayBuilder<T> AppendOffset()
        {
            OffsetsBuffer.AppendValue(Length);
            return this;
        }
    }
}

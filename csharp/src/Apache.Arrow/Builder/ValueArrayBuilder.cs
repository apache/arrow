using System;
using System.Drawing;
using Apache.Arrow.Types;

namespace Apache.Arrow.Builder
{
    public class ValueArrayBuilder<T>
        : BaseArrayBuilder where T : struct
    {
        public virtual IBufferBuilder ValuesBuffer => Buffers[1];

        public ValueArrayBuilder(
            IArrowType dataType,
            IBufferBuilder validity, IBufferBuilder values
            ) : this(dataType, new IBufferBuilder[] { validity, values })
        {
        }

        public ValueArrayBuilder(
            IArrowType dataType, IBufferBuilder[] buffers,
            IDataBuilder[] children = null, IDataBuilder dictionary = null
            ) : base(dataType, buffers, children, dictionary)
        {
        }

        public virtual ValueArrayBuilder<T> AppendValue(T value, bool isValid = true)
        {
            ValuesBuffer.AppendStruct(value);
            ValidityBuffer.AppendBit(isValid);
            Length++;
            return this;
        }

        public virtual ValueArrayBuilder<T> AppendValue(T? value)
        {
            ValuesBuffer.AppendStruct(value.GetValueOrDefault());
            ValidityBuffer.AppendBit(value.HasValue);
            Length++;
            return this;
        }

        public virtual ValueArrayBuilder<T> AppendValues(ReadOnlySpan<T> values)
        {
            ValuesBuffer.AppendStructs(values);
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
            ValuesBuffer.AppendStructs(values);
            ValidityBuffer.AppendBits(mask);
            Length += values.Length;
            return this;
        }
    }
}

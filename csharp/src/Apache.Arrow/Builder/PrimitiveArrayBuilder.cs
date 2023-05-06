using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
using Apache.Arrow.Types;
using FlatBuffers;

namespace Apache.Arrow.Builder
{
    public class VariableBinaryArrayBuilder : ArrayBuilder
    {
        public IValueBufferBuilder ValuesBuffer { get; }

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

        public int CurrentOffset { get; internal set; }

        public VariableBinaryArrayBuilder(IArrowType dataType, int capacity = 64)
            : this(dataType, new ValueBufferBuilder<bool>(capacity), new ValueBufferBuilder<int>(capacity), new ValueBufferBuilder(64, capacity))
        {
        }

        public VariableBinaryArrayBuilder(
            IArrowType dataType,
            IPrimitiveBufferBuilder<bool> validity, IPrimitiveBufferBuilder<int> offsets, IValueBufferBuilder values
            ) : base(dataType, new IValueBufferBuilder[] { validity, offsets, values })
        {
            ValuesBuffer = values;
            OffsetsBuffer = offsets;
            CurrentOffset = 0;

            OffsetsBuffer.AppendValue(CurrentOffset);
        }

        public override IArrayBuilder AppendNull() => AppendNull(default);
        public virtual VariableBinaryArrayBuilder AppendNull(byte nullValue = default)
        {
            base.AppendNull();

            // Append Offset
            OffsetsBuffer.AppendValue(CurrentOffset);

            // Not Append Value, get is based on offsets
            // ValuesBuffer.AppendValue(nullValue);
            return this;
        }

        public override IArrayBuilder AppendNulls(int count) => AppendNulls(count, null);
        public VariableBinaryArrayBuilder AppendNulls(int count, ReadOnlySpan<byte> nullValues)
        {
            base.AppendNulls(count);

            // Append Offset
            OffsetsBuffer.AppendValues(CurrentOffset, count);

            // Not Append Values, get is based on offsets
            // ValuesBuffer.AppendValues(nullValues);
            return this;
        }

        public virtual VariableBinaryArrayBuilder AppendValue(string value, Encoding encoding = default)
            => AppendValue((encoding ?? StringType.DefaultEncoding).GetBytes(value), value != null);

        public virtual VariableBinaryArrayBuilder AppendValue(byte value)
        {
            AppendValid();

            // Append Offset
            CurrentOffset++;
            OffsetsBuffer.AppendValue(CurrentOffset);

            // Not Append Value, get is based on offsets
            ValuesBuffer.AppendByte(value);
            return this;
        }

        public virtual VariableBinaryArrayBuilder AppendValue(byte? value)
            => value.HasValue ? AppendValue(value.Value) : AppendNull();

        public virtual VariableBinaryArrayBuilder AppendValue(ReadOnlySpan<byte> value, bool isValid = true)
        {
            if (isValid)
            {
                AppendValid();
                // Append Offset
                CurrentOffset += value.Length;
                OffsetsBuffer.AppendValue(CurrentOffset);
                ValuesBuffer.AppendBytes(value);
            }
            else
            {
                AppendNull();
                OffsetsBuffer.AppendValue(CurrentOffset);
            }
            return this;
        }

        public virtual VariableBinaryArrayBuilder AppendValues(ICollection<byte[]> values)
        {
            Span<int> offsets = new int[values.Count];
            Span<bool> mask = new bool[offsets.Length];
            int offset = 0;
            int i = 0;

            foreach (byte[] value in values)
            {
                if (value == null)
                {
                    offsets[i] = CurrentOffset;
                    // default is already false
                    // mask[i] = false;
                }
                else
                {
                    // Copy to memory
                    ValuesBuffer.AppendBytes(value);

                    offset += value.Length;
                    CurrentOffset += value.Length;

                    // Fill other buffers
                    offsets[i] = CurrentOffset;
                    mask[i] = true;
                }
                i++;
            }

            AppendValidity(mask);

            // Append Offset
            OffsetsBuffer.AppendValues(offsets);

            return this;
        }

        public virtual VariableBinaryArrayBuilder AppendValues(ICollection<string> values, Encoding encoding = default)
        {
            encoding = encoding ?? StringType.DefaultEncoding;
            Span<int> offsets = new int[values.Count];
            Span<bool> mask = new bool[offsets.Length];
            int i = 0;

            foreach (string value in values)
            {
                if (value == null)
                {
                    offsets[i] = CurrentOffset;
                    // default is already false
                    // mask[i] = false;
                }
                else
                {
                    // Copy to memory
                    byte[] bytes = encoding.GetBytes(value);
                    ValuesBuffer.AppendBytes(bytes);

                    CurrentOffset += bytes.Length;

                    // Fill other buffers
                    offsets[i] = CurrentOffset;
                    mask[i] = true;
                }
                i++;
            }

            AppendValidity(mask);

            // Append Offset
            OffsetsBuffer.AppendValues(offsets);

            return this;
        }
    }

    public class FixedBinaryArrayBuilder : ArrayBuilder
    {
        private readonly byte[] _defaultByteValue;
        private readonly bool[] _defaultBitValue;

        private readonly int _bitSize;
        private readonly int _byteSize;

        private readonly bool _isFullByte;

        public IValueBufferBuilder ValuesBuffer { get; }

        public FixedBinaryArrayBuilder(FixedWidthType dtype, int capacity = 64)
            : this(dtype, new ValueBufferBuilder<bool>(capacity), new ValueBufferBuilder(dtype.BitWidth, capacity))
        {
        }

        public FixedBinaryArrayBuilder(
            IArrowType dataType,
            IPrimitiveBufferBuilder<bool> validity, IValueBufferBuilder values
            ) : base(dataType, new IValueBufferBuilder[] { validity, values })
        {
            ValuesBuffer = values;

            _bitSize = (dataType as FixedWidthType).BitWidth;
            _byteSize = _bitSize / 8;

            _isFullByte = _bitSize % 8 == 0;

            _defaultByteValue = new byte[_byteSize];
            _defaultBitValue = new bool[_bitSize];
        }

        public override IArrayBuilder AppendNull() => AppendNull(default);
        public virtual FixedBinaryArrayBuilder AppendNull(byte nullValue = default)
        {
            base.AppendNull();

            // Append Empty values
            if (_isFullByte)
                ValuesBuffer.AppendBytes(_defaultByteValue);
            else
                ValuesBuffer.AppendBits(_defaultBitValue);

            return this;
        }

        public override IArrayBuilder AppendNulls(int count) => AppendNulls(count, default);
        public FixedBinaryArrayBuilder AppendNulls(int count, byte nullValue = default)
        {
            base.AppendNulls(count);

            // Append Empty values
            if (_isFullByte)
                for (int i = 0; i < count; i++)
                    ValuesBuffer.AppendBytes(_defaultByteValue);
            else
                for (int i = 0; i < count; i++)
                    ValuesBuffer.AppendBits(_defaultBitValue);

            return this;
        }

        // Raw struct
        public virtual FixedBinaryArrayBuilder AppendValue(bool value)
        {
            AppendValid();
            ValuesBuffer.AppendBit(value);
            return this;
        }
        public virtual FixedBinaryArrayBuilder AppendValue(byte value)
        {
            AppendValid();
            ValuesBuffer.AppendByte(value);
            return this;
        }
        public virtual FixedBinaryArrayBuilder AppendValue<T>(T value) where T : struct
        {
            AppendValid();
#if NETCOREAPP3_1_OR_GREATER
            var span = MemoryMarshal.CreateReadOnlySpan(ref value, 1);
#else
            var span = new T[] { value }.AsSpan();
#endif
            ValuesBuffer.AppendBytes(MemoryMarshal.AsBytes<T>(span));
            return this;
        }

        // Nullable raw struct
        public virtual FixedBinaryArrayBuilder AppendValue(bool? value)
            => value.HasValue ? AppendValue(value.Value) : AppendNull();

        public virtual FixedBinaryArrayBuilder AppendValue(byte? value)
            => value.HasValue ? AppendValue(value.Value) : AppendNull();
        public virtual FixedBinaryArrayBuilder AppendValue<T>(T? value) where T : struct
            => value.HasValue ? AppendValue(value.Value) : AppendNull();

        // ReadOnlySpan value
        public virtual FixedBinaryArrayBuilder AppendValue(ReadOnlySpan<bool> value)
        {
            AppendValid();
            ValuesBuffer.AppendBits(value);
            return this;
        }
        public virtual FixedBinaryArrayBuilder AppendValue(ReadOnlySpan<byte> value)
        {
            AppendValid();
            ValuesBuffer.AppendBytes(value);
            return this;
        }
        public virtual FixedBinaryArrayBuilder AppendValue<T>(ReadOnlySpan<T> value) where T : struct
        {
            AppendValid();
            ValuesBuffer.AppendBytes(MemoryMarshal.AsBytes(value));
            return this;
        }


        // Bulk raw struct several values
        public virtual FixedBinaryArrayBuilder AppendValues(ReadOnlySpan<bool> values)
        {
            AppendValidity(true, values.Length / _bitSize);
            ValuesBuffer.AppendBits(values);
            return this;
        }
        public virtual FixedBinaryArrayBuilder AppendValues(ReadOnlySpan<byte> values)
        {
            AppendValidity(true, values.Length * 8 / _bitSize);
            ValuesBuffer.AppendBytes(values);
            return this;
        }
        public virtual FixedBinaryArrayBuilder AppendValues(ReadOnlySpan<byte> values, ReadOnlySpan<bool> validity)
        {
            AppendValidity(validity);
            ValuesBuffer.AppendBytes(values);
            return this;
        }
        public virtual FixedBinaryArrayBuilder AppendValues<T>(ReadOnlySpan<T> values) where T : struct
            => AppendValues(MemoryMarshal.AsBytes(values));
        public virtual FixedBinaryArrayBuilder AppendValues<T>(T[] values) where T : struct
            => AppendValues(MemoryMarshal.AsBytes<T>(values));
        public virtual FixedBinaryArrayBuilder AppendValues<T>(ReadOnlySpan<T> values, ReadOnlySpan<bool> validity) where T : struct
            => AppendValues(MemoryMarshal.AsBytes(values), validity);

        // Bulk nullable raw struct several values
        public virtual FixedBinaryArrayBuilder AppendValues(ICollection<bool?> values)
        {
            int count = values.Count;
            Span<bool> mask = new bool[count];
            int i = 0;

            foreach (bool? value in values)
            {
                // default is already false
                // mask[i] = false;

                // default is already filled with empty values
                // _defaultValue.CopyTo(memory.Slice(offset, _defaultValue.Length));

                if (value.HasValue)
                {
                    // Copy to memory
                    ValuesBuffer.AppendBit(value.Value);
                    mask[i] = true;
                }
                else
                {
                    ValuesBuffer.AppendBits(_defaultBitValue);
                }
                i++;
            }

            AppendValidity(mask);

            return this;
        }

        public virtual FixedBinaryArrayBuilder AppendValues(ICollection<bool[]> values)
        {
            int count = values.Count;
            Span<bool> mask = new bool[count];
            int i = 0;

            foreach (bool[] value in values)
            {
                // default is already false
                // mask[i] = false;

                // default is already filled with empty values
                // _defaultValue.CopyTo(memory.Slice(offset, _defaultValue.Length));

                if (value == null)
                {
                    ValuesBuffer.AppendBits(_defaultBitValue);
                }
                else
                {
                    // Copy to memory
                    ValuesBuffer.AppendBits(value);
                    mask[i] = true;
                }
                i++;
            }

            AppendValidity(mask);

            return this;
        }

        public virtual FixedBinaryArrayBuilder AppendValues(ICollection<byte?> values)
        {
            int count = values.Count;
            Span<bool> mask = new bool[count];
            int i = 0;

            foreach (byte? value in values)
            {
                // default is already false
                // mask[i] = false;

                // default is already filled with empty values
                // _defaultValue.CopyTo(memory.Slice(offset, _defaultValue.Length));

                if (value.HasValue)
                {
                    // Copy to memory
                    ValuesBuffer.AppendByte(value.Value);
                    mask[i] = true;
                }
                else
                {
                    ValuesBuffer.AppendBytes(_defaultByteValue);
                }
                i++;
            }

            AppendValidity(mask);

            return this;
        }
        public virtual FixedBinaryArrayBuilder AppendValues(ICollection<byte[]> values)
        {
            int count = values.Count;
            Span<byte> memory = new byte[count * _byteSize];
            Span<bool> mask = new bool[count];
            int i = 0;
            int offset = 0;

            foreach (byte[] value in values)
            {
                // default is already false
                // mask[i] = false;

                // default is already filled with empty values
                // _defaultValue.CopyTo(memory.Slice(offset, _defaultValue.Length));

                if (value != null)
                {
                    // Copy to memory
                    value.CopyTo(memory.Slice(offset, _defaultByteValue.Length));
                    mask[i] = true;
                }
                offset += _defaultByteValue.Length;
                i++;
            }

            ValuesBuffer.AppendBytes(memory);
            AppendValidity(mask);

            return this;
        }

        public virtual FixedBinaryArrayBuilder AppendValues<T>(ICollection<T?> values) where T : struct
        {
            int count = values.Count;
            Span<byte> memory = new byte[count * _byteSize];
            Span<bool> mask = new bool[count];
            int i = 0;
            int offset = 0;

            foreach (T? value in values)
            {
                // default is already false
                // mask[i] = false;

                // default is already filled with empty values
                // _defaultValue.CopyTo(memory.Slice(offset, _defaultValue.Length));

                if (value.HasValue)
                {
                    // Copy to memory
                    T v = value.Value;
#if NETCOREAPP3_1_OR_GREATER
                    var span = MemoryMarshal.CreateReadOnlySpan(ref v, 1);
#else
                    var span = new T[] { v }.AsSpan();
#endif
                    MemoryMarshal.AsBytes(span).CopyTo(memory.Slice(offset, _defaultByteValue.Length));

                    mask[i] = true;
                }
                offset += _defaultByteValue.Length;
                i++;
            }

            ValuesBuffer.AppendBytes(memory);
            AppendValidity(mask);

            return this;
        }

        public virtual FixedBinaryArrayBuilder AppendValues<T>(ICollection<T[]> values) where T : struct
        {
            int count = values.Count;
            Span<byte> memory = new byte[count * _byteSize];
            Span<bool> mask = new bool[count];
            int i = 0;
            int offset = 0;

            foreach (T[] value in values)
            {
                // default is already false
                // mask[i] = false;

                // default is already filled with empty values
                // _defaultValue.CopyTo(memory.Slice(offset, _defaultValue.Length));

                if (value != null)
                {
                    // Copy to memory
                    MemoryMarshal.AsBytes<T>(value).CopyTo(memory.Slice(offset, _defaultByteValue.Length));
                    mask[i] = true;
                }
                offset += _defaultByteValue.Length;
                i++;
            }

            ValuesBuffer.AppendBytes(memory);
            AppendValidity(mask);

            return this;
        }
    }

    public class FixedBinaryArrayBuilder<T> : FixedBinaryArrayBuilder where T : struct
    {
        public FixedBinaryArrayBuilder(int capacity = 64) : base(CStructType<T>.Default as FixedWidthType, capacity)
        {
        }
    }
}

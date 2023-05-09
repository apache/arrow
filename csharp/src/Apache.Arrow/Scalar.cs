using System;
using Apache.Arrow.Reflection;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    // Inspired from C++ implementation
    // https://arrow.apache.org/docs/cpp/api/scalar.html

    public struct BinaryScalar : IBaseBinaryScalar
    {
        private int _offset;
        public int ByteLength { get; }
        public ArrowBuffer Buffer { get; }

        public IArrowType Type => BinaryType.Default;

        public BinaryScalar(ArrowBuffer value)
            : this(value, value.Length, 0)
        {
        }

        public BinaryScalar(ArrowBuffer value, int length, int offset = 0)
        {
            Buffer = value;
            ByteLength = length;

            _offset = offset;
        }

        public ReadOnlySpan<byte> View() => Buffer.Span.Slice(_offset, ByteLength);
    }

    public struct StringScalar : IBaseBinaryScalar
    {
        private int _offset;
        public int ByteLength { get; }
        public ArrowBuffer Buffer { get; }

        public IArrowType Type => StringType.Default;

        public StringScalar(string value)
            : this(new ArrowBuffer(StringType.DefaultEncoding.GetBytes(value)))
        {
        }

        private StringScalar(ArrowBuffer value)
            : this(value, value.Length, 0)
        {
        }

        public StringScalar(ArrowBuffer value, int length, int offset = 0)
        {
            Buffer = value;
            ByteLength = length;

            _offset = offset;
        }

        public ReadOnlySpan<byte> View() => Buffer.Span.Slice(_offset, ByteLength);
    }

    // Numeric scalars
    public struct Int32Scalar : INumericScalar<Int32Type>
    {
        private int _offset;
        public ArrowBuffer Buffer { get; }

        public Int32Type Type => Int32Type.Default;

        IArrowType IScalar.Type => Type;

        public Int32Scalar(int value)
            : this(new ArrowBuffer(TypeReflection.AsMemoryBytes(value)))
        {
        }

        private Int32Scalar(ArrowBuffer value)
            : this(value, 0)
        {
        }

        public Int32Scalar(ArrowBuffer value, int offset = 0)
        {
            Buffer = value;
            _offset = offset;
        }

        public ReadOnlySpan<byte> View() => Buffer.Span.Slice(_offset, 4);
    }


    // Nested scalars
    public struct StructScalar : IScalar
    {
        public StructType Type { get; }
        IArrowType IScalar.Type => Type;
        public IScalar[] Fields { get; }

        public bool IsValid => true;

        public StructScalar(StructType type, IScalar[] fields)
        {
            Type = type;
            Fields = fields;
        }
    }
}

using System;
using System.Collections.Generic;
using Apache.Arrow.Builder;
using Apache.Arrow.Types;

namespace Apache.Arrow.Values
{
    public interface Scalar
    {
        IArrowType DataType { get; }
        bool IsValid { get; }
    }

    public interface IPrimitiveScalar<T> : Scalar where T : struct
    {
        T Value { get; }
    }

    public interface NestedScalar : Scalar

    {
        IArrowArray Values { get; }
    }

    // Binary
    public interface IBinaryScalar : Scalar
    {
        byte[] Values { get; }
    }

    public struct BinaryScalar : IBinaryScalar
    {
        public IArrowType DataType { get; }
        public bool IsValid { get; }
        public byte[] Values { get; }

        public BinaryScalar(byte[] values, bool isValid = true)
            : this(BinaryType.Default, values, isValid)
        {
        }

        public BinaryScalar(BinaryType dataType, byte[] values, bool isValid = true)
        {
            DataType = dataType;
            Values = values;
            IsValid = isValid;
        }
    }

    public struct StringScalar : IBinaryScalar
    {
        IArrowType Scalar.DataType => DataType;
        public StringType DataType { get; }
        public bool IsValid { get; }
        public string Value { get; }
        public byte[] Values => DataType.Encoding.GetBytes(Value);

        public StringScalar(string value) : this(StringType.Default, value, value != null)
        {
        }

        public StringScalar(StringType dataType, string value, bool isValid)
        {
            Value = value;
        }
    }

    // Primitive / Numeric
    public struct PrimitiveScalar<T> : IPrimitiveScalar<T> where T : struct
    {
        public IArrowType DataType { get; }

        public T Value { get; }
        public bool IsValid { get; }

        public PrimitiveScalar(T? value)
            : this(PrimitiveType<T>.Default, value.GetValueOrDefault(), value != null)
        {
        }

        public PrimitiveScalar(T value, bool isValid = true)
            : this(PrimitiveType<T>.Default, value, isValid)
        {
        }

        public PrimitiveScalar(IArrowType dataType, T value, bool isValid = true)
        {
            DataType = dataType;
            Value = value;
            IsValid = isValid;
        }
    }

    // Nested
    public struct ListScalar : NestedScalar
    {
        // Makers
        public static ListScalar Make<T>(ICollection<T?> values) where T : struct
            => Make(PrimitiveType<T>.Default, values);
        public static ListScalar Make<T>(IArrowType dtype, ICollection<T?> values) where T : struct
        {
            IArrowArray array = new PrimitiveArrayBuilder<T>(dtype, values.Count).AppendValues(values).Build();

            return new ListScalar(new ListType(array.Data.DataType), array, true);
        }

        public IArrowType DataType { get; }

        public IArrowArray Values { get; }
        public bool IsValid { get; }

        public ListScalar(ListType dataType, IArrowArray values, bool isValid)
        {
            DataType = dataType;
            Values = values;
            IsValid = isValid;
        }
    }
}

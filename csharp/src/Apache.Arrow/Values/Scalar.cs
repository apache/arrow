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

    public struct PrimitiveScalar<T> : IPrimitiveScalar<T> where T : struct

    {
        public IArrowType DataType { get; }

        public T Value { get; }
        public bool IsValid { get; }

        public PrimitiveScalar(IArrowType dataType, T value, bool isValid)
        {
            DataType = dataType;
            Value = value;
            IsValid = isValid;
        }
    }

    public struct ListScalar : NestedScalar

    {
        public IArrowType DataType { get; }

        public IArrowArray Values { get; }
        public bool IsValid { get; }

        public ListScalar(ListType dataType, Array values, bool isValid)
        {
            DataType = dataType;
            Values = values;
            IsValid = isValid;
        }
    }
}

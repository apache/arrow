namespace Apache.Arrow.Types
{
    internal class PrimitiveType<T> where T : struct
    {
        internal static readonly IArrowType Default = new Field.Builder().DataType(typeof(T))._type;
    }
}

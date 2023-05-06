namespace Apache.Arrow.Types
{
    internal class CStructType<T> where T : struct
    {
        internal static readonly IArrowType Default = new Field.Builder().DataType(typeof(T)).CurrentType;
    }
}

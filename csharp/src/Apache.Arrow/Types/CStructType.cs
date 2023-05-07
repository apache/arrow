using System.Linq;
using System.Reflection;

namespace Apache.Arrow.Types
{
    internal class CStructType
    {
#if NETCOREAPP2_0_OR_GREATER
        internal static PropertyInfo[] GetProperties(System.Type type) => type
            .GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .Where(p => p.GetIndexParameters().Length == 0)
            .ToArray();
#endif
    }

    internal class CStructType<T> : CStructType
    {
        internal static readonly IArrowType Default = new Field.Builder().DataType(typeof(T)).CurrentType;
    }
}

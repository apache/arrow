using System.Collections.Generic;
using Apache.Arrow.Builder;
using Apache.Arrow.Types;

namespace Apache.Arrow.Values
{
    public class ScalarFactory
    {
        public static IPrimitiveScalar<T> MakeScalar<T>(T? value) where T : struct
            => MakeScalar(value.GetValueOrDefault(), true);

        public static IPrimitiveScalar<T> MakeScalar<T>(T value, bool isValid = true) where T : struct
        {
            IArrowType dtype = new Field.Builder().DataType(typeof(T))._type;

            return new PrimitiveScalar<T>(dtype, value, isValid);
        }

        public static ListScalar MakeScalar<T>(ICollection<T?> values) where T : struct
        {
            var array = new PrimitiveArrayBuilder<T>(values.Count).AppendValues(values).Build();

            return new ListScalar(new ListType(array.Data.DataType), array as Array, true);
        }
    }
}

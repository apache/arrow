﻿// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Apache.Arrow.Types;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Apache.Arrow
{
    public partial class Field
    {
        public class Builder
        {
            private Dictionary<string, string> _metadata;
            private string _name;
            internal IArrowType _type;
            private bool _nullable;

            public Builder()
            {
                _name = string.Empty;
                _type = NullType.Default;
                _nullable = true;
            }

            public Builder Name(string value)
            {
                if (string.IsNullOrWhiteSpace(value))
                {
                    throw new ArgumentNullException(nameof(value));
                }

                _name = value;
                return this;
            }

            public Builder DataType(IArrowType type)
            {
                _type = type ?? NullType.Default;
                return this;
            }

            // Build DataType from C# Type
            public Builder DataType(System.Type valueType, string defaultTimezone = null, Dictionary<string, string> fieldTimezones = null)
            {
                System.Type child = System.Nullable.GetUnderlyingType(valueType);

                if (child != null)
                {
                    // Nullable Type
                    DataType(child, defaultTimezone);
                    Nullable(true);
                    return this;
                }

                // Not Nullable Type
                Nullable(false);

                switch (valueType)
                {
                    // Boolean
                    case var _ when valueType == typeof(bool):
                        DataType(new BooleanType());
                        break;
                    // String
                    case var _ when valueType == typeof(string):
                        DataType(new StringType());
                        Nullable(true);
                        break;
                    // Integers
                    case var _ when valueType == typeof(sbyte):
                        DataType(new Int8Type());
                        break;
                    case var _ when valueType == typeof(short):
                        DataType(new Int16Type());
                        break;
                    case var _ when valueType == typeof(int):
                        DataType(new Int32Type());
                        break;
                    case var _ when valueType == typeof(long):
                        DataType(new Int64Type());
                        break;
                    // Unisgned Integers
                    case var _ when valueType == typeof(byte):
                        DataType(new UInt8Type());
                        break;
                    case var _ when valueType == typeof(ushort):
                        DataType(new UInt16Type());
                        break;
                    case var _ when valueType == typeof(uint):
                        DataType(new UInt32Type());
                        break;
                    case var _ when valueType == typeof(ulong):
                        DataType(new UInt64Type());
                        break;
                    // Decimal
                    case var _ when valueType == typeof(decimal):
                        DataType(Decimal256Type.SystemDefault);
                        break;
                    // Float
                    case var _ when valueType == typeof(float):
                        DataType(new FloatType());
                        break;
                    // Double
                    case var _ when valueType == typeof(double):
                        DataType(new DoubleType());
                        break;
                    // DateTime
                    case var _ when valueType == typeof(DateTime) || valueType == typeof(DateTimeOffset):
                        DataType(new TimestampType(TimeUnit.Nanosecond, defaultTimezone));
                        break;
                    // Time
                    case var _ when valueType == typeof(TimeSpan):
                        DataType(new Time64Type(TimeUnit.Nanosecond));
                        break;
#if NETCOREAPP3_1_OR_GREATER
                    // IEnumerable: List, Array, ...
                    case var list when typeof(IEnumerable).IsAssignableFrom(valueType):
                        Type elementType = list.GetGenericArguments()[0];

                        switch (elementType)
                        {
                            // Binary
                            case var _ when valueType == typeof(byte):
                                DataType(new BinaryType());
                                break;
                            // List of Arrays
                            default:
                                DataType(new ListType(new Builder().Name("item").DataType(elementType, defaultTimezone, fieldTimezones).Build()));
                                break;
                        }
                        Nullable(true);
                        break;
                    // Struct like: get all properties
                    case var structure when (valueType.IsValueType && !valueType.IsEnum && !valueType.IsPrimitive):
                        if (string.IsNullOrEmpty(_name))
                            Name(structure.Name);

                        PropertyInfo[] properties = structure.GetProperties(BindingFlags.Instance | BindingFlags.Public);

                        DataType(new StructType(
                            properties
                                .Select(property => {
                                    // Check if it is not given in parameters, or get default
                                    string fieldTimezone = fieldTimezones?.GetValueOrDefault(structure.Name, defaultTimezone);

                                    return new Builder().Name(property.Name).DataType(property.PropertyType, fieldTimezone, fieldTimezones).Build();
                                })
                                .ToArray()
                        ));

                        break;
#endif
                    // Error
                    // TODO: Dictionary: IDictionary<?,?> : should be MapType, not implemented yet
                    default:
                        throw new InvalidCastException($"Cannot convert System.Type<{valueType}> to ArrowType");
                }

                return this;
            }

            public Builder Nullable(bool value)
            {
                _nullable = value;
                return this;
            }

            public Builder Metadata(string key, string value)
            {
                if (string.IsNullOrWhiteSpace(key))
                {
                    throw new ArgumentNullException(nameof(key));
                }

                _metadata ??= new Dictionary<string, string>();

                _metadata[key] = value;
                return this;
            }

            public Builder Metadata(IEnumerable<KeyValuePair<string, string>> dictionary)
            {
                if (dictionary == null)
                {
                    throw new ArgumentNullException(nameof(dictionary));
                }
                foreach (KeyValuePair<string, string> entry in dictionary)
                {
                    Metadata(entry.Key, entry.Value);
                }
                return this;
            }

            public Field Build()
            {
                return new Field(_name, _type, _nullable, _metadata);
            }
        }
    }
}

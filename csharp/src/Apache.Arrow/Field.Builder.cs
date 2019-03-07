// Licensed to the Apache Software Foundation (ASF) under one or more
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
using System.Collections.Generic;

namespace Apache.Arrow
{
    public partial class Field
    {
        public class Builder
        {
            private readonly Dictionary<string, string> _metadata;
            private string _name;
            private IArrowType _type;
            private bool _nullable;

            public Builder()
            {
                _metadata = new Dictionary<string, string>();
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

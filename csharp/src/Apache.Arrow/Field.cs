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

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    public partial class Field
    {
        public IArrowType DataType { get; }

        public string Name { get; }

        public bool IsNullable { get; }

        public bool HasMetadata => Metadata?.Count > 0;

        public IReadOnlyDictionary<string, string> Metadata { get; }

        public Field(string name, IArrowType dataType, bool nullable,
            IEnumerable<KeyValuePair<string, string>> metadata = default)
            : this(name, dataType, nullable)
        {
            Metadata = metadata?.ToDictionary(kv => kv.Key, kv => kv.Value);
        }

        internal Field(string name, IArrowType dataType, bool nullable,
            IReadOnlyDictionary<string, string> metadata, bool copyCollections)
            : this(name, dataType, nullable)
        {
            Debug.Assert(copyCollections == false, "This internal constructor is to not copy the collections.");

            Metadata = metadata;
        }

        private Field(string name, IArrowType dataType, bool nullable)
        {
            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            Name = name;
            DataType = dataType ?? NullType.Default;
            IsNullable = nullable;
        }

        public override string ToString() => $"{nameof(Field)}: Name={Name}, DataType={DataType.Name}, IsNullable={IsNullable}, Metadata count={Metadata?.Count ?? 0}";
    }
}

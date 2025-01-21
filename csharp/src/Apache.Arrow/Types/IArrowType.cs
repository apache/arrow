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


namespace Apache.Arrow.Types
{
    public enum ArrowTypeId
    {
        Null,
        Boolean,
        UInt8,
        Int8,
        UInt16,
        Int16,
        UInt32,
        Int32,
        UInt64,
        Int64,
        HalfFloat,
        Float,
        Double,
        String,
        Binary,
        FixedSizedBinary,
        Date32,
        Date64,
        Timestamp,
        Time32,
        Time64,
        Interval,
        Decimal128,
        Decimal256,
        List,
        Struct,
        Union,
        Dictionary,
        Map,
        FixedSizeList,
        Duration,
        RecordBatch,
        BinaryView,
        StringView,
        ListView,
        LargeList,
        LargeBinary,
        LargeString,
        Decimal32,
        Decimal64,
    }

    public interface IArrowType
    {
        ArrowTypeId TypeId { get; }

        string Name { get; }
 
        void Accept(IArrowTypeVisitor visitor);

        bool IsFixedWidth { get; }
    
    }
}

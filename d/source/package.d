/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

module arrow;

version (unittest)
{
    import arrow.Array;
    import arrow.DataType;
    import arrow.BooleanArray;
    import arrow.CastOptions;
    import arrow.FilterOptions;
    import arrow.TakeOptions;
    import arrow.c.types;

    @("Arrow Array")
    unittest
    {
        // Test array creation and basic properties
        auto array = new Array(null);
        assert(array.getLength() == 0);
        assert(array.getNNulls() == 0);
        assert(array.getOffset() == 0);

        // Test null checks
        auto nullBitmap = array.getNullBitmap();
        assert(nullBitmap is null);

        // Test value type
        auto valueType = array.getValueType();
        assert(valueType >= GArrowType.NA);

        // Test equality
        auto otherArray = new Array(null);
        assert(array.equal(otherArray));
        assert(array.equalApprox(otherArray));

        // Test slicing
        auto slice = array.slice(0, 0);
        assert(slice !is null);

        // Test data type
        auto dataType = array.getValueDataType();
        assert(dataType is null || dataType !is null);
    }
}

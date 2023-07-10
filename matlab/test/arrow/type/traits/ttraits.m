% Licensed to the Apache Software Foundation (ASF) under one or more
% contributor license agreements.  See the NOTICE file distributed with
% this work for additional information regarding copyright ownership.
% The ASF licenses this file to you under the Apache License, Version
% 2.0 (the "License"); you may not use this file except in compliance
% with the License.  You may obtain a copy of the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS,
% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
% implied.  See the License for the specific language governing
% permissions and limitations under the License.

classdef ttraits < matlab.unittest.TestCase
    % Tests for the traits (i.e. arrow.type.traits.traits) "gateway" function.

    methods(Test)

        function TestUInt8Type(testCase)
            import arrow.type.traits.*

            type = arrow.type.UInt8Type();
            expectedTraits = arrow.type.traits.UInt8Traits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestUInt16Type(testCase)
            import arrow.type.traits.*

            type = arrow.type.UInt16Type();
            expectedTraits = arrow.type.traits.UInt16Traits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestUInt32Type(testCase)
            import arrow.type.traits.*

            type = arrow.type.UInt32Type();
            expectedTraits = arrow.type.traits.UInt32Traits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestUInt64Type(testCase)
            import arrow.type.traits.*

            type = arrow.type.UInt64Type();
            expectedTraits = arrow.type.traits.UInt64Traits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestInt8Type(testCase)
            import arrow.type.traits.*

            type = arrow.type.Int8Type();
            expectedTraits = arrow.type.traits.Int8Traits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestInt16Type(testCase)
            import arrow.type.traits.*

            type = arrow.type.Int16Type();
            expectedTraits = arrow.type.traits.Int16Traits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestInt32Type(testCase)
            import arrow.type.traits.*

            type = arrow.type.Int32Type();
            expectedTraits = arrow.type.traits.Int32Traits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end
        
        function TestInt64Type(testCase)
            import arrow.type.traits.*

            type = arrow.type.Int64Type();
            expectedTraits = arrow.type.traits.Int64Traits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestStringType(testCase)
            import arrow.type.traits.*

            type = arrow.type.StringType();
            expectedTraits = arrow.type.traits.StringTraits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestTimestampType(testCase)
            import arrow.type.traits.*

            type = arrow.type.TimestampType();
            expectedTraits = arrow.type.traits.TimestampTraits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestBooleanType(testCase)
            import arrow.type.traits.*

            type = arrow.type.BooleanType();
            expectedTraits = arrow.type.traits.BooleanTraits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestErrorIfNotType(testCase)
            import arrow.type.traits.*

            type = "not-a-type";

            testCase.verifyError(@() traits(type), "MATLAB:validation:UnableToConvert");
        end

    end

end
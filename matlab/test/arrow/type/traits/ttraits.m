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
    % Tests for the type traits (i.e. arrow.type.traits.traits)
    % "gateway" function.

    methods(Test)

        function TestUInt8(testCase)
            import arrow.type.traits.*
            import arrow.type.*

            typeID = ID.UInt8;
            expectedTraits = UInt8Traits();

            actualTraits = traits(typeID);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestUInt16(testCase)
            import arrow.type.traits.*
            import arrow.type.*

            type = ID.UInt16;
            expectedTraits = UInt16Traits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestUInt32(testCase)
            import arrow.type.traits.*
            import arrow.type.*

            type = ID.UInt32;
            expectedTraits = UInt32Traits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestUInt64(testCase)
            import arrow.type.traits.*
            import arrow.type.*

            type = ID.UInt64;
            expectedTraits = UInt64Traits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestInt8(testCase)
            import arrow.type.traits.*
            import arrow.type.*

            type = ID.Int8;
            expectedTraits = Int8Traits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestInt16(testCase)
            import arrow.type.traits.*
            import arrow.type.*

            type = ID.Int16;
            expectedTraits = Int16Traits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestInt32(testCase)
            import arrow.type.traits.*
            import arrow.type.*

            type = ID.Int32;
            expectedTraits = Int32Traits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end
        
        function TestInt64(testCase)
            import arrow.type.traits.*
            import arrow.type.*

            type = ID.Int64;
            expectedTraits = Int64Traits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestString(testCase)
            import arrow.type.traits.*
            import arrow.type.*

            type = ID.String;
            expectedTraits = StringTraits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestTimestamp(testCase)
            import arrow.type.traits.*
            import arrow.type.*

            type = ID.Timestamp;
            expectedTraits = TimestampTraits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestBoolean(testCase)
            import arrow.type.traits.*
            import arrow.type.*

            type = ID.Boolean;
            expectedTraits = BooleanTraits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestTime32(testCase)
            import arrow.type.traits.*
            import arrow.type.*

            type = ID.Time32;
            expectedTraits = Time32Traits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits); 
        end

        function TestTime64(testCase)
            import arrow.type.traits.*
            import arrow.type.*

            type = ID.Time64;
            expectedTraits = Time64Traits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits); 
        end

        function TestDate32(testCase)
            import arrow.type.traits.*
            import arrow.type.*

            type = ID.Date32;
            expectedTraits = Date32Traits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestDate64(testCase)
            import arrow.type.traits.*
            import arrow.type.*

            type = ID.Date64;
            expectedTraits = Date64Traits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestStruct(testCase)
            import arrow.type.traits.*
            import arrow.type.*

            type = ID.Struct;
            expectedTraits = StructTraits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestList(testCase)
            import arrow.type.traits.*
            import arrow.type.*

            type = ID.List;
            expectedTraits = ListTraits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestMatlabUInt8(testCase)
            import arrow.type.traits.*

            type = "uint8";
            expectedTraits = UInt8Traits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestMatlabUInt16(testCase)
            import arrow.type.traits.*

            type = "uint16";
            expectedTraits = UInt16Traits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestMatlabUInt32(testCase)
            import arrow.type.traits.*

            type = "uint32";
            expectedTraits = UInt32Traits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestMatlabUInt64(testCase)
            import arrow.type.traits.*

            type = "uint64";
            expectedTraits = UInt64Traits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestMatlabInt8(testCase)
            import arrow.type.traits.*

            type = "int8";
            expectedTraits = Int8Traits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestMatlabInt16(testCase)
            import arrow.type.traits.*

            type = "int16";
            expectedTraits = Int16Traits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestMatlabInt32(testCase)
            import arrow.type.traits.*

            type = "int32";
            expectedTraits = Int32Traits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestMatlabInt64(testCase)
            import arrow.type.traits.*

            type = "int64";
            expectedTraits = Int64Traits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestMatlabSingle(testCase)
            import arrow.type.traits.*

            type = "single";
            expectedTraits = Float32Traits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestMatlabDouble(testCase)
            import arrow.type.traits.*

            type = "double";
            expectedTraits = Float64Traits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestMatlabLogical(testCase)
            import arrow.type.traits.*

            type = "logical";
            expectedTraits = BooleanTraits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestMatlabString(testCase)
            import arrow.type.traits.*

            type = "string";
            expectedTraits = StringTraits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestMatlabDatetime(testCase)
            import arrow.type.traits.*

            type = "datetime";
            expectedTraits = TimestampTraits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestMatlabDuration(testCase)
            import arrow.type.traits.*

            type = "duration";
            expectedTraits = Time64Traits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestMatlabTable(testCase)
            import arrow.type.traits.*

            type = "table";
            expectedTraits = StructTraits();

            actualTraits = traits(type);

            testCase.verifyEqual(actualTraits, expectedTraits);
        end

        function TestErrorIfUnsupportedMatlabClass(testCase)
            import arrow.type.traits.*

            type = "not-a-class";

            testCase.verifyError(@() traits(type), "arrow:type:traits:UnsupportedMatlabClass");
        end

        function TestErrorIfUnsupportedInputType(testCase)
            import arrow.type.traits.*

            type = 123;
            testCase.verifyError(@() traits(type), "arrow:type:traits:UnsupportedInputType");

            type = {'double'};
            testCase.verifyError(@() traits(type), "arrow:type:traits:UnsupportedInputType");

            type = datetime(2023, 1, 1);
            testCase.verifyError(@() traits(type), "arrow:type:traits:UnsupportedInputType");
        end

    end

end

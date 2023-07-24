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

classdef tField < matlab.unittest.TestCase
% Test class for arrow.type.Field and arrow.field.

    methods(Test)
        function TestBasic(testCase)
            name = "A";
            type = arrow.type.uint64;
            field = arrow.field(name, type);

            testCase.verifyEqual(field.Name, name);
            testCase.verifyEqual(field.Type.ID, type.ID);
        end

        function TestSupportedTypes(testCase)
            name = "name";
            supportedTypes = { ...
                                 arrow.type.uint8, ...
                                 arrow.type.uint16, ...
                                 arrow.type.uint32, ...
                                 arrow.type.uint64, ...
                                 arrow.type.int8, ...
                                 arrow.type.int16, ...
                                 arrow.type.int32, ...
                                 arrow.type.int64, ...
                                 arrow.type.boolean, ...
                                 arrow.type.float32, ...
                                 arrow.type.float64, ...
                                 arrow.type.string, ...
                                 arrow.type.timestamp, ...
                             };
            for ii = 1:numel(supportedTypes)
                supportedType = supportedTypes{ii};
                field = arrow.field(name, supportedType);
                testCase.verifyEqual(field.Name, name);
                testCase.verifyEqual(field.Type.ID, supportedType.ID);
            end
        end

        function TestNameUnicode(testCase)
            smiley = "😀";
            tree =  "🌲";
            mango = "🥭";

            type = arrow.type.uint64;
            field = arrow.field(smiley, type);

            testCase.verifyEqual(field.Name, smiley);
            testCase.verifyEqual(field.Type.ID, type.ID);

            field = arrow.field(tree, type);

            testCase.verifyEqual(field.Name, tree);
            testCase.verifyEqual(field.Type.ID, type.ID);

            field = arrow.field(mango, type);

            testCase.verifyEqual(field.Name, mango);
            testCase.verifyEqual(field.Type.ID, type.ID);
        end

        function TestErrorIfNameStringMissing(testCase)
            name = string(missing);
            type = arrow.type.uint64;
            testCase.verifyError(@() arrow.field(name, type), "MATLAB:validators:mustBeNonmissing");
        end

        function TestNameEmptyString(testCase)
            name = "";
            type = arrow.type.uint64;
            field = arrow.field(name, type);

            testCase.verifyEqual(field.Name, name);
            testCase.verifyEqual(field.Type.ID, type.ID);
        end

        function TestNameCharVector(testCase)
            name = 'ABC';
            type = arrow.type.uint64;
            field = arrow.field(name, type);

            testCase.verifyEqual(field.Name, string(name));
            testCase.verifyEqual(field.Type.ID, type.ID);
        end

        function TestNameNumber(testCase)
            name = 123;
            type = arrow.type.uint64;
            field = arrow.field(name, type);

            testCase.verifyEqual(field.Name, string(123));
            testCase.verifyEqual(field.Type.ID, type.ID);
        end

        function TestArrowTypeUnsupportedInput(testCase)
            name = "A";
            type = { 123 };
            testCase.verifyError(@() arrow.field(name, type), "MATLAB:validation:UnableToConvert");
        end

        function TestNameUnsupportedInput(testCase)
            name = table();
            type = arrow.type.uint64;
            testCase.verifyError(@() arrow.field(name, type), "MATLAB:validation:UnableToConvert");
        end

        function TestImmutableProperties(testCase)
            name = "A";
            type = arrow.type.uint64;
            field = arrow.field(name, type);

            testCase.verifyError(@() setfield(field, "Name", "NewValue"), "MATLAB:class:noSetMethod")
            testCase.verifyError(@() setfield(field, "Type", arrow.type.boolean), "MATLAB:class:noSetMethod")
        end

    end
end

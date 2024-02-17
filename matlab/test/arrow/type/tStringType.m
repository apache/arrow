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

classdef tStringType < matlab.unittest.TestCase
%TSTRINGTYPE Test class for arrow.type.StringType

    methods (Test)

        function Basic(tc)
            type = arrow.string;
            className = string(class(type));
            tc.verifyEqual(className, "arrow.type.StringType");
            tc.verifyEqual(type.ID, arrow.type.ID.String);
        end

        function NumFields(tc)
            type = arrow.string;
            tc.verifyEqual(type.NumFields, int32(0));
        end

        function IsEqualTrue(testCase)
            % Verifies isequal method of arrow.type.StringType returns true if
            % these conditions are met:
            %
            % 1. All input arguments have a class type arrow.type.StringType
            % 2. All inputs have the same size

            % Scalar StringType arrays
            stringType1 = arrow.string();
            stringType2 = arrow.string();
            testCase.verifyTrue(isequal(stringType1, stringType2));

            % Non-scalar StringType arrays
            typeArray1 = [stringType1 stringType1];
            typeArray2 = [stringType2 stringType2];
            testCase.verifyTrue(isequal(typeArray1, typeArray2));
        end

        function IsEqualFalse(testCase)
            % Verifies the isequal method of arrow.type.StringType returns
            % false when expected.
            
            % Pass a different arrow.type.Type subclass to isequal
            stringType = arrow.string();
            int32Type = arrow.int32();
            testCase.verifyFalse(isequal(stringType, int32Type));
            testCase.verifyFalse(isequal([stringType stringType], [int32Type int32Type]));

            % StringType arrays have different sizes
            typeArray1 = [stringType stringType];
            typeArray2 = [stringType stringType]';
            testCase.verifyFalse(isequal(typeArray1, typeArray2));
        end

        function TestFieldsProperty(testCase)
            % Verify Fields is a 0x0 arrow.type.Field array.
            type = arrow.string();
            fields = type.Fields;
            testCase.verifyEqual(fields, arrow.type.Field.empty(0, 0));
        end

        function FieldsNoSetter(testCase)
            % Verify the Fields property is not settable.
            type = arrow.string();
            testCase.verifyError(@() setfield(type, "Fields", "1"), "MATLAB:class:SetProhibited");
        end

        function InvalidFieldIndex(testCase)
            % Verify the field() method throws the expected error message
            % when given an invalid index.
            type = arrow.string();

            testCase.verifyError(@() type.field(0), "arrow:badsubscript:NonPositive");
            testCase.verifyError(@() type.field("A"), "arrow:badsubscript:NonNumeric");

            % NOTE: For StringType, Fields is always empty.
            testCase.verifyError(@() type.field(1), "arrow:index:EmptyContainer");
        end

    end

end


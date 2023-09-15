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

classdef hFixedWidthType < matlab.unittest.TestCase
% Test class that defines shared unit tests for classes that inherit from
% arrow.type.FixedWidthType

    properties(Abstract)
        ArrowType
        TypeID
        BitWidth
        ClassName
    end

    methods(Test)
        function TestClass(testCase)
            % Verify ArrowType is an object of the expected class type.
            name = string(class(testCase.ArrowType));
            testCase.verifyEqual(name, testCase.ClassName);
        end

        function TestTypeID(testCase)
            % Verify ID is set to the appropriate arrow.type.ID value.
            arrowType = testCase.ArrowType;
            testCase.verifyEqual(arrowType.ID, testCase.TypeID);
        end

        function TestBitWidth(testCase)
            % Verify the BitWidth value.
            arrowType = testCase.ArrowType;
            testCase.verifyEqual(arrowType.BitWidth, testCase.BitWidth);
        end

        function TestNumFields(testCase)
            % Verify NumFields is set to 0 for primitive types.
            arrowType = testCase.ArrowType;
            testCase.verifyEqual(arrowType.NumFields, int32(0));
        end

        function TestFieldsProperty(testCase)
            % Verify Fields is a 0x0 arrow.type.Field array.
            type = testCase.ArrowType;
            fields = type.Fields;
            testCase.verifyEqual(fields, arrow.type.Field.empty(0, 0));
        end

        function FieldsNoSetter(testCase)
            % Verify the Fields property is not settable.
            type = testCase.ArrowType;
            testCase.verifyError(@() setfield(type, "Fields", "1"), "MATLAB:class:SetProhibited");
        end

        function InvalidFieldIndex(testCase)
            % Verify the field() method throws the expected error message
            % when given an invalid index.
            type = testCase.ArrowType;

            testCase.verifyError(@() type.field(0), "arrow:badsubscript:NonPositive");
            testCase.verifyError(@() type.field("A"), "arrow:badsubscript:NonNumeric");

            % NOTE: For FixedWidthTypes, Fields is always empty.
            testCase.verifyError(@() type.field(1), "arrow:index:EmptyContainer");
        end

        function TestBitWidthNoSetter(testCase)
            % Verify that an error is thrown when trying to set the value
            % of the BitWidth property.
            arrowType = testCase.ArrowType;
            testCase.verifyError(@() setfield(arrowType, "BitWidth", 64), "MATLAB:class:SetProhibited");
        end

        function TestIDNoSetter(testCase)
            % Verify that an error is thrown when trying to set the value
            % of the ID property.
            arrowType = testCase.ArrowType;
            testCase.verifyError(@() setfield(arrowType, "ID", 15), "MATLAB:class:SetProhibited");
        end

        function TestNumFieldsNoSetter(testCase)
            % Verify that an error is thrown when trying to set the value
            % of the NumFields property.
            arrowType = testCase.ArrowType;
            testCase.verifyError(@() setfield(arrowType, "NumFields", 2), "MATLAB:class:SetProhibited");
        end

    end

end

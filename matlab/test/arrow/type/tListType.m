% TLISTTYPE Tests for arrow.type.ListType

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

classdef tListType < matlab.unittest.TestCase

    properties (Constant)
        BasicList = arrow.list(arrow.int8())
        NestedList = arrow.list(arrow.list(arrow.list(arrow.uint64())))
        ConstructionFcn = @arrow.list
        TypeID = arrow.type.ID.List
        ClassName = "arrow.type.ListType"
    end

    methods (Test)

        function ConstructionFcnBasic(testCase)
            % Verify construction function returns an instance of the
            % expected arrow.type.Type subclass.
            type = testCase.BasicList;
            testCase.verifyInstanceOf(type, testCase.ClassName);
        end

        function ConstructionFcnTooFewInputsError(testCase)
            % Verify construction function errors if given zero input arguments.
            fcn = @() testCase.ConstructionFcn();
            testCase.verifyError(fcn, "MATLAB:minrhs");
        end

        function ConstructionFcnInvalidInputTypeError(testCase)
            % Verify construction function errors if any one of the input
            % arguments is not an arrow.type.Type object.
            fcn = @() testCase.ConstructionFcn("abc");
            testCase.verifyError(fcn, "MATLAB:validation:UnableToConvert");
        end

        function ConstructionFcnEmptyTypeError(testCase)
            % Verify construction function errors if given an empty
            % arrow.type.Type array as one of its inputs.
            fcn = @() testCase.ConstructionFcn(arrow.type.Type.empty(0, 0));
            testCase.verifyError(fcn, "MATLAB:validation:IncompatibleSize");
        end

        function TypeGetter(testCase)
            % Verify the Type property getter returns the expected value.
            type = testCase.BasicList;
            testCase.verifyEqual(type.Type, arrow.int8());

            type = testCase.NestedList;
            testCase.verifyEqual(type.Type, arrow.list(arrow.list(arrow.uint64())));
        end

        function TypeNoSetter(testCase)
            % Verify the Type property is not settable.
            type = testCase.BasicList;
            fcn = @() setfield(type, "Type", arrow.string());
            testCase.verifyError(fcn, "MATLAB:class:SetProhibited");
        end

        function IDGetter(testCase)
            % Verify the ID property getter returns the expected enum value.
            type = testCase.BasicList;
            actual = type.ID;
            expected = testCase.TypeID;
            testCase.verifyEqual(actual, expected);
        end

        function IDNoSetter(testCase)
            % Verify the ID property is not settable.
            type = testCase.BasicList;
            fcn = @() setfield(type, "ID", arrow.type.ID.Boolean);
            testCase.verifyError(fcn, "MATLAB:class:SetProhibited");
        end

        function IsEqualTrue(testCase)
            % Verify two ListTypes are considered equal if their Type
            % properties are equal.

            type1 = arrow.list(arrow.list(arrow.string()));
            type2 = arrow.list(arrow.list(arrow.string()));
            testCase.verifyTrue(isequal(type1, type2));

            % Non-scalar arrow.type.ListType arrays
            type3 = [type1 type2];
            type4 = [type1 type2];
            testCase.verifyTrue(isequal(type3, type4));
        end

        function IsEqualFalse(testCase)
            % Verify isequal returns false when expected.
            type1 = arrow.list(arrow.time32());
            type2 = arrow.list(arrow.time64());
            type3 = arrow.list(arrow.timestamp());
            type4 = arrow.list(arrow.list(arrow.timestamp()));

            % Type properties are different.
            testCase.verifyFalse(isequal(type1, type2));
            testCase.verifyFalse(isequal(type3, type4));

            % Non-scalar arrow.type.ListType arrays
            type5 = [type1 type2];
            type6 = [type1; type2];
            type7 = [type3 type4];
            type8 = [type4 type3];
            testCase.verifyFalse(isequal(type5, type6));
            testCase.verifyFalse(isequal(type7, type8));
        end

    end

end
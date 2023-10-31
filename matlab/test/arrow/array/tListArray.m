%TLISTARRAY Tests for arrow.array.ListArray

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

classdef tListArray < matlab.unittest.TestCase

    properties (Constant)
        Traits = arrow.type.traits.traits(arrow.type.ID.List)
    end

    properties (TestParameter)
        TestArrowArray
        TestValidationModeArray
    end

    methods (TestParameterDefinition, Static)

        function TestArrowArray = initializeTestArrowArray()
            %% Empty (zero-element) list (List<Float64>)
            Type = arrow.list(arrow.float64());
            NumElements = int64(0);
            Valid = logical.empty(0, 1);
            Offsets = arrow.array(int32(0));
            Values = arrow.array([]);
            ArrowArray = arrow.array.ListArray.fromArrays(Offsets, Values, Valid=Valid);
            MatlabArray = {cell.empty(0, 1)};

            TestArrowArray.EmptyList = struct( ...
                ArrowArray=ArrowArray, ...
                MatlabArray=MatlabArray, ...
                Properties=struct(...
                    Type=Type, ...
                    NumElements=NumElements, ...
                    Valid=Valid, ...
                    Offsets=Offsets, ...
                    Values=Values ...
                ) ...
            );

            %% List with NULLs (List<String>)
            Type = arrow.list(arrow.string());
            NumElements = int64(4);
            Valid = [true, false, true, false];
            Offsets = arrow.array(int32([0, 1, 4, 6, 7]));
            Values = arrow.array(["A", missing, "C", "D", "E", missing, "G"]);
            ArrowArray = arrow.array.ListArray.fromArrays(Offsets, Values, Valid=Valid);
            MatlabArray = {{"A"; missing; ["E"; missing]; missing}};

            TestArrowArray.NullList = struct( ...
                ArrowArray=ArrowArray, ...
                MatlabArray=MatlabArray, ...
                Properties=struct(...
                    Type=Type, ...
                    NumElements=NumElements, ...
                    Valid=Valid, ...
                    Offsets=Offsets, ...
                    Values=Values ...
                ) ...
            );

            %% Single-level list (List<Float64>)
            Type = arrow.list(arrow.float64());
            NumElements = int64(3);
            Valid = true(1, NumElements);
            Offsets = arrow.array(int32([0, 2, 5, 9]));
            Values = arrow.array([1, 2, 3, 4, 5, 6, 7, 8, 9]);
            ArrowArray = arrow.array.ListArray.fromArrays(Offsets, Values, Valid=Valid);
            MatlabArray = {{[1; 2]; [3; 4; 5]; [6; 7; 8; 9]}};

            TestArrowArray.SingleLevelList = struct( ...
                ArrowArray=ArrowArray, ...
                MatlabArray=MatlabArray, ...
                Properties=struct(...
                    Type=Type, ...
                    NumElements=NumElements, ...
                    Valid=Valid, ...
                    Offsets=Offsets, ...
                    Values=Values ...
                ) ...
            );

            %% Multi-level list (List<List<Float64>>)
            Type = arrow.list(arrow.list(arrow.float64()));
            NumElements = int64(2);
            Valid = true(1, NumElements);
            Offsets = arrow.array(int32([0, 1, 3]));
            Values = TestArrowArray.SingleLevelList.ArrowArray;
            ArrowArray = arrow.array.ListArray.fromArrays(Offsets, Values, Valid=Valid);
            MatlabArray = {{{[1; 2]}; {[3; 4; 5]; [6; 7; 8; 9]}}};

            TestArrowArray.MultiLevelList = struct( ...
                ArrowArray=ArrowArray, ...
                MatlabArray=MatlabArray, ...
                Properties=struct(...
                    Type=Type, ...
                    NumElements=NumElements, ...
                    Valid=Valid, ...
                    Offsets=Offsets, ...
                    Values=Values ...
                ) ...
            );
        end

        function TestValidationModeArray = initializeTestValidationModeArray()
            %% Valid ListArray
            Offsets = arrow.array(int32([0, 1, 2, 3]));
            Values = arrow.array([1, 2, 3]);

            TestValidationModeArray.ValidList = struct( ...
                Offsets=Offsets, ...
                Values=Values, ...
                Valid=true ...
            );

            %% Invalid ListArray
            % Incorrect number of offsets (length should be 1 more than the number of Values).
            Offsets = arrow.array(int32([0, 1, 2, 3, 4, 5]));
            Values = arrow.array([1, 2, 3]);

            TestValidationModeArray.InvalidList = struct( ...
                Offsets=Offsets, ...
                Values=Values, ...
                Valid=false ...
            );
        end

    end

    methods (Test)

        function TestClass(testCase, TestArrowArray)
            % Verify that the arrow.array.Array has the expected class.
            testCase.verifyInstanceOf(TestArrowArray.ArrowArray, testCase.Traits.ArrayClassName);
        end

        function TestProperties(testCase, TestArrowArray)
            % Verify that all properties of the arrow.array.Array:
            %
            % 1. Return the expected value
            % 2. Cannot be modified (i.e. are read-only).
            %
            properties = string(fieldnames(TestArrowArray.Properties));
            for ii = numel(properties)
                property = properties(ii);
                expected = TestArrowArray.Properties.(property);
                actual = getfield(TestArrowArray.ArrowArray, property);
                % Verify that the property returns the expected value.
                testCase.verifyEqual(actual, expected);
                fcn = @() setfield(TestArrowArray.ArrowArray, property, "NewValue");
                % Verify that the property cannot be modified (i.e. that it
                % is read-only).
                testCase.verifyError(fcn, "MATLAB:class:SetProhibited");
            end
        end

        function TestToMatlab(testCase, TestArrowArray)
            % Verify that the toMATLAB method returns the
            % expected MATLAB array.
            actual = TestArrowArray.ArrowArray.toMATLAB();
            expected = TestArrowArray.MatlabArray;
            testCase.verifyEqual(actual, expected);
        end

        function TestErrorIfEmptyOffsets(testCase)
            % Verify that an arrow:array:ListArrayFromArraysFailed error
            % is thrown if an empty Offsets array is provided to the
            % arrow.array.ListArray.fromArrays.
            offsets = arrow.array(int32.empty(0, 0));
            values = arrow.array([1, 2, 3]);
            fcn = @() arrow.array.ListArray.fromArrays(offsets, values);
            testCase.verifyError(fcn, "arrow:array:ListArrayFromArraysFailed");
        end

        function TestValidationModeDefault(testCase, TestValidationModeArray)
            % Verify that the default ValidationMode value for the
            % arrow.array.ListArray.fromArrays method is
            % arrow.array.ValidationMode.Minimal.
            offsets = TestValidationModeArray.Offsets;
            values = TestValidationModeArray.Values;
            valid = TestValidationModeArray.Valid;
            fcn = @() arrow.array.ListArray.fromArrays(offsets, values);
            if valid
                testCase.verifyWarningFree(fcn);
            else
                testCase.verifyError(fcn, "arrow:array:ValidateMinimalFailed");
            end
        end

        function TestValidationModeNone(testCase, TestValidationModeArray)
            % Verify that no error is thrown when supplying the
            % ValidatationMode name-value pair, with a value of
            % arrow.array.ValidationMode.None, to the
            % arrow.array.ListArray.fromArrays method.
            offsets = TestValidationModeArray.Offsets;
            values = TestValidationModeArray.Values;
            validationMode = arrow.array.ValidationMode.None;
            fcn = @() arrow.array.ListArray.fromArrays(offsets, values, ValidationMode=validationMode);
            testCase.verifyWarningFree(fcn);
        end

        function TestValidationModeMinimal(testCase, TestValidationModeArray)
            % Verify that an error of type arrow:array:ValidateMinimalFailed
            % is thrown when supplying the ValidatationMode name-value pair,
            % with a value of arrow.array.ValidationMode.Minimal, to the
            % arrow.array.ListArray.fromArrays method, if the provided offsets
            % and values arrays are invalid.
            offsets = TestValidationModeArray.Offsets;
            values = TestValidationModeArray.Values;
            valid = TestValidationModeArray.Valid;
            validationMode = arrow.array.ValidationMode.Minimal;
            fcn = @() arrow.array.ListArray.fromArrays(offsets, values, ValidationMode=validationMode);
            if valid
                testCase.verifyWarningFree(fcn);
            else
                testCase.verifyError(fcn, "arrow:array:ValidateMinimalFailed");
            end
        end

        function TestValidationModeFull(testCase, TestValidationModeArray)
            % Verify that an error of type arrow:array:ValidateFullFailed
            % is thrown when supplying the ValidatationMode name-value pair,
            % with a value of arrow.array.ValidationMode.Full, to the
            % arrow.array.ListArray.fromArrays method, if the provided offsets
            % and values arrays are invalid.
            offsets = TestValidationModeArray.Offsets;
            values = TestValidationModeArray.Values;
            validationMode = arrow.array.ValidationMode.Full;
            valid = TestValidationModeArray.Valid;
            fcn = @() arrow.array.ListArray.fromArrays(offsets, values, ValidationMode=validationMode);
            if valid
                testCase.verifyWarningFree(fcn);
            else
                testCase.verifyError(fcn, "arrow:array:ValidateFullFailed");
            end
        end

        function TestValidationModeUnsupportedEnum(testCase)
            % Verify that an error of type arrow:array:ValidateUnsupportedEnum
            % is thrown when an unsupported integer enumeration value is
            % supplied for the ValidatationMode parameter to the internal
            % C++ ListArray Proxy validate method.
            offsets = arrow.array.Int32Array.fromMATLAB(int32([0, 1, 2]));
            values = arrow.array.Float64Array.fromMATLAB([1, 2, 3]);
            array = arrow.array.ListArray.fromArrays(offsets, values);
            % Get the underlying Proxy instance from the ListArray.
            proxy = array.Proxy;
            % Call the internal Proxy method "validate" with an unsupported
            % integer ValidationMode value.
            validationMode = uint8(3);
            args = struct(ValidationMode=validationMode);
            fcn = @() proxy.validate(args);
            testCase.verifyError(fcn, "arrow:array:ValidateUnsupportedEnum");
        end

    end

end

%TCLASSTYPEVALIDATOR Unit tests for arrow.array.internal.list.ClassTypeValidator

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

classdef tClassTypeValidator < matlab.unittest.TestCase

    methods (Test)
        function Smoke(testCase)
            import arrow.array.internal.list.ClassTypeValidator
            validator = ClassTypeValidator("Sample Data");
            testCase.verifyInstanceOf(validator, "arrow.array.internal.list.ClassTypeValidator");
        end

        function ClassNameGetter(testCase)
            % Verify the ClassName getter returns the expected scalar
            % string.
            import arrow.array.internal.list.ClassTypeValidator

            validator = ClassTypeValidator("Sample Data");
            testCase.verifyEqual(validator.ClassName, "string");
        end

        function ClassNameNoSetter(testCase)
            % Verify ClassName property is not settable.
            import arrow.array.internal.list.ClassTypeValidator

            validator = ClassTypeValidator(1);
            fcn = @() setfield(validator, "ClassName", "duration");
            testCase.verifyError(fcn, "MATLAB:class:SetProhibited");
        end

        function ValidateElementNoThrow(testCase) %#ok<MANU>
            % Verify validateElement does not throw an exception
            % if class type of the input element matches the ClassName
            % property value.
            import arrow.array.internal.list.ClassTypeValidator

            validator = ClassTypeValidator(1);
            validator.validateElement(2);
            validator.validateElement([1 2 3]);
            validator.validateElement([1; 2; 3; 3]);
            validator.validateElement([5 6; 7 8]);
            validator.validateElement(double.empty(0, 1));
        end

        function ValidateElementClassTypeMismatchError(testCase)
            % Verify validateElement throws an exception whose identifier
            % is "arrow:array:list:ClassTypeMismatch" if the input
            % element's class type does not match the ClassName property
            % value.
            import arrow.array.internal.list.ClassTypeValidator

            % validator will expect all elements to be of type double, since "1" is a double.
            validator = ClassTypeValidator(1);
            errorID = "arrow:array:list:ClassTypeMismatch";
            testCase.verifyError(@() validator.validateElement("A"), errorID);
            testCase.verifyError(@() validator.validateElement(uint8([1 2])), errorID);
            testCase.verifyError(@() validator.validateElement(datetime(2023, 1, 1)), errorID);
        end

        function GetElementLength(testCase)
            % Verify getElementLength returns the expected length values
            % for the given input arrays.
            import arrow.array.internal.list.ClassTypeValidator

            validator = ClassTypeValidator(1);
            testCase.verifyEqual(validator.getElementLength(2), 1);
            testCase.verifyEqual(validator.getElementLength([1 2; 3 4]), 4);
            testCase.verifyEqual(validator.getElementLength(double.empty(1, 0)), 0);
        end

        function ReshapeCellElements(testCase)
            % Verify reshapeCellElements reshapes all elements in the input
            % cell array into column vectors.
            import arrow.array.internal.list.ClassTypeValidator

            validator = ClassTypeValidator(1);
            C = {[1 2 3], [4; 5], [6 7; 8 9], double.empty(1, 0), 10};
            act = validator.reshapeCellElements(C);
            exp = {[1; 2; 3], [4; 5], [6; 8; 7; 9], double.empty(0, 1), 10};
            testCase.verifyEqual(act, exp);
        end

    end

end
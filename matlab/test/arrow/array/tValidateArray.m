%TVALIDATEARRAY Test class for the arrow.array.Array/validate() method.

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

classdef tValidateArray < matlab.unittest.TestCase

    methods (Test)

        function InvalidModeInput(test)
            % Verify arrow.array.Array/validate() throws an exception if
            % provided an invalid value for the Mode name-value 
            % pair.
            array = arrow.array.Float64Array.fromMATLAB(1:5);

            % Cannot convert "abc" to a ValidationMode value
            fcn = @() array.validate(Mode="abc");
            test.verifyError(fcn, "MATLAB:validation:UnableToConvert");

            % Mode must be scalar
            modes = [arrow.array.ValidationMode.Full arrow.array.ValidationMode.Minimal];
            fcn = @() array.validate(Mode=modes);
            test.verifyError(fcn, "MATLAB:validation:IncompatibleSize");

            % ValidationMode.None is not supported
            mode = arrow.array.ValidationMode.None;
            fcn = @() array.validate(Mode=mode);
            test.verifyError(fcn, "arrow:array:InvalidValidationMode");
        end

        function ValidationModeMinimalFails(test)
            % Verify arrow.array.Array/validate() throws an exception 
            % with the ID arrow:array:ValidateMinimalFailed if
            % Mode="Minimal" and the array fails the "Minimal"
            % validation checks.
            offsets = arrow.array(int32([0 1 3 4 5]));
            values = arrow.array([1 2 3]);
            array = arrow.array.ListArray.fromArrays(offsets, values, ValidationMode="None");
            fcn = @() array.validate(Mode="Minimal");
            test.verifyError(fcn, "arrow:array:ValidateMinimalFailed")
        end

        function ValidationModeMinimalPasses(test)
            % Verify arrow.array.Array/validate() does not throw an
            % exception if Mode="Minimal" and the array passes the
            % "Minimal" validation checks.
            offsets = arrow.array(int32([0 1 0]));
            values = arrow.array([1 2 3]);
            % NOTE: the array is actually invalid, but it passes the
            % "Minimal" validation checks.
            array = arrow.array.ListArray.fromArrays(offsets, values);
            fcn = @() array.validate(Mode="Minimal");
            test.verifyWarningFree(fcn, "arrow:array:ValidateMinimalFailed")
        end

        function ValidationModeFullFails(test)
            % Verify arrow.array.Array/validate() throws an exception 
            % with the ID arrow:array:ValidateFullFailed if
            % Mode="Full" and the array fails the "Full"
            % validation checks.
            offsets = arrow.array(int32([0 1 0]));
            values = arrow.array([1 2 3]);
            array = arrow.array.ListArray.fromArrays(offsets, values);
            fcn = @() array.validate(Mode="Full");
            test.verifyError(fcn, "arrow:array:ValidateFullFailed")
        end

        function ValidationModeFullPasses(test)
            % Verify arrow.array.Array/validate() does not throw an
            % exception if Mode="Full" and the array passes
            % the "full" validation checks.
            offsets = arrow.array(int32([0 1 3]));
            values = arrow.array([1 2 3]);
            array = arrow.array.ListArray.fromArrays(offsets, values);
            fcn = @() array.validate(Mode="Full");
            test.verifyWarningFree(fcn);
        end

        function DefaultValidationModeIsMimimal(test)
            % Verify the default Mode value is "Minimal".
            offsets = arrow.array(int32([0 1 2 3]));
            values = arrow.array([1 2 3]);
            array = arrow.array.ListArray.fromArrays(offsets, values);
            fcn = @() array.validate();
            test.verifyWarningFree(fcn, "arrow:array:ValidateMinimalFailed")
        end
    end

end
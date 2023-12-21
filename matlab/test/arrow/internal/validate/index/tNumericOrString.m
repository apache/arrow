%tNUMERICORSTRING Unit tests for
% arrow.internal.validate.index.numericOrString.

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

classdef tNumericOrString < matlab.unittest.TestCase

    methods (Test)

        function ValidNumericIndex(testCase)
            % Verify numericOrString() returns the expected index array
            % when given a valid numeric index array.

            import arrow.internal.validate.index.numericOrString

            original = 1;
            expected = int32(1);
            actual = numericOrString(original, "int32");
            testCase.verifyEqual(actual, expected);

            original = [1 2 5];
            expected = int32([1 2 5])';
            actual = numericOrString(original, "int32");
            testCase.verifyEqual(actual, expected);
        end

        function InvalidNumericIndexError(testCase)
            % Verify numericOrString() errors if given an invalid numeric
            % index array.

            import arrow.internal.validate.index.numericOrString

            fcn = @() numericOrString(-1.1, "int8");
            testCase.verifyError(fcn, "arrow:badsubscript:NonPositive");

            fcn = @() numericOrString([2 -1.1], "int8");
            testCase.verifyError(fcn, "arrow:badsubscript:NonPositive");
        end

        function ValidStringArray(testCase)
            % Verify numericOrString() returns the expected index array if
            % given a valid string index array.

            import arrow.internal.validate.index.numericOrString

            testCase.verifyEqual(numericOrString("A", "int32"), "A");

            testCase.verifyEqual(numericOrString(["B" "A"], "int32"), ["B", "A"]');
        end

        function AllowNonScalarTrue(testCase)
            % Verify numericOrString() behaves as expected provided
            % AllowNonScalar=true.

            import arrow.internal.validate.index.numericOrString
            
            % Provide a nonscalar double array
            original = [1 2 3]';
            expected = int32([1 2 3])';
            actual = numericOrString(original, "int32", AllowNonScalar=true);
            testCase.verifyEqual(actual, expected);

            % Provide a scalar double array
            original = 1;
            expected = int32(1);
            actual = numericOrString(original, "int32", AllowNonScalar=true);
            testCase.verifyEqual(actual, expected);

            % Provide a nonscalar string array
            original = ["A", "B", "C"];
            expected = ["A", "B", "C"]';
            actual = numericOrString(original, "int32", AllowNonScalar=true);
            testCase.verifyEqual(actual, expected);

            % Provide a scalar string array
            original = "A";
            expected = "A";
            actual = numericOrString(original, "int32", AllowNonScalar=true);
            testCase.verifyEqual(actual, expected);
        end

        function AllowNonScalarFalse(testCase)
            % Verify numericOrString() behaves as expected when provided
            % AllowNonScalar=false.

            import arrow.internal.validate.index.numericOrString
            
            % Should throw an error when provided a nonscalar double array
            original = [1 2 3]';
            fcn = @() numericOrString(original, "int32", AllowNonScalar=false);
            testCase.verifyError(fcn, "arrow:badsubscript:NonScalar");

            % Should not throw an error when provided a scalar double array
            original = 1;
            expected = int32(1);
            actual = numericOrString(original, "int32", AllowNonScalar=true);
            testCase.verifyEqual(actual, expected);

            % Should throw an error if provided a nonscalar string array
            original = ["A", "B", "C"];
            fcn = @() numericOrString(original, "int32", AllowNonScalar=false);
            testCase.verifyError(fcn, "arrow:badsubscript:NonScalar");

            % Should not throw an error if provided a scalar string array
            original = "A";
            expected = "A";
            actual = numericOrString(original, "int32", AllowNonScalar=false);
            testCase.verifyEqual(actual, expected);

            % Should not throw an error if provided a character row vector
            original = 'ABC';
            expected = "ABC";
            actual = numericOrString(original, "int32", AllowNonScalar=false);
            testCase.verifyEqual(actual, expected);
        end
    end
end
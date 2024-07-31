%TNUMERIC Unit tests for arrow.internal.validate.index.numeric

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

classdef tNumeric < matlab.unittest.TestCase

    methods (Test)

        function NonPositiveError(testCase)
            % Verify numeric() throws an error whose identifier is 
            % "arrow:badsubscript:NonPositive" if the index array provided
            % has non-positive values.

            import arrow.internal.validate.index.numeric

            errid = "arrow:badsubscript:NonPositive";

            fcn = @() numeric(0, "int32");
            testCase.verifyError(fcn, errid);

            fcn = @() numeric(-1, "int32");
            testCase.verifyError(fcn, errid);

            fcn = @() numeric([1 -1 2], "int32");
            testCase.verifyError(fcn, errid);
        end

        function NonIntegerError(testCase)
            % Verify numeric() throws an error whose identifier is 
            % "arrow:badsubscript:NonInteger" if the index array provided
            % has non-integer values.

            import arrow.internal.validate.index.numeric

            errid = "arrow:badsubscript:NonInteger";

            fcn = @() numeric(1.1, "int32");
            testCase.verifyError(fcn, errid);

            fcn = @() numeric(NaN, "int32");
            testCase.verifyError(fcn, errid);

            fcn = @() numeric(inf, "int32");
            testCase.verifyError(fcn, errid);

            fcn = @() numeric([1 1.2 2], "int32");
            testCase.verifyError(fcn, errid);
        end

        function NonRealError(testCase)
            % Verify numeric() throws an error whose identifier is 
            % "arrow:badsubscript:NonReal" if the index array is
            % complex.

            import arrow.internal.validate.index.numeric

            errid = "arrow:badsubscript:NonReal";

            fcn = @() numeric(1 + 1i, "int32");
            testCase.verifyError(fcn, errid);

            fcn = @() numeric([1 2 + 2i], "int32");
            testCase.verifyError(fcn, errid);
        end

        function ExceedsIntMaxError(testCase)
            % Verify numeric() throws an error whose identifier is 
            % "arrow:badsubscript:ExceedsIntMax" if the index array 
            % provided has values that exceed the intmax(intType).

            import arrow.internal.validate.index.numeric

            errid = "arrow:badsubscript:ExceedsIntMax";

            fcn = @() numeric(flintmax("double"), "int32");
            testCase.verifyError(fcn, errid);

            fcn = @() numeric([1 flintmax("double")], "int32");
            testCase.verifyError(fcn, errid);
        end

        function CastToIntType(testCase)
            % Verify numeric() returns an index array of the provided
            % intType if the index array is valid.
            
            import arrow.internal.validate.index.numeric

            original = [1 2 3 4];
            expected = int32(original)';
            actual = numeric(original, "int32");
            testCase.verifyEqual(actual, expected);

            original = uint32([1 2 3 4]);
            expected = int64(original)';
            actual = numeric(original, "int64");
            testCase.verifyEqual(actual, expected);
        end

        function ConvertSparseToFullStorage(testCase)
            % Verify numeric() converts sparse index arrays into full
            % storage arrays.
            
            import arrow.internal.validate.index.numeric

            original = sparse([1 2 3 4]);
            expected = int32(full(original))';
            actual = numeric(original, "int32");
            testCase.verifyEqual(actual, expected);
        end

        function ErrorIfNonNumeric(testCase)
            % Verify numeric() throws an error whose identifier is 
            % "arrow:badsubscript:NonNumeric" if provided a non-numeric
            % array as the index.

            import arrow.internal.validate.index.numeric

            fcn = @() numeric(false, "int32");
            testCase.verifyError(fcn, "arrow:badsubscript:NonNumeric");
        end

        function OutputShape(testCase)
            % Verify numeric() always returns a column vector.

            import arrow.internal.validate.index.numeric

            % Provide a 2x2 matrix
            original = int32([1 2; 3 4]);
            expected = int32([1 3 2 4])';
            actual = numeric(original, "int32");
            testCase.verifyEqual(actual, expected);

            % Provide a 1x3 vector
            original = int32([1 2 3]);
            expected = int32([1 2 3])';
            actual = numeric(original, "int32");
            testCase.verifyEqual(actual, expected);

            % Provide a 3x1 vector
            original = int32([1 2 3])';
            expected = int32([1 2 3])';
            actual = numeric(original, "int32");
            testCase.verifyEqual(actual, expected);

            % Provide a 2x2x2 N-dimensional array
            original = int8(reshape(1:8, 2, 2, 2));
            expected = int32(1:8)';
            actual = numeric(original, "int32");
            testCase.verifyEqual(actual, expected);
        end

        function AllowNonScalarTrue(testCase)
            % Verify numeric() behaves as expected provided
            % AllowNonScalar=true.

            import arrow.internal.validate.index.numeric
            
            % Provide a nonscalar array
            original = [1 2 3]';
            expected = int32([1 2 3])';
            actual = numeric(original, "int32", AllowNonScalar=true);
            testCase.verifyEqual(actual, expected);

            % Provide a scalar array
            original = 1;
            expected = int32(1);
            actual = numeric(original, "int32", AllowNonScalar=true);
            testCase.verifyEqual(actual, expected);
        end

        function AllowNonScalarFalse(testCase)
            % Verify numeric() behaves as expected when provided
            % AllowNonScalar=false.

            import arrow.internal.validate.index.numeric
            
            % Should throw an error when provided a nonscalar double array
            original = [1 2 3]';
            fcn = @() numeric(original, "int32", AllowNonScalar=false);
            testCase.verifyError(fcn, "arrow:badsubscript:NonScalar");

            % Should not throw an error when provided a scalar double array
            original = 1;
            expected = int32(1);
            actual = numeric(original, "int32", AllowNonScalar=true);
            testCase.verifyEqual(actual, expected);
        end
    end
end
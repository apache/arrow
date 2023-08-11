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
            % Verify numeric() throws an error whose idenitifier is 
            % "arrow:badSubscript:NonPositive" if the index array provided
            % has non-positive values.

            import arrow.internal.validate.index.numeric

            errid = "arrow:badSubscript:NonPositive";

            fcn = @() numeric(0, "int32");
            testCase.verifyError(fcn, errid);

            fcn = @() numeric(-1, "int32");
            testCase.verifyError(fcn, errid);

            fcn = @() numeric([1 -1 2], "int32");
            testCase.verifyError(fcn, errid);
        end

        function NonIntegerError(testCase)
            % Verify numeric() throws an error whose idenitifier is 
            % "arrow:badSubscript:NonInteger" if the index array provided
            % has non-integer values.

            import arrow.internal.validate.index.numeric

            errid = "arrow:badSubscript:NonInteger";

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
            % Verify numeric() throws an error whose idenitifier is 
            % "arrow:badSubscript:NonInteger" if the index array is
            % complex.

            import arrow.internal.validate.index.numeric

            errid = "arrow:badSubscript:NonReal";

            fcn = @() numeric(1 + 1i, "int32");
            testCase.verifyError(fcn, errid);

            fcn = @() numeric([1 2 + 2i], "int32");
            testCase.verifyError(fcn, errid);
        end

        function ExceedsIntMaxError(testCase)
            % Verify numeric() throws an error whose idenitifier is 
            % "arrow:badSubscript:NonInteger" if the index array provided
            % has values that exceed the intmax of the intType provided.

            import arrow.internal.validate.index.numeric

            errid = "arrow:badSubscript:ExceedsIntMax";

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
            expected = int32(original);
            actual = numeric(original, "int32");
            testCase.verifyEqual(actual, expected);

            original = uint32([1 2 3 4]);
            expected = int64(original);
            actual = numeric(original, "int64");
            testCase.verifyEqual(actual, expected);
        end

        function ConvertSparseToFullStorage(testCase)
            % Verify numeric() converts sparse index arrays into full
            % storage arrays.
            
            import arrow.internal.validate.index.numeric

            original = sparse([1 2 3 4]);
            expected = int32(full(original));
            actual = numeric(original, "int32");
            testCase.verifyEqual(actual, expected);
        end

        function AssertIfNotNumeric(testCase)
            % Verify numeric() throws an assetion error if the input
            % provided is not numeric.

            import arrow.internal.validate.index.numeric

            fcn = @() numeric(false);
            testCase.verifyError(fcn, "MATLAB:assertion:failed");
        end
    end
end
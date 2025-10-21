%TROUNDTRIP Tests for roundtripping using the C Data Interface format.

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
classdef tRoundTrip < matlab.unittest.TestCase

    methods (Test)

        function EmptyArray(testCase)
            expected = arrow.array(double.empty(0, 1));
            cArray = arrow.c.Array();
            cSchema = arrow.c.Schema();

            expected.export(cArray.Address, cSchema.Address);
            actual = arrow.array.Array.import(cArray, cSchema);

            testCase.verifyEqual(actual, expected);
        end

        function ArrayWithNulls(testCase)
            % Scalar null
            expected = arrow.array(double(NaN));
            cArray = arrow.c.Array();
            cSchema = arrow.c.Schema();

            expected.export(cArray.Address, cSchema.Address);
            actual = arrow.array.Array.import(cArray, cSchema);

            testCase.verifyEqual(actual, expected);

            % Vector with nulls
            expected = arrow.array([1, NaN, 3, NaN, 5]);
            cArray = arrow.c.Array();
            cSchema = arrow.c.Schema();

            expected.export(cArray.Address, cSchema.Address);
            actual = arrow.array.Array.import(cArray, cSchema);

            testCase.verifyEqual(actual, expected);

            % Vector all nulls
            expected = arrow.array([NaN, NaN, NaN]);
            cArray = arrow.c.Array();
            cSchema = arrow.c.Schema();

            expected.export(cArray.Address, cSchema.Address);
            actual = arrow.array.Array.import(cArray, cSchema);

            testCase.verifyEqual(actual, expected);
        end

        function Float64Array(testCase)
            % Scalar
            expected = arrow.array(double(1));
            cArray = arrow.c.Array();
            cSchema = arrow.c.Schema();

            expected.export(cArray.Address, cSchema.Address);
            actual = arrow.array.Array.import(cArray, cSchema);

            testCase.verifyEqual(actual, expected);

            % Vector
            expected = arrow.array([1, 2, 3]);
            cArray = arrow.c.Array();
            cSchema = arrow.c.Schema();

            expected.export(cArray.Address, cSchema.Address);
            actual = arrow.array.Array.import(cArray, cSchema);

            testCase.verifyEqual(actual, expected);
        end

        function StringArray(testCase)
            % Scalar
            expected = arrow.array("A");
            cArray = arrow.c.Array();
            cSchema = arrow.c.Schema();

            expected.export(cArray.Address, cSchema.Address);
            actual = arrow.array.Array.import(cArray, cSchema);

            testCase.verifyEqual(actual, expected);

            % Vector
            expected = arrow.array(["A", "B", "C"]);
            cArray = arrow.c.Array();
            cSchema = arrow.c.Schema();

            expected.export(cArray.Address, cSchema.Address);
            actual = arrow.array.Array.import(cArray, cSchema);

            testCase.verifyEqual(actual, expected);
        end

        function TimestampArray(testCase)
            % Scalar
            expected = arrow.array(datetime(2024, 1, 1));
            cArray = arrow.c.Array();
            cSchema = arrow.c.Schema();

            expected.export(cArray.Address, cSchema.Address);
            actual = arrow.array.Array.import(cArray, cSchema);

            testCase.verifyEqual(actual, expected);

            % Vector
            expected = arrow.array([...
                datetime(2024, 1, 1),...
                datetime(2024, 1, 2),...
                datetime(2024, 1, 3)...
            ]);
            cArray = arrow.c.Array();
            cSchema = arrow.c.Schema();

            expected.export(cArray.Address, cSchema.Address);
            actual = arrow.array.Array.import(cArray, cSchema);

            testCase.verifyEqual(actual, expected);
        end

        function ExportErrorWrongInputTypes(testCase)
            A = arrow.array([1, 2, 3]);
            fcn = @() A.export("cArray.Address", "cSchema.Address");
            testCase.verifyError(fcn, "MATLAB:validation:UnableToConvert");
        end

        function ExportTooFewInputs(testCase)
            A = arrow.array([1, 2, 3]);
            fcn = @() A.export();
            testCase.verifyError(fcn, "MATLAB:minrhs");
        end

        function ExportTooManyInputs(testCase)
            A = arrow.array([1, 2, 3]);
            fcn = @() A.export("A", "B", "C");
            testCase.verifyError(fcn, "MATLAB:TooManyInputs");
        end

        function ImportErrorWrongInputTypes(testCase)
            cArray = "arrow.c.Array";
            cSchema = "arrow.c.Schema";
            fcn = @() arrow.array.Array.import(cArray, cSchema);
            testCase.verifyError(fcn, "MATLAB:validation:UnableToConvert");
        end

        function ImportTooFewInputs(testCase)
            fcn = @() arrow.array.Array.import();
            testCase.verifyError(fcn, "MATLAB:minrhs");
        end

        function ImportTooManyInputs(testCase)
            A = arrow.array([1, 2, 3]);
            fcn = @() arrow.array.Array.import("A", "B", "C");
            testCase.verifyError(fcn, "MATLAB:TooManyInputs");
        end

        function ImportErrorImportFailed(testCase)
            cArray = arrow.c.Array();
            cSchema = arrow.c.Schema();
            % An arrow:c:import:ImportFailed error should be thrown
            % if the supplied arrow.c.Array and arrow.c.Schema were
            % never populated previously from an exported Array.
            fcn = @() arrow.array.Array.import(cArray, cSchema);
            testCase.verifyError(fcn, "arrow:c:import:ImportFailed");
        end

    end

end

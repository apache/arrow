%TVALIDATECOLUMNNAMES Unit tests for
% arrow.tabular.internal.validateColumnNames.

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

classdef tValidateColumnNames < matlab.unittest.TestCase
    
    methods(Test)
        % Test methods
        
        function ColumnNamesProvided(testCase)
            % Verify validateColumnNames() does not error if the input
            % struct has the ColumnNames field and the ColumnNames has the
            % expected number of elements.

            import arrow.tabular.internal.validateColumnNames

            opts.ColumnNames = ["A", "B", "C"];
            actual = validateColumnNames(opts, 3);
            testCase.verifyEqual(actual, ["A", "B", "C"]);

            opts.ColumnNames = string.empty(1, 0);
            actual = validateColumnNames(opts, 0);
            testCase.verifyEqual(actual, string.empty(1, 0));
        end

        function WrongNumberColumnNamesProvided(testCase)
            % Verify validateColumnNames() errors if the input
            % struct has the ColumnNames field and the ColumnNames has
            % the wrong number of elements. The error thrown should have
            % the identifier "arrow:tabular:WrongNumberColumnNames";

            import arrow.tabular.internal.validateColumnNames

            opts.ColumnNames = ["A", "B", "C"];
            fcn = @() validateColumnNames(opts, 2);
            testCase.verifyError(fcn, "arrow:tabular:WrongNumberColumnNames");

            fcn = @() validateColumnNames(opts, 4);
            testCase.verifyError(fcn, "arrow:tabular:WrongNumberColumnNames");

            fcn = @() validateColumnNames(opts, 0);
            testCase.verifyError(fcn, "arrow:tabular:WrongNumberColumnNames");
        end

        function ColumnNamesNotProvided(testCase)
            % Verify validateColumnNames() returns the expected string
            % array if the input struct does not have the ColumnNames
            % fied.

            import arrow.tabular.internal.validateColumnNames

            opts = struct;
            actual = validateColumnNames(opts, 0);
            expected = string.empty(1, 0);
            testCase.verifyEqual(actual, expected);

            actual = validateColumnNames(opts, 1);
            expected = "Column1";
            testCase.verifyEqual(actual, expected);

            actual = validateColumnNames(opts, 2);
            expected = ["Column1" "Column2"];
            testCase.verifyEqual(actual, expected);

            actual = validateColumnNames(opts, 3);
            expected = ["Column1", "Column2", "Column3"];
            testCase.verifyEqual(actual, expected);
        end
    end
end
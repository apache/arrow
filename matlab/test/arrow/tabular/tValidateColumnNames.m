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
        
        function ValidColumnNames(testCase)
            % Verify validateColumnNames() does not error column names 
            % array has the expected number of elements.

            import arrow.tabular.internal.validateColumnNames

            columnNames = ["A", "B", "C"];
            actual = validateColumnNames(columnNames, 3);
            testCase.verifyEqual(actual, ["A", "B", "C"]);

            columnNames = string.empty(1, 0);
            actual = validateColumnNames(columnNames, 0);
            testCase.verifyEqual(actual, string.empty(1, 0));
        end

        function WrongNumberColumnNames(testCase)
            % Verify validateColumnNames() errors if the column names
            % array provided does not have the correct number of elements.
            % The error thrown should have the identifier 
            % "arrow:tabular:WrongNumberColumnNames";

            import arrow.tabular.internal.validateColumnNames

            columnNames = ["A", "B", "C"];
            fcn = @() validateColumnNames(columnNames, 2);
            testCase.verifyError(fcn, "arrow:tabular:WrongNumberColumnNames");

            fcn = @() validateColumnNames(columnNames, 4);
            testCase.verifyError(fcn, "arrow:tabular:WrongNumberColumnNames");

            fcn = @() validateColumnNames(columnNames, 0);
            testCase.verifyError(fcn, "arrow:tabular:WrongNumberColumnNames");
        end
    end
end
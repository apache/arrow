%TMAKEVALIDDIMENSIONNAMES Unit tests for
% arrow.tabular.internal.makeValidDimensionNames.

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

classdef tMakeValidDimensionNames < matlab.unittest.TestCase

    methods(Test)

        function VariableNamedRow(testCase)
        % Verify the default dimension name "Row" is replaced with "Row_1"
        % if one of the variables is named "Row".
            import arrow.tabular.internal.*

            varnames = ["Row" "Var2"];
            dimnames = makeValidDimensionNames(varnames);
            testCase.verifyEqual(dimnames, ["Row_1", "Variables"]); 
        end

        function VariableNamedVariables(testCase)
        % Verify the default dimension name "Variables" is replaced with
        % "Variables_1" if one of the variables is named "Variables".
            import arrow.tabular.internal.*

            varnames = ["Var1" "Variables"];
            dimnames = makeValidDimensionNames(varnames);
            testCase.verifyEqual(dimnames, ["Row", "Variables_1"]); 
        end

        function VariablesWithConflictingNumericSuffix(testCase)
            % Verify that conflicting numeric suffixes (e.g. "Variables"
            % and "Variables_1") are resolved as expected.
            
            import arrow.tabular.internal.*

            varnames = ["A" "Variables_1" "Variables"];
            dimnames = makeValidDimensionNames(varnames);
            testCase.verifyEqual(dimnames, ["Row", "Variables_2"]); 
        end

        function RowWithConflictingNumericSuffix(testCase)
            % Verify that conflicting numeric suffixes (e.g. "Row"
            % and "Row_1") are resolved as expected.
            
            import arrow.tabular.internal.*

            varnames = ["Row_1" "Row" "Row_3" "Test"];
            dimnames = makeValidDimensionNames(varnames);
            testCase.verifyEqual(dimnames, ["Row_2", "Variables"]); 
        end

        function DefaultDimensionNamesOk(testCase)
            % Verify the dimension names are set to the default values
            % ("Row" and "Variables") if they are not one of the variable
            % names.

            import arrow.tabular.internal.*

            varnames = ["row" "variables"];
            dimnames = makeValidDimensionNames(varnames);
            testCase.verifyEqual(dimnames, ["Row", "Variables"]); 
            
            varnames = ["A" "B" "C"];
            dimnames = makeValidDimensionNames(varnames);
            testCase.verifyEqual(dimnames, ["Row", "Variables"]); 
        end
    end
end
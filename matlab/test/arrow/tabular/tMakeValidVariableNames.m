%TMAKEVALIDVARIABLENAMES Unit tests for
% arrow.tabular.internal.makeValidVariableNames.

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
classdef tMakeValidVariableNames < matlab.unittest.TestCase

    methods(Test)

        function Colon(testCase)
            % Verify that ":" becomes ":_1".
            import arrow.tabular.internal.*

            original = ":";
            expected = ":_1";

            [actual, modified] = makeValidVariableNames(original);

            testCase.verifyEqual(actual, expected);
            testCase.verifyTrue(modified);
        end

        function RowNames(testCase)
            % Verify that "RowNames" becomes "RowNames_1".
            import arrow.tabular.internal.*

            original = "RowNames";
            expected = "RowNames_1";
            [actual, modified] = makeValidVariableNames(original);
            testCase.verifyEqual(actual, expected);
            testCase.verifyTrue(modified);
        end

        function Properties(testCase)
            % Verify that "Properties" becomes "Properties_1".
            import arrow.tabular.internal.*

            original = "Properties";
            expected = "Properties_1";

            [actual, modified] = makeValidVariableNames(original);

            testCase.verifyEqual(actual, expected);
            testCase.verifyTrue(modified);
        end

        function VariableNames(testCase)
            % Verify that "VariableNames" becomes VariableNames_1.
            import arrow.tabular.internal.*

            original = "VariableNames";
            expected = "VariableNames_1";

            [actual, modified] = makeValidVariableNames(original);

            testCase.verifyEqual(actual, expected);
            testCase.verifyTrue(modified);
        end

        function ValidVariableNames(testCase)
            % Verify that when all of the input strings
            % are valid table variable names, that none of them
            % are modified.
            import arrow.tabular.internal.*

            original = ["A", "B", "C"];
            expected = original;

            [actual, modified] = makeValidVariableNames(original);

            testCase.verifyEqual(actual, expected);
            testCase.verifyFalse(modified);
        end

        function ValidVariableNamesUnicode(testCase)
            % Verify that when all of the input strings are valid Unicode
            % table variable names, that none of them are modified.
            import arrow.tabular.internal.*

            smiley = "😀";
            tree =  "🌲";
            mango = "🥭";

            original = [smiley, tree, mango];
            expected = original;

            [actual, modified] = makeValidVariableNames(original);

            testCase.verifyEqual(actual, expected);
            testCase.verifyFalse(modified);
        end

        function PropertiesWithConflictingNumericSuffix(testCase)
            % Verify that conflicting numeric suffixes (e.g. "Properties"
            % and "Properties_1") are resolved as expected.
            import arrow.tabular.internal.*

            original = ["Properties", "Properties_1"];
            expected = ["Properties_2", "Properties_1"];

            [actual, modified] = makeValidVariableNames(original);

            testCase.verifyEqual(actual, expected);
            testCase.verifyTrue(modified);

            original = ["Properties_1", "Properties", "Properties_4"];
            expected = ["Properties_1", "Properties_2", "Properties_4"];

            [actual, modified] = makeValidVariableNames(original);

            testCase.verifyEqual(actual, expected);
            testCase.verifyTrue(modified);
        end

        function RowNamesWithConflictingNumericSuffix(testCase)
            % Verify that conflicting numeric suffixes (e.g. "RowNames"
            % and "RowNames_1") are resolved as expected.
            import arrow.tabular.internal.*

            original = ["RowNames", "RowNames_1"];
            expected = ["RowNames_2", "RowNames_1"];

            [actual, modified] = makeValidVariableNames(original);

            testCase.verifyEqual(actual, expected);
            testCase.verifyTrue(modified);

            original = ["RowNames_1", "RowNames", "RowNames_4"];
            expected = ["RowNames_1", "RowNames_2", "RowNames_4"];

            [actual, modified] = makeValidVariableNames(original);

            testCase.verifyEqual(actual, expected);
            testCase.verifyTrue(modified);
        end

        function VariableNamesWithConflictingNumericSuffix(testCase)
            % Verify that conflicting numeric suffixes (e.g. "VariableNames"
            % and "VariableNames_1") are resolved as expected.
            import arrow.tabular.internal.*

            original = ["VariableNames", "VariableNames_1"];
            expected = ["VariableNames_2", "VariableNames_1"];

            [actual, modified] = makeValidVariableNames(original);

            testCase.verifyEqual(actual, expected);
            testCase.verifyTrue(modified);

            original = ["VariableNames_1", "VariableNames", "VariableNames_4"];
            expected = ["VariableNames_1", "VariableNames_2", "VariableNames_4"];

            [actual, modified] = makeValidVariableNames(original);

            testCase.verifyEqual(actual, expected);
            testCase.verifyTrue(modified);
        end

        function ColonWithConflictingSuffix(testCase)
            % Verify that conflicting suffixes (e.g. ":"
            % and "x_") are resolved as expected.
            import arrow.tabular.internal.*

            original = [":", ":_1"];
            expected = [":_2", ":_1"];

            [actual, modified] = makeValidVariableNames(original);

            testCase.verifyEqual(actual, expected);
            testCase.verifyTrue(modified);

            original = [":_1", ":", ":_4"];
            expected = [":_1", ":_2", ":_4"];

            [actual, modified] = makeValidVariableNames(original);

            testCase.verifyEqual(actual, expected);
            testCase.verifyTrue(modified);
        end

        function EmptyStrings(testCase)
            % Verify that empty strings are mapped to Var1, ..., Vari, ...,
            % VarN as expected and that conflicting names are resolved as
            % expected.
            import arrow.tabular.internal.*

            original = "";
            expected = "Var1";

            [actual, modified] = makeValidVariableNames(original);

            testCase.verifyEqual(actual, expected);
            testCase.verifyTrue(modified);

            original = ["", "Var1", ""];
            expected = ["Var1", "Var1_1", "Var3"];

            [actual, modified] = makeValidVariableNames(original);

            testCase.verifyEqual(actual, expected);
            testCase.verifyTrue(modified);

            original = ["", "Var1", "Var1_1"];
            expected = ["Var1", "Var1_2", "Var1_1"];

            [actual, modified] = makeValidVariableNames(original);

            testCase.verifyEqual(actual, expected);
            testCase.verifyTrue(modified);
        end

        function VariableNameLengthMax(testCase)
            % Verify strings whose character length exceeds 63
            % are truncated to the max variable name length (63).
            import arrow.tabular.internal.*

            original = string(repmat('a', [1 64]));
            expected = extractBefore(original, 64); 

            [actual, modified] = makeValidVariableNames(original);

            testCase.verifyEqual(actual, expected);
            testCase.verifyTrue(modified);
        end

    end
    
end
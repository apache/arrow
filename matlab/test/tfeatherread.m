classdef tfeatherread < matlab.unittest.TestCase
    % Tests for MATLAB featherread.

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

    methods(TestClassSetup)

        function addFeatherreadToMATLABPath(testcase)
            import matlab.unittest.fixtures.PathFixture
            % Add featherread.m to the MATLAB path.
            testcase.applyFixture(PathFixture('../'));
        end

    end

    methods(Test)

        function NumericDatatypesNoNulls(testCase)
            filename = 'numeric_datatypes_with_no_nulls.feather';
            actualTable = featherread(filename);

            variableNames = {'single', ...
                             'double', ...
                             'int8', ...
                             'int16', ...
                             'int32', ...
                             'int64', ...
                             'uint8', ...
                             'uint16', ...
                             'uint32', ...
                             'uint64'};
            variableTypes = {'single', ...
                             'double', ...
                             'int8', ...
                             'int16', ...
                             'int32', ...
                             'int64', ...
                             'uint8', ...
                             'uint16', ...
                             'uint32', ...
                             'uint64'};
            numRows = 2;
            numVariables = 10;

            expectedTable = table('Size', [numRows, numVariables], 'VariableTypes', variableTypes, 'VariableNames', variableNames);
            expectedTable(1, :) = {1/3, 2/3, 1, 2, 3, 4,  9, 10, 11, 12};
            expectedTable(2, :) = {4,   5,   5, 6, 7, 8, 13, 14, 15, 16};

            testCase.verifyEqual(actualTable, expectedTable);
        end

        function NumericDatatypesWithEmptyVariableName(testCase)
            filename = 'numeric_datatypes_6th_variable_name_is_empty.feather';
            t = featherread(filename);

            actualVariableName = t.Properties.VariableNames(6);
            expectedVariableName = {'x'};
            testCase.verifyEqual(actualVariableName, expectedVariableName);
        end

        function NumericDatatypesWithNaNRow(testCase)
            filename = 'numeric_datatypes_with_nan_row.feather';
            t = featherread(filename);

            actualVariableData = t{3, {'single'}};
            expectedVariableData = single(NaN);
            testCase.verifyEqual(actualVariableData, expectedVariableData);

            actualRemainingVariablesData = t{3, {'double','int8','int16','int32','int64',...
                'uint8','uint16','uint32','uint64'}};
            expectedRemainingVariablesData = double([NaN, NaN, NaN, NaN, NaN, NaN, NaN, NaN, NaN]);
            testCase.verifyEqual(actualRemainingVariablesData, expectedRemainingVariablesData);
        end

        function NumericDatatypesWithNaNColumn(testCase)
            filename = 'numeric_datatypes_with_nan_column.feather';
            t = featherread(filename);

            actualVariable6 = t.int64;
            expectedVariable6 = double([NaN; NaN]);
            testCase.verifyEqual(actualVariable6, expectedVariable6);

            actualVariable9 = t.uint32;
            expectedVariable9 = double([NaN;NaN]);
            testCase.verifyEqual(actualVariable9, expectedVariable9);
        end

        % %%%%%%%%%%%%%%%%%%%
        % Negative test cases
        % %%%%%%%%%%%%%%%%%%%
        function ErrorIfNotAFeatherFile(testCase)
            filename = 'not_a_feather_file.feather';

            testCase.verifyError(@() featherread(filename), 'MATLAB:arrow:status:Invalid');
        end

        function ErrorIfUnableToOpenFile(testCase)
            filename = 'nonexistent.feather';

            testCase.verifyError(@() featherread(filename), 'MATLAB:arrow:UnableToOpenFile');
        end

        function ErrorIfCorruptedFeatherFile(testCase)
            filename = 'corrupted_feather_file.feather';

            testCase.verifyError(@() featherread(filename), 'MATLAB:arrow:status:Invalid');
        end

        function ErrorIfInvalidFilenameDatatype(testCase)
            filename = {'numeric_datatypes_with_no_nulls.feather'};

            testCase.verifyError(@() featherread(filename), 'MATLAB:arrow:InvalidFilenameDatatype');
        end

        function ErroriIfTooManyInputs(testCase)
            filename = 'numeric_datatypes_with_nan_column.feather';

            testCase.verifyError(@() featherread(filename, 'SomeValue'), 'MATLAB:TooManyInputs');
        end

        function ErrorIfTooFewInputs(testCase)
            testCase.verifyError(@() featherread(), 'MATLAB:narginchk:notEnoughInputs');
        end

    end

end


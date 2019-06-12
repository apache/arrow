classdef tfeather < matlab.unittest.TestCase
    % Tests for MATLAB featherread and featherwrite.

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
        
        function addFeatherFunctionsToMATLABPath(testCase)
            import matlab.unittest.fixtures.PathFixture
            % Add Feather test utilities to the MATLAB path.
            testCase.applyFixture(PathFixture('util'));
            % Add featherread and featherwrite to the MATLAB path.
            testCase.applyFixture(PathFixture(fullfile('..', 'src')));
            % featherreadmex must be on the MATLAB path.
            testCase.assertTrue(~isempty(which('featherreadmex')), ...
                '''featherreadmex'' must be on the MATLAB path. Use ''addpath'' to add folders to the MATLAB path.');
            % featherwritemex must be on the MATLAB path.
            testCase.assertTrue(~isempty(which('featherwritemex')), ...
                '''featherwritemex'' must be on to the MATLAB path. Use ''addpath'' to add folders to the MATLAB path.');
        end
        
    end
    
    methods(TestMethodSetup)
    
        function setupTempWorkingDirectory(testCase)
            import matlab.unittest.fixtures.WorkingFolderFixture;
            testCase.applyFixture(WorkingFolderFixture);
        end
        
    end
    
    methods(Test)

        function NumericDatatypesNoNulls(testCase)
            filename = fullfile(pwd, 'temp.feather');
            
            actualTable = createTable;
            expectedTable = featherRoundTrip(filename, actualTable);
            testCase.verifyEqual(actualTable, expectedTable);
        end

        function NumericDatatypesWithNaNRow(testCase)
            filename = fullfile(pwd, 'temp.feather');
            
            t = createTable;
            
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
            variableTypes = repmat({'double'}, 10, 1)';
            numRows = 1;
            numVariables = 10;
            
            addRow = table('Size', [numRows, numVariables], ...
                           'VariableTypes', variableTypes, ...
                           'VariableNames', variableNames);
            addRow(1,:) = {NaN, NaN, NaN, NaN, NaN, NaN, NaN, NaN, NaN, NaN};
            actualTable = [t; addRow];
            expectedTable = featherRoundTrip(filename, actualTable);
            testCase.verifyEqual(actualTable, expectedTable);
        end

        function NumericDatatypesWithNaNColumns(testCase)
            filename = fullfile(pwd, 'temp.feather');
            
            actualTable = createTable;
            actualTable.double = [NaN; NaN; NaN];
            actualTable.int64  = [NaN; NaN; NaN];
            
            expectedTable = featherRoundTrip(filename, actualTable);
            testCase.verifyEqual(actualTable, expectedTable);
        end
        
        function NumericDatatypesWithExpInfSciNotation(testCase)
            filename = fullfile(pwd, 'temp.feather');
            
            actualTable = createTable;
            actualTable.single(2) = 1.0418e+06;
            
            actualTable.double(1) = Inf;
            actualTable.double(2) = exp(9);
            
            actualTable.int64(2) = 1.0418e+03;
           
            expectedTable = featherRoundTrip(filename, actualTable);
            testCase.verifyEqual(actualTable, expectedTable);
        end
        
        function IgnoreRowVarNames(testCase)
            filename = fullfile(pwd, 'temp.feather');
            
            actualTable = createTable;
            time = {'day1', 'day2', 'day3'};
            actualTable.Properties.RowNames = time;
            expectedTable = featherRoundTrip(filename, actualTable);
            actualTable = createTable;
            testCase.verifyEqual(actualTable, expectedTable);
        end

        function NotFeatherExtension(testCase)
            filename = fullfile(pwd, 'temp.txt');
            
            actualTable = createTable;
            expectedTable = featherRoundTrip(filename, actualTable);
            testCase.verifyEqual(actualTable, expectedTable);
        end
        
        function EmptyTable(testCase)
            filename = fullfile(pwd, 'temp.feather');
            
            actualTable = table;
            expectedTable = featherRoundTrip(filename, actualTable);
            testCase.verifyEqual(actualTable, expectedTable);
        end

        function zeroByNTable(testCase)
            filename = fullfile(pwd, 'temp.feather');
            
            actualTable = createTable;
            actualTable([1, 2], :) = [];
            expectedTable = featherRoundTrip(filename, actualTable);
            testCase.verifyEqual(actualTable, expectedTable);
        end

        % %%%%%%%%%%%%%%%%%%%
        % Negative test cases
        % %%%%%%%%%%%%%%%%%%%

        function ErrorIfUnableToOpenFile(testCase)
            filename = fullfile(pwd, 'temp.feather');

            testCase.verifyError(@() featherread(filename), 'MATLAB:arrow:UnableToOpenFile');
        end

        function ErrorIfCorruptedFeatherFile(testCase)
            filename = fullfile(pwd, 'temp.feather');
            
            t = createTable;
            featherwrite(filename, t);
            
            fileID = fopen(filename, 'w');
            fwrite(fileID, [1; 5]);
            fclose(fileID);
            
            testCase.verifyError(@() featherread(filename), 'MATLAB:arrow:status:Invalid');
        end
        
        function ErrorIfInvalidFilenameDatatype(testCase)
            filename = fullfile(pwd, 'temp.feather');
            
            t = createTable;
            
            testCase.verifyError(@() featherwrite({filename}, t), 'MATLAB:arrow:InvalidFilenameDatatype');
            testCase.verifyError(@() featherread({filename}), 'MATLAB:arrow:InvalidFilenameDatatype');
        end

        function ErrorIfTooManyInputs(testCase)
            filename = fullfile(pwd, 'temp.feather');
            
            t = createTable;

            testCase.verifyError(@() featherwrite(filename, t, 'SomeValue', 'SomeOtherValue'), 'MATLAB:TooManyInputs');
            testCase.verifyError(@() featherread(filename, 'SomeValue', 'SomeOtherValue'), 'MATLAB:TooManyInputs');
        end

        function ErrorIfTooFewInputs(testCase)
            testCase.verifyError(@() featherwrite(), 'MATLAB:narginchk:notEnoughInputs');
            testCase.verifyError(@() featherread(), 'MATLAB:narginchk:notEnoughInputs');
        end
        
        function ErrorIfMultiColVarExist(testCase)
            filename = fullfile(pwd, 'temp.feather');
            
            age           = [38; 43; 38; 40; 49];
            smoker        = logical([1; 0; 1; 0; 1]);
            height        = [71; 69; 64; 67; 64];
            weight        = [176; 163; 131; 133; 119];
            bloodPressure = [124, 93; 109, 77; 125, 83; 117, 75; 122, 80];
            
            t = table(age, smoker, height, weight, bloodPressure);
            
            testCase.verifyError(@() featherwrite(filename, t), 'MATLAB:arrow:UnsupportedVariableType');
        end
        
        function UnsupportedMATLABDatatypes(testCase)
            filename = fullfile(pwd, 'temp.feather');

            actualTable = createTable;
            calendarDurationVariable = [calendarDuration(1, 7, 9); ...
                                        calendarDuration(2, 1, 1); ...
                                        calendarDuration(5, 3, 2)];
            actualTable = addvars(actualTable, calendarDurationVariable);

            testCase.verifyError(@() featherwrite(filename, actualTable) ,'MATLAB:arrow:UnsupportedVariableType');
        end
        
        function NumericComplexUnsupported(testCase)
            filename = fullfile(pwd, 'temp.feather');

            actualTable = createTable;
            actualTable.single(1) = 1.0418 + 2i;
            actualTable.double(2) = exp(9) + 5i;
            actualTable.int64(2) = 1.0418e+03;
           
            expectedTable = featherRoundTrip(filename, actualTable);
            testCase.verifyNotEqual(actualTable, expectedTable);
        end
        
    end
    
end

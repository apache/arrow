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
    
    methods(TestMethodSetup)
    
        function setupTempWorkingDirectory(testCase)
            import matlab.unittest.fixtures.WorkingFolderFixture;
            testCase.applyFixture(WorkingFolderFixture);
        end
        
    end
    
    methods(Test)

        function NumericDatatypesNoNulls(testCase)
            import arrow.internal.test.io.feather.roundtrip

            filename = fullfile(pwd, 'temp.feather');

            actualTable = createFeatherCompatibleTable();
            expectedTable = roundtrip(filename, actualTable);
            testCase.verifyEqual(actualTable, expectedTable);
        end

        function NumericDatatypesWithNaNRow(testCase)
            import arrow.internal.test.io.feather.roundtrip

            filename = fullfile(pwd, 'temp.feather');
            
            t = createFeatherCompatibleTable();
            t = removevars(t, ["logical", "string", "datetime"]);
            
            variableNames = {'uint8', ...
                             'uint16', ...
                             'uint32', ...
                             'uint64', ...
                             'int8', ...
                             'int16', ...
                             'int32', ...
                             'int64', ...
                             'single', ...
                             'double', ...
                             };

            variableTypes = repmat({'double'}, 10, 1)';
            numRows = 1;
            numVariables = 10;
            
            addRow = table('Size', [numRows, numVariables], ...
                           'VariableTypes', variableTypes, ...
                           'VariableNames', variableNames);
            addRow(1,:) = {NaN, NaN, NaN, NaN, NaN, NaN, NaN, NaN, NaN, NaN};
            actualTable = [t; addRow];
            expectedTable = roundtrip(filename, actualTable);
            testCase.verifyEqual(actualTable, expectedTable);
        end

        function NumericDatatypesWithNaNColumns(testCase)
            import arrow.internal.test.io.feather.roundtrip

            filename = fullfile(pwd, 'temp.feather');
            
            actualTable = createFeatherCompatibleTable();
            actualTable.double = [NaN; NaN; NaN];
            actualTable.int64  = [NaN; NaN; NaN];
            
            expectedTable = roundtrip(filename, actualTable);
            testCase.verifyEqual(actualTable, expectedTable);
        end
        
        function NumericDatatypesWithExpInfSciNotation(testCase)
            import arrow.internal.test.io.feather.roundtrip

            filename = fullfile(pwd, 'temp.feather');
            
            actualTable = createFeatherCompatibleTable();
            actualTable.single(2) = 1.0418e+06;
            
            actualTable.double(1) = Inf;
            actualTable.double(2) = exp(9);
            
            actualTable.int64(2) = 1.0418e+03;
           
            expectedTable = roundtrip(filename, actualTable);
            testCase.verifyEqual(actualTable, expectedTable);
        end
        
        function IgnoreRowVarNames(testCase)
            import arrow.internal.test.io.feather.roundtrip

            filename = fullfile(pwd, 'temp.feather');
            
            actualTable = createFeatherCompatibleTable();
            time = {'day1', 'day2', 'day3'};
            actualTable.Properties.RowNames = time;
            expectedTable = roundtrip(filename, actualTable);
            actualTable = createFeatherCompatibleTable();
            testCase.verifyEqual(actualTable, expectedTable);
        end

        function NotFeatherExtension(testCase)
            import arrow.internal.test.io.feather.roundtrip

            filename = fullfile(pwd, 'temp.txt');
            
            actualTable = createFeatherCompatibleTable();
            expectedTable = roundtrip(filename, actualTable);
            testCase.verifyEqual(actualTable, expectedTable);
        end
        
        function EmptyTable(testCase)
            import arrow.internal.test.io.feather.roundtrip

            filename = fullfile(pwd, 'temp.feather');
            
            actualTable = table;
            expectedTable = roundtrip(filename, actualTable);
            testCase.verifyEqual(actualTable, expectedTable);
        end

        function zeroByNTable(testCase)
            import arrow.internal.test.io.feather.roundtrip

            filename = fullfile(pwd, 'temp.feather');
            
            actualTable = createFeatherCompatibleTable();
            actualTable([1, 2], :) = [];
            expectedTable = roundtrip(filename, actualTable);
            testCase.verifyEqual(actualTable, expectedTable);
        end

        % %%%%%%%%%%%%%%%%%%%
        % Negative test cases
        % %%%%%%%%%%%%%%%%%%%

        function ErrorIfUnableToOpenFile(testCase)
            filename = fullfile(pwd, 'temp.feather');

            testCase.verifyError(@() featherread(filename), 'arrow:io:FailedToOpenFileForRead');
        end

        function ErrorIfCorruptedFeatherFile(testCase)

            filename = fullfile(pwd, 'temp.feather');
            
            t = createFeatherCompatibleTable();
            featherwrite(filename, t);
            
            fileID = fopen(filename, 'w');
            fwrite(fileID, [1; 5]);
            fclose(fileID);
            
            testCase.verifyError(@() featherread(filename), 'arrow:io:feather:FailedToCreateReader');
        end
        
        function ErrorIfInvalidFilenameDatatype(testCase)

            t = createFeatherCompatibleTable();
            
            testCase.verifyError(@() featherwrite({table}, t), 'MATLAB:validation:UnableToConvert');
        end

        function ErrorIfTooManyInputs(testCase)

            filename = fullfile(pwd, 'temp.feather');
            
            t = createFeatherCompatibleTable();

            testCase.verifyError(@() featherwrite(filename, t, 'SomeValue', 'SomeOtherValue'), 'MATLAB:TooManyInputs');
            testCase.verifyError(@() featherread(filename, 'SomeValue', 'SomeOtherValue'), 'MATLAB:TooManyInputs');
        end

        function ErrorIfTooFewInputs(testCase)
            testCase.verifyError(@() featherwrite(), 'MATLAB:minrhs');
            testCase.verifyError(@() featherread(), 'MATLAB:minrhs');
        end
        
        function ErrorIfMultiColVarExist(testCase)
            filename = fullfile(pwd, 'temp.feather');
            
            age           = [38; 43; 38; 40; 49];
            smoker        = logical([1; 0; 1; 0; 1]);
            height        = [71; 69; 64; 67; 64];
            weight        = [176; 163; 131; 133; 119];
            bloodPressure = [124, 93; 109, 77; 125, 83; 117, 75; 122, 80];
            
            t = table(age, smoker, height, weight, bloodPressure);
            
            testCase.verifyError(@() featherwrite(filename, t), 'arrow:array:InvalidShape');
        end
        
        function UnsupportedMATLABDatatypes(testCase)

            filename = fullfile(pwd, 'temp.feather');

            actualTable = createFeatherCompatibleTable();
            calendarDurationVariable = [calendarDuration(1, 7, 9); ...
                                        calendarDuration(2, 1, 1); ...
                                        calendarDuration(5, 3, 2)];
            actualTable = addvars(actualTable, calendarDurationVariable);

            testCase.verifyError(@() featherwrite(filename, actualTable) ,'arrow:array:UnsupportedMATLABType');
        end
        
        function NumericComplexUnsupported(testCase)
            filename = fullfile(pwd, 'temp.feather');

            actualTable = createFeatherCompatibleTable();
            actualTable.single(1) = 1.0418 + 2i;
            actualTable.double(2) = exp(9) + 5i;
            actualTable.int64(2) = 1.0418e+03;
           
            testCase.verifyError(@() featherwrite(filename, actualTable) ,'arrow:array:ComplexNumeric');
        end

        function SupportedTypes(testCase)
            import arrow.internal.test.io.feather.roundtrip

            filename = fullfile(pwd, 'temp.feather');

            % Create a table with all supported MATLAB types.
            expectedTable = table(int8   ([1, 2, 3]'), ...
                                  int16  ([1, 2, 3]'), ...
                                  int32  ([1, 2, 3]'), ...
                                  int64  ([1, 2, 3]'), ...
                                  uint8  ([1, 2, 3]'), ...
                                  uint16 ([1, 2, 3]'), ...
                                  uint32 ([1, 2, 3]'), ...
                                  uint64 ([1, 2, 3]'), ...
                                  logical([1, 0, 1]'), ...
                                  single ([1, 2, 3]'), ...
                                  double ([1, 2, 3]'), ...
                                  string (["A", "B", "C"]'), ...
                                  datetime(2023, 6, 28) + days(0:2)');

            actualTable = roundtrip(filename, expectedTable);
            testCase.verifyEqual(actualTable, expectedTable);
        end

        function UnicodeVariableNames(testCase)
            import arrow.internal.test.io.feather.roundtrip

            filename = fullfile(pwd, 'temp.feather');

            smiley = "😀";
            tree =  "🌲";
            mango = "🥭";
            columnNames = [smiley, tree, mango];
            expectedTable = table(1, 2, 3, VariableNames=columnNames);

            actualTable = roundtrip(filename, expectedTable);
            testCase.verifyEqual(actualTable, expectedTable);
        end

    end
end

function t = createFeatherCompatibleTable()
    import arrow.internal.test.tabular.createTableWithSupportedTypes
    t = createTableWithSupportedTypes;
    % Remove the duration variable because Feather V1 does not support
    % either Time32 or Time64 types.
    t = removevars(t, "duration");
end

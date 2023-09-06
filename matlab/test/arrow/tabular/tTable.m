% Tests for the arrow.tabular.Table class and the associated arrow.table
% construction function.

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

classdef tTable < matlab.unittest.TestCase

    methods(Test)

        function Basic(testCase)
            % Verify that an arrow.tabular.Table can be created
            % from a MATLAB table using the arrow.table construction
            % function.
            matlabTable = table(...
                [1, 2, 3]', ...
                ["A", "B", "C"]', ...
                [true, false, true]' ...
            );
            arrowTable = arrow.table(matlabTable);
            testCase.verifyInstanceOf(arrowTable, "arrow.tabular.Table");
        end

        function NumRows(testCase)
            % Verify that the NumRows property of arrow.tabular.Table
            % returns the expected number of rows.
            numRows = int64([1, 5, 100]);

            for expectedNumRows = numRows
                matlabTable = array2table(ones(expectedNumRows, 1));
                arrowTable = arrow.table(matlabTable);
                testCase.verifyEqual(arrowTable.NumRows, expectedNumRows);
            end

        end

        function NumColumns(testCase)
            % Verify that the NumColumns property of arrow.tabular.Table
            % returns the expected number of columns.
            numColumns = int32([1, 5, 100]);

            for expectedNumColumns = numColumns
                matlabTable = array2table(ones(1, expectedNumColumns));
                arrowTable = arrow.table(matlabTable);
                testCase.verifyEqual(arrowTable.NumColumns, expectedNumColumns);
            end

        end

        function ColumnNames(testCase)
            % Verify that the ColumnNames property of arrow.tabular.Table
            % returns the expected string array of column names.
            columnNames = ["A", "B", "C"];
            matlabTable = table(1, 2, 3, VariableNames=columnNames);
            arrowTable = arrow.table(matlabTable);
            testCase.verifyEqual(arrowTable.ColumnNames, columnNames);
        end

        function UnicodeColumnNames(testCase)
            % Verify that an arrow.tabular.Table can be created from
            % a MATLAB table with Unicode VariableNames.
            smiley = "😀";
            tree =  "🌲";
            mango = "🥭";
            columnNames = [smiley, tree, mango];
            matlabTable = table(1, 2, 3, VariableNames=columnNames);
            arrowTable = arrow.table(matlabTable);
            testCase.verifyEqual(arrowTable.ColumnNames, columnNames);
        end

        function EmptyTable(testCase)
            % Verify that an arrow.tabular.Table can be created from an
            % empty MATLAB table.
            matlabTable = table.empty(0, 0);
            arrowTable = arrow.table(matlabTable);
            testCase.verifyEqual(arrowTable.NumRows, int64(0));
            testCase.verifyEqual(arrowTable.NumColumns, int32(0));
            testCase.verifyEqual(arrowTable.ColumnNames, string.empty(1, 0));

            matlabTable = table.empty(1, 0);
            arrowTable = arrow.table(matlabTable);
            testCase.verifyEqual(arrowTable.NumRows, int64(0));
            testCase.verifyEqual(arrowTable.NumColumns, int32(0));
            testCase.verifyEqual(arrowTable.ColumnNames, string.empty(1, 0));

            matlabTable = table.empty(0, 1);
            arrowTable = arrow.table(matlabTable);
            testCase.verifyEqual(arrowTable.NumRows, int64(0));
            testCase.verifyEqual(arrowTable.NumColumns, int32(1));
            testCase.verifyEqual(arrowTable.ColumnNames, "Var1");
        end

        function FromArraysWithNoColumnNames(testCase)
            % Verify arrow.tabular.Table.fromArrays creates the expected
            % Table when given a comma-separated list of arrow.array.Array values.
            import arrow.tabular.Table
            import arrow.internal.test.tabular.createAllSupportedArrayTypes

            [arrowArrays, matlabData] = createAllSupportedArrayTypes();
            matlabTable = table(matlabData{:});

            arrowTable = Table.fromArrays(arrowArrays{:});
            expectedColumnNames = compose("Column%d", 1:width(matlabTable));
            testCase.verifyEqual(arrowTable.ColumnNames, expectedColumnNames)
        end

        function FromArraysWithColumnNames(testCase)
            % Verify arrow.tabular.Table.fromArrays creates the expected
            % Table when given a comma-separated list of arrow.array.Array values
            % and when the ColumnNames nv-pair is provided.
            import arrow.tabular.Table
            import arrow.internal.test.tabular.createAllSupportedArrayTypes

            [arrowArrays, ~] = createAllSupportedArrayTypes();

            expectedColumnNames = compose("MyVar%d", 1:numel(arrowArrays));
            arrowTable = Table.fromArrays(arrowArrays{:}, ColumnNames=expectedColumnNames);
            testCase.verifyEqual(arrowTable.ColumnNames, expectedColumnNames)
        end

        function FromArraysUnequalArrayLengthsError(testCase)
            % Verify arrow.tabular.Table.fromArrays throws an error whose
            % identifier is "arrow:tabular:UnequalArrayLengths" if the arrays
            % provided don't all have the same length.
            import arrow.tabular.Table

            A1 = arrow.array([1, 2]);
            A2 = arrow.array(["A", "B", "C"]);
            fcn = @() Table.fromArrays(A1, A2);
            testCase.verifyError(fcn, "arrow:tabular:UnequalArrayLengths");
        end

        function FromArraysWrongNumberColumnNamesError(testCase)
            % Verify arrow.tabular.Table.fromArrays throws an error whose
            % identifier is "arrow:tabular:WrongNumberColumnNames" if the
            % ColumnNames provided doesn't have one element per array.
            import arrow.tabular.Table

            A1 = arrow.array([1, 2]);
            A2 = arrow.array(["A", "B"]);
            fcn = @() Table.fromArrays(A1, A2, ColumnNames=["A", "B", "C"]);
            testCase.verifyError(fcn, "arrow:tabular:WrongNumberColumnNames");
        end

        function FromArraysColumnNamesHasMissingString(testCase)
            % Verify arrow.tabular.Table.fromArrays throws an error whose
            % identifier is "MATLAB:validators:mustBeNonmissing" if the
            % ColumnNames provided has a missing string value.
            import arrow.tabular.Table

            A1 = arrow.array([1, 2]);
            A2 = arrow.array(["A", "B"]);
            fcn = @() Table.fromArrays(A1, A2, ColumnNames=["A", missing]);
            testCase.verifyError(fcn, "MATLAB:validators:mustBeNonmissing");
        end

        function Schema(testCase)
            % Verify that the public Schema property returns an approprate
            % instance of arrow.tabular.Table.
            matlabTable = table(...
                ["A"; "B"; "C"], ...
                [1; 2; 3], ...
                [true; false; true], ...
                VariableNames=["A", "B", "C"] ...
            );
            arrowTable = arrow.table(matlabTable);
            schema = arrowTable.Schema;
            testCase.verifyEqual(schema.NumFields, int32(3));
            testCase.verifyEqual(schema.field(1).Type.ID, arrow.type.ID.String);
            testCase.verifyEqual(schema.field(1).Name, "A");
            testCase.verifyEqual(schema.field(2).Type.ID, arrow.type.ID.Float64);
            testCase.verifyEqual(schema.field(2).Name, "B");
            testCase.verifyEqual(schema.field(3).Type.ID, arrow.type.ID.Boolean);
            testCase.verifyEqual(schema.field(3).Name, "C");
        end

        function NoColumnsNoSetter(testCase)
            % Verify that trying to set the value of the public NumColumns property
            % results in an error of type "MATLAB:class:SetProhibited".
            matlabTable = table([1; 2; 3]);
            arrowTable = arrow.table(matlabTable);
            testCase.verifyError(@() setfield(arrowTable, "NumColumns", int32(100)), ...
                "MATLAB:class:SetProhibited");
        end

        function SchemaNoSetter(testCase)
            % Verify that trying to set the value of the public Schema property
            % results in an error of type "MATLAB:class:SetProhibited".
            matlabTable = table([1; 2; 3]);
            arrowTable = arrow.table(matlabTable);
            testCase.verifyError(@() setfield(arrowTable, "Schema", "Value"), ...
                "MATLAB:class:SetProhibited");
        end

        function ColumnNamesNoSetter(testCase)
            % Verify that trying to set the value of the public ColumnNames property
            % results in an error of type "MATLAB:class:SetProhibited".
            matlabTable = table([1; 2; 3]);
            arrowTable = arrow.table(matlabTable);
            testCase.verifyError(@() setfield(arrowTable, "ColumnNames", "Value"), ...
                "MATLAB:class:SetProhibited");
        end

    end

end

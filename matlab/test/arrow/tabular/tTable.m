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

        function SupportedTypes(testCase)
            % Verify that a MATLAB table containing all types
            % supported for conversion to Arrow Arrays can be round-tripped
            % from an arrow.tabular.Table to a MATLAB table and back.
            import arrow.internal.test.tabular.createTableWithSupportedTypes
            import arrow.type.traits.traits

            matlabTable = createTableWithSupportedTypes();
            arrowTable = arrow.table(matlabTable);
            expectedColumnNames = string(matlabTable.Properties.VariableNames);

            % For each variable in the input MATLAB table, look up the
            % corresponding Arrow Type of the corresponding ChunkedArray using type traits.
            expectedChunkedArrayTypes = varfun(@(var) traits(string(class(var))).TypeClassName, ...
                matlabTable, OutputFormat="uniform");
            testCase.verifyTable(arrowTable, expectedColumnNames, expectedChunkedArrayTypes, matlabTable);
        end

        function ToMATLAB(testCase)
            % Verify that the toMATLAB method converts
            % an arrow.tabular.Table to a MATLAB table as expected.
            expectedMatlabTable = table([1, 2, 3]');
            arrowTable = arrow.table(expectedMatlabTable);
            actualMatlabTable = arrowTable.toMATLAB();
            testCase.verifyEqual(actualMatlabTable, expectedMatlabTable);
        end

        function Table(testCase)
            % Verify that the toMATLAB method converts
            % an arrow.tabular.Table to a MATLAB table as expected.
            TOriginal = table([1, 2, 3]');
            arrowTable = arrow.table(TOriginal);
            TConverted = table(arrowTable);
            testCase.verifyEqual(TOriginal, TConverted);
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
            testCase.verifyEqual(toMATLAB(arrowTable), matlabTable);

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

        function EmptyTableColumnIndexError(tc)
            % Verify that an "arrow:tabular:table:NumericIndexWithEmptyTable" error
            % is thrown when calling the column method on an empty Table.
            matlabTable = table();
            arrowTable = arrow.table(matlabTable);
            fcn = @() arrowTable.column(1);
            tc.verifyError(fcn, "arrow:tabular:table:NumericIndexWithEmptyTable");
        end

        function InvalidNumericIndexError(tc)
            % Verify that an "arrow:tabular:table:InvalidNumericColumnIndex" error
            % is thrown when providing an index to the column
            % method that is outside the range of valid column indices
            % (e.g. greater than the number of columns).
            matlabTable = table(1, 2, 3);
            arrowTable = arrow.table(matlabTable);
            fcn = @() arrowTable.column(4);
            tc.verifyError(fcn, "arrow:tabular:table:InvalidNumericColumnIndex");
        end

        function UnsupportedColumnIndexType(tc)
            % Verify that an "arrow:badsubscript:UnsupportedIndexType" error
            % is thrown when providing an index to the column
            % method that is not a positive scalar integer.
            matlabTable = table(1, 2, 3);
            arrowTable = arrow.table(matlabTable);
            fcn = @() arrowTable.column(datetime(2022, 1, 3));
            tc.verifyError(fcn, "arrow:badsubscript:UnsupportedIndexType");
        end

        function ErrorIfIndexIsNonScalar(tc)
            % Verify that an "arrow:badsubscript:NonScalar" error
            % is thrown when providing a non-scalar index to the column
            % method.
            matlabtable = table(1, 2, 3);
            arrowTable = arrow.table(matlabtable);
            fcn = @() arrowTable.column([1 2]);
            tc.verifyError(fcn, "arrow:badsubscript:NonScalar");
        end

        function ErrorIfIndexIsNonPositive(tc)
            % Verify that an "arrow:badsubscript:NonPositive" error
            % is thrown when providing a non-positive index to the column
            % method.
            matlabTable = table(1, 2, 3);
            arrowTable = arrow.table(matlabTable);
            fcn = @() arrowTable.column(-1);
            tc.verifyError(fcn, "arrow:badsubscript:NonPositive");
        end

        function GetColumnByName(testCase)
            % Verify that columns can be accessed by name.
            matlabArray1 = [1; 2; 3];
            matlabArray2 = ["A"; "B"; "C"];
            matlabArray3 = [true; false; true];

            arrowArray1 = arrow.array(matlabArray1);
            arrowArray2 = arrow.array(matlabArray2);
            arrowArray3 = arrow.array(matlabArray3);

            arrowTable = arrow.tabular.Table.fromArrays(...
                arrowArray1, ...
                arrowArray2, ...
                arrowArray3, ...
                ColumnNames=["A", "B", "C"] ...
            );

            column = arrowTable.column("A");
            expectedNumChunks = int32(1);
            expectedNumElements = int64(3);
            expectedArrowType = arrow.float64();
            testCase.verifyChunkedArray(column, ...
                                        matlabArray1, ...
                                        expectedNumChunks, ...
                                        expectedNumElements, ...
                                        expectedArrowType);

            column = arrowTable.column("B");
            expectedNumChunks = int32(1);
            expectedNumElements = int64(3);
            expectedArrowType = arrow.string();
            testCase.verifyChunkedArray(column, ...
                                        matlabArray2, ...
                                        expectedNumChunks, ...
                                        expectedNumElements, ...
                                        expectedArrowType);

            column = arrowTable.column("C");
            expectedNumChunks = int32(1);
            expectedNumElements = int64(3);
            expectedArrowType = arrow.boolean();
            testCase.verifyChunkedArray(column, ...
                                        matlabArray3, ...
                                        expectedNumChunks, ...
                                        expectedNumElements, ...
                                        expectedArrowType);
        end

        function GetColumnByNameWithEmptyString(testCase)
            % Verify that a column whose name is the empty string ("")
            % can be accessed using the column() method.
            matlabArray1 = [1; 2; 3];
            matlabArray2 = ["A"; "B"; "C"];
            matlabArray3 = [true; false; true];

            arrowArray1 = arrow.array(matlabArray1);
            arrowArray2 = arrow.array(matlabArray2);
            arrowArray3 = arrow.array(matlabArray3);

            arrowTable = arrow.tabular.Table.fromArrays(...
                arrowArray1, ...
                arrowArray2, ...
                arrowArray3, ...
                ColumnNames=["A", "", "C"] ...
            );

            column = arrowTable.column("");
            expectedNumChunks = int32(1);
            expectedNumElements = int64(3);
            expectedArrowType = arrow.string();
            testCase.verifyChunkedArray(column, ...
                                        matlabArray2, ...
                                        expectedNumChunks, ...
                                        expectedNumElements, ...
                                        expectedArrowType);
        end

        function GetColumnByNameWithWhitespace(testCase)
            % Verify that a column whose name contains only whitespace
            % characters can be accessed using the column() method.
            matlabArray1 = [1; 2; 3];
            matlabArray2 = ["A"; "B"; "C"];
            matlabArray3 = [true; false; true];

            arrowArray1 = arrow.array(matlabArray1);
            arrowArray2 = arrow.array(matlabArray2);
            arrowArray3 = arrow.array(matlabArray3);

            arrowTable = arrow.tabular.Table.fromArrays(...
                arrowArray1, ...
                arrowArray2, ...
                arrowArray3, ...
                ColumnNames=[" ", "  ", "   "] ...
            );

            column = arrowTable.column(" ");
            expectedNumChunks = int32(1);
            expectedNumElements = int64(3);
            expectedArrowType = arrow.float64();
            testCase.verifyChunkedArray(column, ...
                                        matlabArray1, ...
                                        expectedNumChunks, ...
                                        expectedNumElements, ...
                                        expectedArrowType);

            column = arrowTable.column("  ");
            expectedNumChunks = int32(1);
            expectedNumElements = int64(3);
            expectedArrowType = arrow.string();
            testCase.verifyChunkedArray(column, ...
                                        matlabArray2, ...
                                        expectedNumChunks, ...
                                        expectedNumElements, ...
                                        expectedArrowType);

            column = arrowTable.column("   ");
            expectedNumChunks = int32(1);
            expectedNumElements = int64(3);
            expectedArrowType = arrow.boolean();
            testCase.verifyChunkedArray(column, ...
                                        matlabArray3, ...
                                        expectedNumChunks, ...
                                        expectedNumElements, ...
                                        expectedArrowType);
        end

        function ErrorIfColumnNameDoesNotExist(testCase)
            % Verify that an error is thrown when trying to access a column
            % with a name that is not part of the Schema of the Table.
            matlabArray1 = [1; 2; 3];
            matlabArray2 = ["A"; "B"; "C"];
            matlabArray3 = [true; false; true];

            arrowArray1 = arrow.array(matlabArray1);
            arrowArray2 = arrow.array(matlabArray2);
            arrowArray3 = arrow.array(matlabArray3);

            arrowTable = arrow.tabular.Table.fromArrays(...
                arrowArray1, ...
                arrowArray2, ...
                arrowArray3, ...
                ColumnNames=["A", "B", "C"] ...
            );

            % Matching should be case-sensitive.
            name = "a";
            testCase.verifyError(@() arrowTable.column(name), "arrow:tabular:schema:AmbiguousFieldName");

            name = "aA";
            testCase.verifyError(@() arrowTable.column(name), "arrow:tabular:schema:AmbiguousFieldName");

            name = "D";
            testCase.verifyError(@() arrowTable.column(name), "arrow:tabular:schema:AmbiguousFieldName");

            name = "";
            testCase.verifyError(@() arrowTable.column(name), "arrow:tabular:schema:AmbiguousFieldName");

            name = " ";
            testCase.verifyError(@() arrowTable.column(name), "arrow:tabular:schema:AmbiguousFieldName");
        end

        function ErrorIfAmbiguousColumnName(testCase)
            % Verify that an error is thrown when trying to access a column
            % with a name that is ambiguous / occurs more than once in the
            % Schema of the Table.
            arrowTable = arrow.tabular.Table.fromArrays(...
                arrow.array([1, 2, 3]), ...
                arrow.array(["A", "B", "C"]), ...
                arrow.array([true, false, true]), ...
                arrow.array([days(1), days(2), days(3)]), ...
                ColumnNames=["A", "A", "B", "B"] ...
            );

            name = "A";
            testCase.verifyError(@() arrowTable.column(name), "arrow:tabular:schema:AmbiguousFieldName");

            name = "B";
            testCase.verifyError(@() arrowTable.column(name), "arrow:tabular:schema:AmbiguousFieldName");
        end

        function GetColumnByNameWithChar(testCase)
            % Verify that the column method works when supplied a char
            % vector as input.
            matlabArray1 = [1; 2; 3];
            matlabArray2 = ["A"; "B"; "C"];
            matlabArray3 = [true; false; true];

            arrowArray1 = arrow.array(matlabArray1);
            arrowArray2 = arrow.array(matlabArray2);
            arrowArray3 = arrow.array(matlabArray3);

            arrowTable = arrow.tabular.Table.fromArrays(...
                arrowArray1, ...
                arrowArray2, ...
                arrowArray3, ...
                ColumnNames=["", "B", "123"] ...
            );

            % Should match the first column whose name is the
            % empty string ("").
            name = char.empty(0, 0);
            column = arrowTable.column(name);
            expectedNumChunks = int32(1);
            expectedNumElements = int64(3);
            expectedArrowType = arrow.float64();
            testCase.verifyChunkedArray(column, ...
                                        matlabArray1, ...
                                        expectedNumChunks, ...
                                        expectedNumElements, ...
                                        expectedArrowType);

            name = char.empty(0, 1);
            column = arrowTable.column(name);
            expectedNumChunks = int32(1);
            expectedNumElements = int64(3);
            expectedArrowType = arrow.float64();
            testCase.verifyChunkedArray(column, ...
                                        matlabArray1, ...
                                        expectedNumChunks, ...
                                        expectedNumElements, ...
                                        expectedArrowType);

            name = char.empty(1, 0);
            column = arrowTable.column(name);
            expectedNumChunks = int32(1);
            expectedNumElements = int64(3);
            expectedArrowType = arrow.float64();
            testCase.verifyChunkedArray(column, ...
                                        matlabArray1, ...
                                        expectedNumChunks, ...
                                        expectedNumElements, ...
                                        expectedArrowType);

            % Should match the second column whose name is "B".
            name = 'B';
            column = arrowTable.column(name);
            expectedNumChunks = int32(1);
            expectedNumElements = int64(3);
            expectedArrowType = arrow.string();
            testCase.verifyChunkedArray(column, ...
                                        matlabArray2, ...
                                        expectedNumChunks, ...
                                        expectedNumElements, ...
                                        expectedArrowType);

            % Should match the third column whose name is "123".
            name = '123';
            column = arrowTable.column(name);
            expectedNumChunks = int32(1);
            expectedNumElements = int64(3);
            expectedArrowType = arrow.boolean();
            testCase.verifyChunkedArray(column, ...
                                        matlabArray3, ...
                                        expectedNumChunks, ...
                                        expectedNumElements, ...
                                        expectedArrowType);
        end

        function ErrorIfColumnNameIsNonScalar(testCase)
            % Verify that an error is thrown if a nonscalar string array is
            % specified as a column name to the column method.
            arrowTable = arrow.tabular.Table.fromArrays(...
                arrow.array([1, 2, 3]), ...
                arrow.array(["A", "B", "C"]), ...
                arrow.array([true, false, true]), ...
                ColumnNames=["A", "B", "C"] ...
            );

            name = ["A", "B", "C"];
            testCase.verifyError(@() arrowTable.column(name), "arrow:badsubscript:NonScalar");

            name = ["A";  "B"; "C"];
            testCase.verifyError(@() arrowTable.column(name), "arrow:badsubscript:NonScalar");
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

        function FromArraysNoInputs(testCase)
            % Verify that an empty Table is returned when calling fromArrays
            % with no input arguments.
            arrowTable = arrow.tabular.Table.fromArrays();
            testCase.verifyEqual(arrowTable.NumRows, int64(0));
            testCase.verifyEqual(arrowTable.NumColumns, int32(0));
            testCase.verifyEqual(arrowTable.ColumnNames, string.empty(1, 0));
        end

        function ConstructionFunctionNoInputs(testCase)
            % Verify that an empty Table is returned when calling
            % the arrow.table construction function with no inputs.
            arrowTable = arrow.table();
            testCase.verifyEqual(arrowTable.NumRows, int64(0));
            testCase.verifyEqual(arrowTable.NumColumns, int32(0));
            testCase.verifyEqual(arrowTable.ColumnNames, string.empty(1, 0));
        end

        function Schema(testCase)
            % Verify that the public Schema property returns an appropriate
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

        function TestIsEqualTrue(testCase)
            % Verify two tables are considered equal if:
            %   1. They have the same schema
            %   2. Their corresponding columns are equal
            import arrow.tabular.Table

            a1 = arrow.array([1 2 3]);
            a2 = arrow.array(["A" "B" "C"]);
            a3 = arrow.array([true true false]);

            t1 = Table.fromArrays(a1, a2, a3, ...
                ColumnNames=["A", "B", "C"]);
            t2 = Table.fromArrays(a1, a2, a3, ...
                ColumnNames=["A", "B", "C"]);
            testCase.verifyTrue(isequal(t1, t2));

            % Compare zero-column tables
            t3 = Table.fromArrays();
            t4 = Table.fromArrays();
            testCase.verifyTrue(isequal(t3, t4));

            % Compare zero-row tables
            a4 = arrow.array([]);
            a5 = arrow.array(strings(0, 0));
            t5 = Table.fromArrays(a4, a5, ColumnNames=["D" "E"]);
            t6 = Table.fromArrays(a4, a5, ColumnNames=["D" "E"]);
            testCase.verifyTrue(isequal(t5, t6));

            % Call isequal with more than two arguments
            testCase.verifyTrue(isequal(t3, t4, t3, t4));
        end

        function TestIsEqualFalse(testCase)
            % Verify isequal returns false when expected.
            import arrow.tabular.Table

            a1 = arrow.array([1 2 3]);
            a2 = arrow.array(["A" "B" "C"]);
            a3 = arrow.array([true true false]);
            a4 = arrow.array(["A" missing "C"]); 
            a5 = arrow.array([1 2]);
            a6 = arrow.array(["A" "B"]);
            a7 = arrow.array([true true]);

            t1 = Table.fromArrays(a1, a2, a3, ...
                ColumnNames=["A", "B", "C"]);
            t2 = Table.fromArrays(a1, a2, a3, ...
                ColumnNames=["D", "E", "F"]);
            t3 = Table.fromArrays(a1, a4, a3, ...
                ColumnNames=["A", "B", "C"]);
            t4 = Table.fromArrays(a5, a6, a7, ...
                ColumnNames=["A", "B", "C"]);
            t5 = Table.fromArrays(a1, a2, a3, a1, ...
                ColumnNames=["A", "B", "C", "D"]);

            % The column names are not equal
            testCase.verifyFalse(isequal(t1, t2));

            % The columns are not equal
            testCase.verifyFalse(isequal(t1, t3));

            % The number of rows are not equal
            testCase.verifyFalse(isequal(t1, t4));

            % The number of columns are not equal
            testCase.verifyFalse(isequal(t1, t5));

            % Call isequal with more than two arguments
            testCase.verifyFalse(isequal(t1, t2, t3, t4));
        end

        function FromRecordBatchesZeroInputsError(testCase)
            % Verify the arrow.tabular.Table.fromRecordBatches function
            % throws an `arrow:Table:FromRecordBatches:ZeroBatches` 
            % exception if called with zero input arguments.
            import arrow.tabular.Table
            fcn = @() Table.fromRecordBatches();
            testCase.verifyError(fcn, "arrow:Table:FromRecordBatches:ZeroBatches");
        end

        function FromRecordBatchesOneInput(testCase)
            % Verify the arrow.tabular.Table.fromRecordBatches function
            % returns the expected arrow.tabular.Table instance when 
            % provided a single RecordBatch as input.
            import arrow.tabular.Table
            matlabTable = table([1; 2], ["A"; "B"], VariableNames=["Number" "Letter"]);
            recordBatch = arrow.recordBatch(matlabTable);
            arrowTable = Table.fromRecordBatches(recordBatch);
            testCase.verifyTable(arrowTable, ["Number", "Letter"], ["arrow.type.Float64Type", "arrow.type.StringType"], matlabTable);
        end

        function FromRecordBatchesMultipleInputs(testCase)
            % Verify the arrow.tabular.Table.fromRecordBatches function
            % returns the expected arrow.tabular.Table instance when 
            % provided mulitple RecordBatches as input.
            import arrow.tabular.Table
            matlabTable1 = table([1; 2], ["A"; "B"], VariableNames=["Number" "Letter"]);
            matlabTable2 = table([10; 20; 30], ["A1"; "B1"; "C1"], VariableNames=["Number" "Letter"]);
            matlabTable3 = table([100; 200], ["A2"; "B2"], VariableNames=["Number" "Letter"]);

            recordBatch1 = arrow.recordBatch(matlabTable1);
            recordBatch2 = arrow.recordBatch(matlabTable2);
            recordBatch3 = arrow.recordBatch(matlabTable3);

            arrowTable = Table.fromRecordBatches(recordBatch1, recordBatch2, recordBatch3);
            testCase.verifyTable(arrowTable, ["Number", "Letter"], ["arrow.type.Float64Type", "arrow.type.StringType"], [matlabTable1; matlabTable2; matlabTable3]);
        end

        function FromRecordBatchesInconsistentSchemaError(testCase)
            % Verify the arrow.tabular.Table.fromRecordBatches function
            % throws an `arrow:Table:FromRecordBatches:InconsistentSchema`
            % exception if the Schemas of the provided  RecordBatches are 
            % inconsistent.
            import arrow.tabular.Table
            matlabTable1 = table("A", 1);
            matlabTable2 = table(2, "B");
            recordBatch1 = arrow.recordBatch(matlabTable1);
            recordBatch2 = arrow.recordBatch(matlabTable2);

            fcn = @() Table.fromRecordBatches(recordBatch1, recordBatch2);
            testCase.verifyError(fcn, "arrow:Table:FromRecordBatches:InconsistentSchema");
        end
    end

    methods

        function verifyTable(testCase, arrowTable, expectedColumnNames, expectedArrayClasses, expectedMatlabTable)
            testCase.verifyEqual(arrowTable.NumColumns, int32(width(expectedMatlabTable)));
            testCase.verifyEqual(arrowTable.ColumnNames, expectedColumnNames);
            matlabTable = table(arrowTable);
            testCase.verifyEqual(matlabTable, expectedMatlabTable);
            for ii = 1:arrowTable.NumColumns
                column = arrowTable.column(ii);
                testCase.verifyEqual(column.toMATLAB(), expectedMatlabTable{:, ii});
                testCase.verifyInstanceOf(column.Type, expectedArrayClasses(ii));
            end
        end

        function verifyChunkedArray(testCase, chunkedArray, expectedMatlabData, expectedNumChunks, expectedNumElements, expectedArrowType)
            testCase.verifyInstanceOf(chunkedArray, "arrow.array.ChunkedArray");
            testCase.verifyEqual(toMATLAB(chunkedArray), expectedMatlabData);
            testCase.verifyEqual(chunkedArray.NumChunks, expectedNumChunks)
            testCase.verifyEqual(chunkedArray.NumElements, expectedNumElements);
            testCase.verifyEqual(chunkedArray.Type, expectedArrowType);
        end

    end

end

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

classdef tRecordBatch < matlab.unittest.TestCase
% Test class containing tests for arrow.tabular.RecordBatch

    methods(Test)

        function Basic(tc)
            T = table([1, 2, 3]');
            arrowRecordBatch = arrow.recordBatch(T);
            className = string(class(arrowRecordBatch));
            tc.verifyEqual(className, "arrow.tabular.RecordBatch");
        end

        function SupportedTypes(tc)
            % Create a table all supported MATLAB types.
            import arrow.internal.test.tabular.createTableWithSupportedTypes
            import arrow.type.traits.traits

            TOriginal = createTableWithSupportedTypes();
            arrowRecordBatch = arrow.recordBatch(TOriginal);
            expectedColumnNames = string(TOriginal.Properties.VariableNames);

            % For each variable in the input MATLAB table, look up the 
            % corresponding Arrow Array type using type traits.
            expectedArrayClasses = varfun(@(var) traits(string(class(var))).ArrayClassName, ...
                TOriginal, OutputFormat="uniform");
            tc.verifyRecordBatch(arrowRecordBatch, expectedColumnNames, expectedArrayClasses, TOriginal);
        end

        function ToMATLAB(tc)
            TOriginal = table([1, 2, 3]');
            arrowRecordBatch = arrow.recordBatch(TOriginal);
            TConverted = arrowRecordBatch.toMATLAB();
            tc.verifyEqual(TOriginal, TConverted);
        end

        function Table(tc)
            TOriginal = table([1, 2, 3]');
            arrowRecordBatch = arrow.recordBatch(TOriginal);
            TConverted = table(arrowRecordBatch);
            tc.verifyEqual(TOriginal, TConverted);
        end

        function ColumnNames(tc)
            columnNames = ["A", "B", "C"];
            TOriginal = table(1, 2, 3, VariableNames=columnNames);
            arrowRecordBatch = arrow.recordBatch(TOriginal);
            tc.verifyEqual(arrowRecordBatch.ColumnNames, columnNames);
        end

        function NumColumns(tc)
            numColumns = int32([1, 5, 100]);

            for nc = numColumns
                T = array2table(ones(1, nc));
                arrowRecordBatch = arrow.recordBatch(T);
                tc.verifyEqual(arrowRecordBatch.NumColumns, nc);
            end
        end

        function UnicodeColumnNames(tc)
            smiley = "😀";
            tree =  "🌲";
            mango = "🥭";
            columnNames = [smiley, tree, mango];
            TOriginal = table(1, 2, 3, VariableNames=columnNames);
            arrowRecordBatch = arrow.recordBatch(TOriginal);
            tc.verifyEqual(arrowRecordBatch.ColumnNames, columnNames);
            TConverted = arrowRecordBatch.toMATLAB();
            tc.verifyEqual(TOriginal, TConverted);
        end

        function EmptyTable(tc)
            TOriginal = table();
            arrowRecordBatch = arrow.recordBatch(TOriginal);
            TConverted = arrowRecordBatch.toMATLAB();
            tc.verifyEqual(TOriginal, TConverted);
        end

        function EmptyRecordBatchColumnIndexError(tc)
            TOriginal = table();
            arrowRecordBatch = arrow.recordBatch(TOriginal);
            fcn = @() arrowRecordBatch.column(1);
            tc.verifyError(fcn, "arrow:tabular:recordbatch:NumericIndexWithEmptyRecordBatch");
        end

        function InvalidNumericIndexError(tc)
            TOriginal = table(1, 2, 3);
            arrowRecordBatch = arrow.recordBatch(TOriginal);
            fcn = @() arrowRecordBatch.column(4);
            tc.verifyError(fcn, "arrow:tabular:recordbatch:InvalidNumericColumnIndex");
        end

        function UnsupportedColumnIndexType(tc)
            TOriginal = table(1, 2, 3);
            arrowRecordBatch = arrow.recordBatch(TOriginal);
            fcn = @() arrowRecordBatch.column(datetime(2022, 1, 3));
            tc.verifyError(fcn, "arrow:badsubscript:UnsupportedIndexType");
        end

        function ErrorIfIndexIsNonScalar(tc)
            TOriginal = table(1, 2, 3);
            arrowRecordBatch = arrow.recordBatch(TOriginal);
            fcn = @() arrowRecordBatch.column([1 2]);
            tc.verifyError(fcn, "MATLAB:expectedScalar");
        end

        function ErrorIfIndexIsNonPositive(tc)
            TOriginal = table(1, 2, 3);
            arrowRecordBatch = arrow.recordBatch(TOriginal);
            fcn = @() arrowRecordBatch.column(-1);
            tc.verifyError(fcn, "arrow:badsubscript:NonPositive");
        end

        function FromArraysColumnNamesNotProvided(tc)
        % Verify arrow.tabular.RecordBatch.fromArrays creates the expected
        % RecordBatch when given a comma-separated list of
        % arrow.array.Array values.
            import arrow.tabular.RecordBatch
            import arrow.internal.test.tabular.createAllSupportedArrayTypes

            [arrowArrays, matlabData] = createAllSupportedArrayTypes();
            TOriginal = table(matlabData{:});

            arrowRecordBatch = RecordBatch.fromArrays(arrowArrays{:});
            expectedColumnNames = compose("Column%d", 1:width(TOriginal));
            TOriginal.Properties.VariableNames = expectedColumnNames;
            expectedArrayClasses = cellfun(@(c) string(class(c)), arrowArrays, UniformOutput=true);
            tc.verifyRecordBatch(arrowRecordBatch, expectedColumnNames, expectedArrayClasses, TOriginal);
        end

        function FromArraysWithColumnNamesProvided(tc)
        % Verify arrow.tabular.RecordBatch.fromArrays creates the expected
        % RecordBatch when given a comma-separated list of
        % arrow.array.Array values and the ColumnNames nv-pair is provided.
            import arrow.tabular.RecordBatch
            import arrow.internal.test.tabular.createAllSupportedArrayTypes

            [arrowArrays, matlabData] = createAllSupportedArrayTypes();
            TOriginal = table(matlabData{:});

            columnNames = compose("MyVar%d", 1:numel(arrowArrays));
            arrowRecordBatch = RecordBatch.fromArrays(arrowArrays{:}, ColumnNames=columnNames);
            TOriginal.Properties.VariableNames = columnNames;
            expectedArrayClasses = cellfun(@(c) string(class(c)), arrowArrays, UniformOutput=true);
            tc.verifyRecordBatch(arrowRecordBatch, columnNames, expectedArrayClasses, TOriginal);
        end

        function FromArraysUnequalArrayLengthsError(tc)
        % Verify arrow.tabular.RecordBatch.fromArrays throws an error whose
        % identifier is "arrow:tabular:UnequalArrayLengths" if the arrays
        % provided don't all have the same length.
            import arrow.tabular.RecordBatch

            A1 = arrow.array([1, 2]);
            A2 = arrow.array(["A", "B", "C"]);
            fcn = @() RecordBatch.fromArrays(A1, A2);
            tc.verifyError(fcn, "arrow:tabular:UnequalArrayLengths");
        end

        function FromArraysWrongNumberColumnNamesError(tc)
        % Verify arrow.tabular.RecordBatch.fromArrays throws an error whose
        % identifier is "arrow:tabular:WrongNumberColumnNames" if the 
        % ColumnNames provided doesn't have one element per array.
            import arrow.tabular.RecordBatch

            A1 = arrow.array([1, 2]);
            A2 = arrow.array(["A", "B"]);
            fcn = @() RecordBatch.fromArrays(A1, A2, columnNames=["A", "B", "C"]);
            tc.verifyError(fcn, "arrow:tabular:WrongNumberColumnNames");
        end

        function FromArraysColumnNamesHasMissingString(tc)
        % Verify arrow.tabular.RecordBatch.fromArrays throws an error whose
        % identifier is "MATLAB:validators:mustBeNonmissing" if the 
        % ColumnNames provided has a missing string value.
            import arrow.tabular.RecordBatch

            A1 = arrow.array([1, 2]);
            A2 = arrow.array(["A", "B"]);
            fcn = @() RecordBatch.fromArrays(A1, A2, columnNames=["A", missing]);
            tc.verifyError(fcn, "MATLAB:validators:mustBeNonmissing");
        end

        function Schema(tc)
        % Verify that the public Schema property returns an approprate
        % instance of arrow.tabular.Schema.
            t = table(["A"; "B"; "C"], ...
                      [1; 2; 3], ...
                      [true; false; true], ...
                      VariableNames=["A", "B", "C"]);
            recordBatch = arrow.recordBatch(t);
            schema = recordBatch.Schema;
            tc.verifyEqual(schema.NumFields, int32(3));
            tc.verifyEqual(schema.field(1).Type.ID, arrow.type.ID.String);
            tc.verifyEqual(schema.field(1).Name, "A");
            tc.verifyEqual(schema.field(2).Type.ID, arrow.type.ID.Float64);
            tc.verifyEqual(schema.field(2).Name, "B");
            tc.verifyEqual(schema.field(3).Type.ID, arrow.type.ID.Boolean);
            tc.verifyEqual(schema.field(3).Name, "C");
        end

        function SchemaNoSetter(tc)
        % Verify that trying to set the value of the public Schema property
        % results in an error of type "MATLAB:class:SetProhibited".
            t = table([1; 2; 3]);
            recordBatch = arrow.recordBatch(t);
            tc.verifyError(@() setfield(recordBatch, "Schema", "Value"), ...
                "MATLAB:class:SetProhibited");
        end

        function GetColumnByName(testCase)
            % Verify that columns can be accessed by name.
            recordBatch = arrow.tabular.RecordBatch.fromArrays(...
                arrow.array([1, 2, 3]), ...
                arrow.array(["A", "B", "C"]), ...
                arrow.array([true, false, true]), ...
                ColumnNames=["A", "B", "C"] ...
            );

            expected = arrow.array([1, 2, 3]);
            actual = recordBatch.column("A");
            testCase.verifyEqual(actual, expected);

            expected = arrow.array(["A", "B", "C"]);
            actual = recordBatch.column("B");
            testCase.verifyEqual(actual, expected);

            expected = arrow.array([true, false, true]);
            actual = recordBatch.column("C");
            testCase.verifyEqual(actual, expected);
        end

        function GetColumnByNameWithEmptyString(testCase)
            % Verify that a column whose name is the empty string ("")
            % can be accessed using the column() method.
            recordBatch = arrow.tabular.RecordBatch.fromArrays(...
                arrow.array([1, 2, 3]), ...
                arrow.array(["A", "B", "C"]), ...
                arrow.array([true, false, true]), ...
                ColumnNames=["A", "", "C"] ...
            );

            expected = arrow.array(["A", "B", "C"]);
            actual = recordBatch.column("");
            testCase.verifyEqual(actual, expected)
        end

        function GetColumnByNameWithWhitespace(testCase)
            % Verify that a column whose name contains only whitespace
            % characters can be accessed using the column() method.
            recordBatch = arrow.tabular.RecordBatch.fromArrays(...
                arrow.array([1, 2, 3]), ...
                arrow.array(["A", "B", "C"]), ...
                arrow.array([true, false, true]), ...
                ColumnNames=[" ", "  ", "   "] ...
            );

            expected = arrow.array([1, 2, 3]);
            actual = recordBatch.column(" ");
            testCase.verifyEqual(actual, expected);

            expected = arrow.array(["A", "B", "C"]);
            actual = recordBatch.column("  ");
            testCase.verifyEqual(actual, expected);

            expected = arrow.array([true, false, true]);
            actual = recordBatch.column("   ");
            testCase.verifyEqual(actual, expected);
        end

        function ErrorIfColumnNameDoesNotExist(testCase)
            % Verify that an error is thrown when trying to access a column
            % with a name that is not part of the Schema of the RecordBatch.
            recordBatch = arrow.tabular.RecordBatch.fromArrays(...
                arrow.array([1, 2, 3]), ...
                arrow.array(["A", "B", "C"]), ...
                arrow.array([true, false, true]), ...
                ColumnNames=["A", "B", "C"] ...
            );

            % Matching should be case sensitive.
            name = "a";
            testCase.verifyError(@() recordBatch.column(name), "arrow:tabular:schema:AmbiguousFieldName");

            name = "aA";
            testCase.verifyError(@() recordBatch.column(name), "arrow:tabular:schema:AmbiguousFieldName");

            name = "D";
            testCase.verifyError(@() recordBatch.column(name), "arrow:tabular:schema:AmbiguousFieldName");

            name = "";
            testCase.verifyError(@() recordBatch.column(name), "arrow:tabular:schema:AmbiguousFieldName");

            name = " ";
            testCase.verifyError(@() recordBatch.column(name), "arrow:tabular:schema:AmbiguousFieldName");
        end

        function ErrorIfAmbiguousColumnName(testCase)
            % Verify that an error is thrown when trying to access a column
            % with a name that is ambiguous / occurs more than once in the
            % Schema of the RecordBatch.
            recordBatch = arrow.tabular.RecordBatch.fromArrays(...
                arrow.array([1, 2, 3]), ...
                arrow.array(["A", "B", "C"]), ...
                arrow.array([true, false, true]), ...
                arrow.array([days(1), days(2), days(3)]), ...
                ColumnNames=["A", "A", "B", "B"] ...
            );

            name = "A";
            testCase.verifyError(@() recordBatch.column(name), "arrow:tabular:schema:AmbiguousFieldName");

            name = "B";
            testCase.verifyError(@() recordBatch.column(name), "arrow:tabular:schema:AmbiguousFieldName");
        end

        function GetColumnByNameWithChar(testCase)
            % Verify that the column method works when supplied a char
            % vector as input.
            recordBatch = arrow.tabular.RecordBatch.fromArrays(...
                arrow.array([1, 2, 3]), ...
                arrow.array(["A", "B", "C"]), ...
                arrow.array([true, false, true]), ...
                ColumnNames=["", "B", "123"] ...
            );

            % Should match the first column whose name is the
            % empty string ("").
            name = char.empty(0, 0);
            expected = arrow.array([1, 2, 3]);
            actual = recordBatch.column(name);
            testCase.verifyEqual(actual, expected);

            name = char.empty(0, 1);
            expected = arrow.array([1, 2, 3]);
            actual = recordBatch.column(name);
            testCase.verifyEqual(actual, expected);

            name = char.empty(1, 0);
            expected = arrow.array([1, 2, 3]);
            actual = recordBatch.column(name);
            testCase.verifyEqual(actual, expected);

            % Should match the second column whose name is "B".
            name = 'B';
            expected = arrow.array(["A", "B", "C"]);
            actual = recordBatch.column(name);
            testCase.verifyEqual(actual, expected);

            % Should match the third column whose name is "123".
            name = '123';
            expected = arrow.array([true, false, true]);
            actual = recordBatch.column(name);
            testCase.verifyEqual(actual, expected);
        end

        function ErrorIfColumnNameIsNonScalar(testCase)
            % Verify that an error is thrown if a nonscalar string array is
            % specified as a column name to the column method.
            recordBatch = arrow.tabular.RecordBatch.fromArrays(...
                arrow.array([1, 2, 3]), ...
                arrow.array(["A", "B", "C"]), ...
                arrow.array([true, false, true]), ...
                ColumnNames=["A", "B", "C"] ...
            );

            name = ["A", "B", "C"];
            testCase.verifyError(@() recordBatch.column(name), "MATLAB:expectedScalar");

            name = ["A";  "B"; "C"];
            testCase.verifyError(@() recordBatch.column(name), "MATLAB:expectedScalar");
        end

    end

    methods
        function verifyRecordBatch(tc, recordBatch, expectedColumnNames, expectedArrayClasses, expectedTable)
            tc.verifyEqual(recordBatch.NumColumns, int32(width(expectedTable)));
            tc.verifyEqual(recordBatch.ColumnNames, expectedColumnNames);
            convertedTable = recordBatch.table();
            tc.verifyEqual(convertedTable, expectedTable);
             for ii = 1:recordBatch.NumColumns
                column = recordBatch.column(ii);
                tc.verifyEqual(column.toMATLAB(), expectedTable{:, ii});
                tc.verifyInstanceOf(column, expectedArrayClasses(ii));
             end
        end
    end
end


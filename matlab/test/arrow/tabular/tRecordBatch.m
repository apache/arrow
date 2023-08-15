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
            arrowRecordBatch = arrow.recordbatch(T);
            className = string(class(arrowRecordBatch));
            tc.verifyEqual(className, "arrow.tabular.RecordBatch");
        end

        function SupportedTypes(tc)
            % Create a table all supported MATLAB types.
            TOriginal = createTableWithAllSupportedTypes();
            arrowRecordBatch = arrow.recordbatch(TOriginal);
            expectedColumnNames = compose("Var%d", 1:13);
            tc.verifyRecordBatch(arrowRecordBatch, expectedColumnNames, TOriginal);
        end

        function ToMATLAB(tc)
            TOriginal = table([1, 2, 3]');
            arrowRecordBatch = arrow.recordbatch(TOriginal);
            TConverted = arrowRecordBatch.toMATLAB();
            tc.verifyEqual(TOriginal, TConverted);
        end

        function Table(tc)
            TOriginal = table([1, 2, 3]');
            arrowRecordBatch = arrow.recordbatch(TOriginal);
            TConverted = table(arrowRecordBatch);
            tc.verifyEqual(TOriginal, TConverted);
        end

        function ColumnNames(tc)
            columnNames = ["A", "B", "C"];
            TOriginal = table(1, 2, 3, VariableNames=columnNames);
            arrowRecordBatch = arrow.recordbatch(TOriginal);
            tc.verifyEqual(arrowRecordBatch.ColumnNames, columnNames);
        end

        function NumColumns(tc)
            numColumns = int32([1, 5, 100]);

            for nc = numColumns
                T = array2table(ones(1, nc));
                arrowRecordBatch = arrow.recordbatch(T);
                tc.verifyEqual(arrowRecordBatch.NumColumns, nc);
            end
        end

        function UnicodeColumnNames(tc)
            smiley = "😀";
            tree =  "🌲";
            mango = "🥭";
            columnNames = [smiley, tree, mango];
            TOriginal = table(1, 2, 3, VariableNames=columnNames);
            arrowRecordBatch = arrow.recordbatch(TOriginal);
            tc.verifyEqual(arrowRecordBatch.ColumnNames, columnNames);
            TConverted = arrowRecordBatch.toMATLAB();
            tc.verifyEqual(TOriginal, TConverted);
        end

        function EmptyTable(tc)
            TOriginal = table();
            arrowRecordBatch = arrow.recordbatch(TOriginal);
            TConverted = arrowRecordBatch.toMATLAB();
            tc.verifyEqual(TOriginal, TConverted);
        end

        function EmptyRecordBatchColumnIndexError(tc)
            TOriginal = table();
            arrowRecordBatch = arrow.recordbatch(TOriginal);
            fcn = @() arrowRecordBatch.column(1);
            tc.verifyError(fcn, "arrow:tabular:recordbatch:NumericIndexWithEmptyRecordBatch");
        end

        function InvalidNumericIndexError(tc)
            TOriginal = table(1, 2, 3);
            arrowRecordBatch = arrow.recordbatch(TOriginal);
            fcn = @() arrowRecordBatch.column(4);
            tc.verifyError(fcn, "arrow:tabular:recordbatch:InvalidNumericColumnIndex");
        end

        function UnsupportedColumnIndexType(tc)
            TOriginal = table(1, 2, 3);
            arrowRecordBatch = arrow.recordbatch(TOriginal);
            fcn = @() arrowRecordBatch.column(datetime(2022, 1, 3));
            tc.verifyError(fcn, "arrow:badsubscript:NonNumeric");
        end

        function ErrorIfIndexIsNonScalar(tc)
            TOriginal = table(1, 2, 3);
            arrowRecordBatch = arrow.recordbatch(TOriginal);
            fcn = @() arrowRecordBatch.column([1 2]);
            tc.verifyError(fcn, "MATLAB:expectedScalar");
        end

        function ErrorIfIndexIsNonPositive(tc)
            TOriginal = table(1, 2, 3);
            arrowRecordBatch = arrow.recordbatch(TOriginal);
            fcn = @() arrowRecordBatch.column(-1);
            tc.verifyError(fcn, "arrow:badsubscript:NonPositive");
        end

        function FromArraysColumnNamesNotProvided(tc)
        % Verify arrow.tabular.RecordBatch.fromArrays creates the expected
        % RecordBatch when given a comma-separated list of
        % arrow.array.Array values.
            import arrow.tabular.RecordBatch

            TOriginal = createTableWithAllSupportedTypes();

            arrowArrays = cell([1 width(TOriginal)]);
            for ii = 1:width(TOriginal)
                arrowArrays{ii} = arrow.array(TOriginal.(ii));
            end

            arrowRecordBatch = RecordBatch.fromArrays(arrowArrays{:});
            expectedColumnNames = compose("Column%d", 1:13);
            TOriginal.Properties.VariableNames = expectedColumnNames;
            tc.verifyRecordBatch(arrowRecordBatch, expectedColumnNames, TOriginal);
        end

        function FromArraysWithColumnNamesProvided(tc)
        % Verify arrow.tabular.RecordBatch.fromArrays creates the expected
        % RecordBatch when given a comma-separated list of
        % arrow.array.Array values and the ColumnNames nv-pair is provided.
            import arrow.tabular.RecordBatch

            TOriginal = createTableWithAllSupportedTypes();

            arrowArrays = cell([1 width(TOriginal)]);
            for ii = 1:width(TOriginal)
                arrowArrays{ii} = arrow.array(TOriginal.(ii));
            end

            columnNames = string(char(65:77)')';
            arrowRecordBatch = RecordBatch.fromArrays(arrowArrays{:}, ColumnNames=columnNames);
            TOriginal.Properties.VariableNames = columnNames;
            tc.verifyRecordBatch(arrowRecordBatch, columnNames, TOriginal);
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
    end

    methods
        function verifyRecordBatch(tc, recordBatch, expectedColumnNames, expectedTable)
            tc.verifyEqual(recordBatch.NumColumns, int32(width(expectedTable)));
            tc.verifyEqual(recordBatch.ColumnNames, expectedColumnNames);
            convertedTable = recordBatch.table();
            tc.verifyEqual(convertedTable, expectedTable);
             for ii = 1:recordBatch.NumColumns
                column = recordBatch.column(ii);
                tc.verifyEqual(column.toMATLAB(), expectedTable{:, ii});
                traits = arrow.type.traits.traits(string(class(expectedTable{:, ii})));
                tc.verifyInstanceOf(column, traits.ArrayClassName);
             end
        end
    end
end

function T = createTableWithAllSupportedTypes()
    T = table(int8   ([1, 2, 3]'), ...
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
end

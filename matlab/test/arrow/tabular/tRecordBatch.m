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

            TOriginal = createTableWithSupportedTypes();
            arrowRecordBatch = arrow.recordBatch(TOriginal);
            expectedColumnNames = string(TOriginal.Properties.VariableNames);
            tc.verifyRecordBatch(arrowRecordBatch, expectedColumnNames, TOriginal);
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
            tc.verifyError(fcn, "arrow:badsubscript:NonNumeric");
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
            import arrow.internal.test.tabular.createTableWithSupportedTypes

            TOriginal = createTableWithSupportedTypes();

            arrowArrays = cell([1 width(TOriginal)]);
            for ii = 1:width(TOriginal)
                arrowArrays{ii} = arrow.array(TOriginal.(ii));
            end

            arrowRecordBatch = RecordBatch.fromArrays(arrowArrays{:});
            expectedColumnNames = compose("Column%d", 1:width(TOriginal));
            TOriginal.Properties.VariableNames = expectedColumnNames;
            tc.verifyRecordBatch(arrowRecordBatch, expectedColumnNames, TOriginal);
        end

        function FromArraysWithColumnNamesProvided(tc)
        % Verify arrow.tabular.RecordBatch.fromArrays creates the expected
        % RecordBatch when given a comma-separated list of
        % arrow.array.Array values and the ColumnNames nv-pair is provided.
            import arrow.tabular.RecordBatch
            import arrow.internal.test.tabular.createTableWithSupportedTypes

            TOriginal = createTableWithSupportedTypes();

            arrowArrays = cell([1 width(TOriginal)]);
            for ii = 1:width(TOriginal)
                arrowArrays{ii} = arrow.array(TOriginal.(ii));
            end

            columnNames = compose("MyVar%d", 1:numel(arrowArrays));
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


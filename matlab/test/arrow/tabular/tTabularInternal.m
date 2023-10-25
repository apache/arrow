%TTABULARINTERNAL Unit tests for internal functionality of tabular types.

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

classdef tTabularInternal < matlab.unittest.TestCase

    properties(TestParameter)
        TabularObjectWithAllTypes

        TabularObjectWithOneColumn

        TabularObjectWithThreeRows
    end

    methods (TestParameterDefinition, Static)
        function TabularObjectWithAllTypes = initializeTabularObjectWithAllTypes()
            arrays = arrow.internal.test.tabular.createAllSupportedArrayTypes(NumRows=1);
            arrowTable = arrow.tabular.Table.fromArrays(arrays{:});
            arrowRecordBatch = arrow.tabular.Table.fromArrays(arrays{:});
            TabularObjectWithAllTypes = struct(Table=arrowTable, ...
                RecordBatch=arrowRecordBatch);
        end

        function TabularObjectWithOneColumn = initializeTabularObjectWithOneColumn()
            t = table((1:3)');
            arrowTable = arrow.table(t);
            arrowRecordBatch = arrow.recordBatch(t);
            TabularObjectWithOneColumn = struct(Table=arrowTable, ...
                RecordBatch=arrowRecordBatch);
        end

        function TabularObjectWithThreeRows = initializeTabularObjectWithThreeRows()
            t = table((1:3)', ["A"; "B"; "C"]);
            arrowTable = arrow.table(t);
            arrowRecordBatch = arrow.recordBatch(t);
            TabularObjectWithThreeRows = struct(Table=arrowTable, ...
                RecordBatch=arrowRecordBatch);
        end
    end

    methods (Test)
        function RowWithAllTypes(testCase, TabularObjectWithAllTypes)
            % Verify getRowString successfully returns the expected string
            % when called on a Table/RecordBatch that contains all
            % supported array types.
            proxy = TabularObjectWithAllTypes.Proxy;
            columnStrs = ["false", "2024-02-23", "2023-08-24", "78", "38", ...
                          "24", "48", "89", "102", "<List>", """107""", "<Struct>", ...
                          "00:03:44", "00:00:07.000000", "2024-02-10 00:00:00.000000", ...
                          "107", "143", "36", "51"];
            expectedString = strjoin(columnStrs, " | ");
            actualString = proxy.getRowString(struct(Index=int64(1)));
            testCase.verifyEqual(actualString, expectedString);
        end

        function RowWithOneColumn(testCase, TabularObjectWithOneColumn)
            % Verify getRowString successfully returns the expected string
            % when called on a Table/RecordBatch with one column.
            proxy = TabularObjectWithOneColumn.Proxy;
            expectedString = "1";
            actualString = proxy.getRowString(struct(Index=int64(1)));
            testCase.verifyEqual(actualString, expectedString);
        end

        function RowIndex(testCase, TabularObjectWithThreeRows)
            % Verify getRowString returns the expected string when provided
            % for each row index value supplied.
            proxy = TabularObjectWithThreeRows.Proxy;

            actualString = proxy.getRowString(struct(Index=int64(1)));
            expectedString = "1 | ""A""";
            testCase.verifyEqual(actualString, expectedString);

            actualString = proxy.getRowString(struct(Index=int64(2)));
            expectedString = "2 | ""B""";
            testCase.verifyEqual(actualString, expectedString);

            actualString = proxy.getRowString(struct(Index=int64(3)));
            expectedString = "3 | ""C""";
            testCase.verifyEqual(actualString, expectedString);
        end

        function PrintRowFailed(testCase, TabularObjectWithThreeRows)
            % Verify getRowString throws an error with the ID
            % arrow:tabular:PrintRowFailed if provided invalid index
            % values.
            proxy = TabularObjectWithThreeRows.Proxy;
            fcn = @() proxy.getRowString(struct(Index=int64(0)));
            testCase.verifyError(fcn, "arrow:tabular:PrintRowFailed");

            fcn = @() proxy.getRowString(struct(Index=int64(4)));
            testCase.verifyError(fcn, "arrow:tabular:PrintRowFailed");
        end

    end

end
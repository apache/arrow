%TTABULARINTERNAL Unit tests for internal tabular functionality.

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
            TabularObjectWithAllTypes = {arrow.tabular.Table.fromArrays(arrays{:})};
        end

        function TabularObjectWithOneColumn = initializeTabularObjectWithOneColumn()
            TabularObjectWithOneColumn = {arrow.table(table((1:3)'))};
        end

        function TabularObjectWithThreeRows = initializeTabularObjectWithThreeRows()
            TabularObjectWithThreeRows = {arrow.table(table((1:3)', ["A"; "B"; "C"]))};
        end
    end

    methods (Test)
        function RowWithAllTypes(testCase, TabularObjectWithAllTypes)
            proxy = TabularObjectWithAllTypes.Proxy;
            columnStrs = ["false", "2024-02-23", "2023-08-24", "78", "38", ...
                          "24", "48", "89", "102", "<List>", """107""", "<Struct>", ...
                          "00:03:44", "00:00:07.000000", "2024-02-10 00:00:00.000000", ...
                          "107", "143", "36", "51"];
            expectedString = strjoin(columnStrs, " | ");
            actualString = proxy.getRowString(struct(Index=int64(1)));
            testCase.verifyEqual(actualString, expectedString);
        end

        function RowWithOneField(testCase, TabularObjectWithOneColumn)
            proxy = TabularObjectWithOneColumn.Proxy;
            expectedString = "1";
            actualString = proxy.getRowString(struct(Index=int64(1)));
            testCase.verifyEqual(actualString, expectedString);
        end

        function RowIndex(testCase, TabularObjectWithThreeRows)
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

        function TabularPrettyPrintRowFailed(testCase, TabularObjectWithThreeRows)
            proxy = TabularObjectWithThreeRows.Proxy;
            fcn = @() proxy.getRowString(struct(Index=int64(0)));
            testCase.verifyError(fcn, "arrow:tabular:PrintRowFailed");

            fcn = @() proxy.getRowString(struct(Index=int64(4)));
            testCase.verifyError(fcn, "arrow:tabular:PrintRowFailed");
        end

    end

end
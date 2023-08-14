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
            TOriginal = table(int8   ([1, 2, 3]'), ...
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
            arrowRecordBatch = arrow.recordbatch(TOriginal);
            TConverted = arrowRecordBatch.toMATLAB();
            tc.verifyEqual(TOriginal, TConverted);
            for ii = 1:arrowRecordBatch.NumColumns
                column = arrowRecordBatch.column(ii);
                tc.verifyEqual(column.toMATLAB(), TOriginal{:, ii});
                traits = arrow.type.traits.traits(string(class(TOriginal{:, ii})));
                tc.verifyInstanceOf(column, traits.ArrayClassName);
            end
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
    end
end

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

classdef tTable < hTabular


    properties
        FromArraysFcn = @arrow.tabular.Table.fromArrays
        ConstructionFcn = @arrow.table
        ClassName = "arrow.tabular.Table"
    end


    methods(Test)

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
            import arrow.type.traits.traits

            matlabTable = table([1; 2], ["A"; "B"], VariableNames=["Number" "Letter"]);
            recordBatch = arrow.recordBatch(matlabTable);
            arrowTable = Table.fromRecordBatches(recordBatch);
            expectedColumnTraits = varfun(@(var) traits(string(class(var))), ...
                matlabTable, OutputFormat="cell");
            testCase.verifyTabularObject(arrowTable, ["Number", "Letter"], expectedColumnTraits, matlabTable);
        end

        function FromRecordBatchesMultipleInputs(testCase)
            % Verify the arrow.tabular.Table.fromRecordBatches function
            % returns the expected arrow.tabular.Table instance when 
            % provided mulitple RecordBatches as input.
            import arrow.tabular.Table
            import arrow.type.traits.traits

            matlabTable1 = table([1; 2], ["A"; "B"], VariableNames=["Number" "Letter"]);
            matlabTable2 = table([10; 20; 30], ["A1"; "B1"; "C1"], VariableNames=["Number" "Letter"]);
            matlabTable3 = table([100; 200], ["A2"; "B2"], VariableNames=["Number" "Letter"]);

            recordBatch1 = arrow.recordBatch(matlabTable1);
            recordBatch2 = arrow.recordBatch(matlabTable2);
            recordBatch3 = arrow.recordBatch(matlabTable3);

            arrowTable = Table.fromRecordBatches(recordBatch1, recordBatch2, recordBatch3);
            expectedColumnTraits = varfun(@(var) traits(string(class(var))), ...
                matlabTable1, OutputFormat="cell");
            testCase.verifyTabularObject(arrowTable, ["Number", "Letter"], expectedColumnTraits, [matlabTable1; matlabTable2; matlabTable3]);
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

        function verifyTabularObject(tc, arrowTabularObj, columnNames, columnTraits, matlabTable)
            tc.verifyInstanceOf(arrowTabularObj, tc.ClassName);
            tc.verifyEqual(arrowTabularObj.NumColumns, int32(width(matlabTable)));
            tc.verifyEqual(arrowTabularObj.ColumnNames, columnNames);
            convertedTable = arrowTabularObj.table();
            tc.verifyEqual(convertedTable, matlabTable);
            for ii = 1:arrowTabularObj.NumColumns
                column = arrowTabularObj.column(ii);
                tc.verifyEqual(column.toMATLAB(), matlabTable{:, ii});
                tc.verifyInstanceOf(column, "arrow.array.ChunkedArray");
                tc.verifyInstanceOf(column.Type, columnTraits{ii}.TypeClassName);
             end
        end
        
        function col = makeColumnFromArray(~, array)
            % Each column in an arrow.tabular.Table is an
            % arrow.array.ChunkedArray.
            col = arrow.array.ChunkedArray.fromArrays(array);
        end

    end
end

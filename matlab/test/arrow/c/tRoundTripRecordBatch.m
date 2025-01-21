%TROUNDTRIPRECORDBATCH Tests for roundtripping RecordBatches using 
% the C Data Interface format.

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
classdef tRoundTripRecordBatch < matlab.unittest.TestCase

    methods (Test)
        function ZeroColumnRecordBatch(testCase)
            expected = arrow.recordBatch(table());
           
            cArray = arrow.c.Array();
            cSchema = arrow.c.Schema();
            expected.export(cArray.Address, cSchema.Address);
            actual = arrow.tabular.RecordBatch.import(cArray, cSchema);

            testCase.verifyEqual(actual, expected);
        end

        function ZeroRowRecordBatch(testCase)
            doubleArray = arrow.array([]);
            stringArray = arrow.array(string.empty(0, 0));
            expected = arrow.tabular.RecordBatch.fromArrays(doubleArray, stringArray);
            
            cArray = arrow.c.Array();
            cSchema = arrow.c.Schema();
            expected.export(cArray.Address, cSchema.Address);
            actual = arrow.tabular.RecordBatch.import(cArray, cSchema);

            testCase.verifyEqual(actual, expected);
        end

        function OneRowRecordBatch(testCase)
            varNames = ["Col1" "Col2" "Col3"];
            t = table(1, "A", false, VariableNames=varNames);
            expected = arrow.recordBatch(t);

            cArray = arrow.c.Array();
            cSchema = arrow.c.Schema();
            expected.export(cArray.Address, cSchema.Address);
            actual = arrow.tabular.RecordBatch.import(cArray, cSchema);

            testCase.verifyEqual(actual, expected);
        end

        function MultiRowRecordBatch(testCase)
            varNames = ["Col1" "Col2" "Col3"];
            t = table((1:3)', ["A"; "B"; "C"], [false; true; false],...
                VariableNames=varNames);
            expected = arrow.recordBatch(t);

            cArray = arrow.c.Array();
            cSchema = arrow.c.Schema();
            expected.export(cArray.Address, cSchema.Address);
            actual = arrow.tabular.RecordBatch.import(cArray, cSchema);

            testCase.verifyEqual(actual, expected);
        end

         function ExportErrorWrongInputTypes(testCase)
            rb = arrow.recordBatch(table([1; 2; 3]));
            fcn = @() rb.export("cArray.Address", "cSchema.Address");
            testCase.verifyError(fcn, "MATLAB:validation:UnableToConvert");
        end

        function ExportTooFewInputs(testCase)
            rb = arrow.recordBatch(table([1; 2; 3]));
            fcn = @() rb.export();
            testCase.verifyError(fcn, "MATLAB:minrhs");
        end

        function ExportTooManyInputs(testCase)
            rb = arrow.recordBatch(table([1; 2; 3]));
            fcn = @() rb.export("A", "B", "C");
            testCase.verifyError(fcn, "MATLAB:TooManyInputs");
        end

        function ImportErrorWrongInputTypes(testCase)
            cArray = "arrow.c.Array";
            cSchema = "arrow.c.Schema";
            fcn = @() arrow.tabular.RecordBatch.import(cArray, cSchema);
            testCase.verifyError(fcn, "MATLAB:validation:UnableToConvert");
        end

        function ImportTooFewInputs(testCase)
            fcn = @() arrow.tabular.RecordBatch.import();
            testCase.verifyError(fcn, "MATLAB:minrhs");
        end

        function ImportTooManyInputs(testCase)
            fcn = @() arrow.tabular.RecordBatch.import("A", "B", "C");
            testCase.verifyError(fcn, "MATLAB:TooManyInputs");
        end

        function ImportErrorImportFailed(testCase)
            cArray = arrow.c.Array();
            cSchema = arrow.c.Schema();
            % An arrow:c:import:ImportFailed error should be thrown
            % if the supplied arrow.c.Array and arrow.c.Schema were
            % never populated previously from an exported Array.
            fcn = @() arrow.tabular.RecordBatch.import(cArray, cSchema);
            testCase.verifyError(fcn, "arrow:c:import:ImportFailed");
        end

        function ImportErrorInvalidSchema(testCase)
            cArray = arrow.c.Array();
            cSchema = arrow.c.Schema();
            % An arrow:c:import:ImportFailed error should be thrown
            % if the supplied arrow.c.Schema was not populated from a 
            % struct-like type (i.e. StructArray or RecordBatch).
            a = arrow.array(1:3);
            a.export(cArray.Address, cSchema.Address);
            fcn = @() arrow.tabular.RecordBatch.import(cArray, cSchema);
            testCase.verifyError(fcn, "arrow:c:import:ImportFailed");
        end

        function ImportFromStructArray(testCase)
            % Verify a StructArray exported via the C Data Interface format
            % can be imported as a RecordBatch.
            field1 = arrow.array(1:3);

            field2 = arrow.array(["A" "B" "C"]);
            structArray = arrow.array.StructArray.fromArrays(field1, field2, ...
                FieldNames=["Number" "Text"]);
            
            cArray = arrow.c.Array();
            cSchema = arrow.c.Schema();
            structArray.export(cArray.Address, cSchema.Address)
            rb = arrow.tabular.RecordBatch.import(cArray, cSchema);

            expected = arrow.tabular.RecordBatch.fromArrays(field1, field2, ...
                ColumnNames=["Number" "Text"]);

            testCase.verifyEqual(rb, expected);
        end

        function ExportToStructArray(testCase)
            % Verify a RecordBatch exported via the C Data Interface
            % format can be imported as a StructArray.
            column1 = arrow.array(1:3);
            column2 = arrow.array(["A" "B" "C"]);
            rb = arrow.tabular.RecordBatch.fromArrays(column1, column2, ...
                ColumnNames=["Number" "Text"]);
            
            cArray = arrow.c.Array();
            cSchema = arrow.c.Schema();
            rb.export(cArray.Address, cSchema.Address)
            structArray = arrow.array.Array.import(cArray, cSchema);

            expected = arrow.array.StructArray.fromArrays(column1, column2, ...
                FieldNames=["Number" "Text"]);

            testCase.verifyEqual(structArray, expected);
        end

    end

end
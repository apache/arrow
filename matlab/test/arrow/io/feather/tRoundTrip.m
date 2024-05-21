%TROUNDTRIP Round trip tests for feather.

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
classdef tRoundTrip < matlab.unittest.TestCase

    methods(Test)
        function Basic(testCase)
            import matlab.unittest.fixtures.TemporaryFolderFixture
            import arrow.internal.io.feather.*

            fixture = testCase.applyFixture(TemporaryFolderFixture);
            filename = fullfile(fixture.Folder, "temp.feather");

            DoubleVar = [10; 20; 30; 40];
            SingleVar = single([10; 15; 20; 25]);

            tableWrite = table(DoubleVar, SingleVar);
            recordBatchWrite = arrow.recordBatch(tableWrite);

            writer = Writer(filename);
            writer.write(recordBatchWrite);

            reader = arrow.internal.io.feather.Reader(filename);
            recordBatchRead = reader.read();

            tableRead = table(recordBatchRead);

            testCase.verifyEqual(tableWrite, tableRead);
        end

        function NumericDatatypesNulls(testCase)
            % Verify integers with null values are roundtripped correctly.
            import matlab.unittest.fixtures.TemporaryFolderFixture
            import arrow.internal.io.feather.*
            import arrow.tabular.RecordBatch

            fixture = testCase.applyFixture(TemporaryFolderFixture);
            filename = fullfile(fixture.Folder, "temp.feather");

            uint8Array = arrow.array(uint8([1, 2, 3, 4, 5]), Valid=[1, 3, 4]);
            int16Array = arrow.array(uint8([1, 2, 3, 4, 5]), Valid=[3, 5]);
            inputRecordBatch = RecordBatch.fromArrays(uint8Array, int16Array);

            writer = Writer(filename);
            writer.write(inputRecordBatch);

            reader = Reader(filename);
            outputRecordBatch = reader.read();

            testCase.verifyEqual(outputRecordBatch.NumColumns, inputRecordBatch.NumColumns);
            testCase.verifyEqual(outputRecordBatch.ColumnNames, inputRecordBatch.ColumnNames);
            
            outColumn1 = outputRecordBatch.column(1);
            testCase.verifyEqual(outColumn1.Valid, uint8Array.Valid);
            testCase.verifyEqual(toMATLAB(outColumn1), toMATLAB(uint8Array));

            outColumn2 = outputRecordBatch.column(2);
            testCase.verifyEqual(outColumn2.Valid, int16Array.Valid);
            testCase.verifyEqual(toMATLAB(outColumn2), toMATLAB(int16Array));
        end

        function InvalidMATLABTableVariableNames(testCase)
            import matlab.unittest.fixtures.TemporaryFolderFixture
            import arrow.internal.io.feather.*
            import arrow.tabular.RecordBatch

            fixture = testCase.applyFixture(TemporaryFolderFixture);
            filename = fullfile(fixture.Folder, "temp.feather");

            doubleArray = arrow.array([1, 2, 3, 4, 5]); 
            stringArray = arrow.array(["A", "B", "C", "D", "E"]);

            % Create a RecordBatch whose first column name is an invalid
            % variable name for a MATLAB table.
            inputRecordBatch = RecordBatch.fromArrays(doubleArray, stringArray, ...
                ColumnNames=[":", "Valid"]);

            writer = Writer(filename);
            writer.write(inputRecordBatch);

            % Verify the RecordBatch returned by read has the same
            % ColumnNames as the original RecordBatch.
            reader = Reader(filename);
            outputRecordBatch = reader.read();
            testCase.verifyEqual(outputRecordBatch.ColumnNames, inputRecordBatch.ColumnNames);
            
            % Verify the VariableNames property in the table returned by
            % featherread has been modified and that 
            % VariableDescriptions is set property.
            outputTable = featherread(filename);
            testCase.verifyEqual(outputTable.Properties.VariableNames, {':_1', 'Valid'});
            testCase.verifyEqual(outputTable.Properties.VariableDescriptions, ...
                {'Original variable name: '':''', ''});
        end
    end
end

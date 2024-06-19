%TRECORDBATCHFILEREADER Unit tests for arrow.io.ipc.RecordBatchFileReader.

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
classdef tRecordBatchFileReader < matlab.unittest.TestCase

    properties
        DataFolder
        ZeroBatchFile
        OneBatchFile
        MultipleBatchFile
    end

    methods(TestClassSetup)
        function setupDataFolder(testCase)
            import matlab.unittest.fixtures.TemporaryFolderFixture
            fixture = testCase.applyFixture(TemporaryFolderFixture);
            testCase.DataFolder = string(fixture.Folder);
        end

        function setupZeroBatchFile(testCase)
            fieldA = arrow.field("A", arrow.string());
            fieldB = arrow.field("B", arrow.float32());
            schema = arrow.schema([fieldA, fieldB]);
            fname = fullfile(testCase.DataFolder, "ZeroBatchFile.arrow");
            writer = arrow.io.ipc.RecordBatchFileWriter(fname, schema);
            writer.close();
            testCase.ZeroBatchFile = fname;
        end

        function setupOneBatchFile(testCase)
            t = table(["Row1"; "Row2"], single([1; 2]), VariableNames=["A", "B"]);
            recordBatch = arrow.recordBatch(t);
            fname = fullfile(testCase.DataFolder, "OneBatchFile.arrow");
            writer = arrow.io.ipc.RecordBatchFileWriter(fname, recordBatch.Schema);
            writer.writeRecordBatch(recordBatch);
            writer.close();
            testCase.OneBatchFile = fname;
        end

        function setupMultipleBatchFile(testCase)
            t1 = table(["Row1"; "Row2"], single([1; 2]), VariableNames=["A", "B"]);
            t2 = table(["Row3"; "Row4"], single([3; 4]), VariableNames=["A", "B"]);
            recordBatch1 = arrow.recordBatch(t1);
            recordBatch2 = arrow.recordBatch(t2);
            fname = fullfile(testCase.DataFolder, "MultipleBatchFile.arrow");
            writer = arrow.io.ipc.RecordBatchFileWriter(fname, recordBatch1.Schema);
            writer.writeRecordBatch(recordBatch1);
            writer.writeRecordBatch(recordBatch2);
            writer.close();
            testCase.MultipleBatchFile = fname;
        end
    end

    methods (Test)
        function ZeroLengthFilenameError(testCase)
            % Verify RecordBatchFileReader throws an exception with the
            % identifier MATLAB:validators:mustBeNonzeroLengthText if the
            % filename input argument given is a zero length string.
            fcn = @() arrow.io.ipc.RecordBatchFileReader("");
            testCase.verifyError(fcn, "MATLAB:validators:mustBeNonzeroLengthText");
        end

        function MissingStringFilenameError(testCase)
            % Verify RecordBatchFileReader throws an exception with the
            % identifier MATLAB:validators:mustBeNonzeroLengthText if the
            % filename input argument given is a missing string.
            fcn = @() arrow.io.ipc.RecordBatchFileReader(string(missing));
            testCase.verifyError(fcn, "MATLAB:validators:mustBeNonzeroLengthText");
        end

        function FilenameInvalidTypeError(testCase)
            % Verify RecordBatchFileReader throws an exception with the
            % identifier MATLAB:validators:UnableToConvert if the filename
            % input argument is neither a scalar string nor a char vector.
            fcn = @() arrow.io.ipc.RecordBatchFileReader(table);
            testCase.verifyError(fcn, "MATLAB:validation:UnableToConvert");
        end

        function NumRecordBatches(testCase)
            % Verify the getter method for NumRecordBatches returns the
            % expected value.
            reader = arrow.io.ipc.RecordBatchFileReader(testCase.ZeroBatchFile);
            testCase.verifyEqual(reader.NumRecordBatches, int32(0));

            reader = arrow.io.ipc.RecordBatchFileReader(testCase.OneBatchFile);
            testCase.verifyEqual(reader.NumRecordBatches, int32(1));

             reader = arrow.io.ipc.RecordBatchFileReader(testCase.MultipleBatchFile);
            testCase.verifyEqual(reader.NumRecordBatches, int32(2));
        end

        function NumRecordNoSetter(testCase)
            % Verify the NumRecordBatches property is not settable.
            reader = arrow.io.ipc.RecordBatchFileReader(testCase.ZeroBatchFile);
            testCase.verifyError(@() setfield(reader, "NumRecordBatches", int32(10)), "MATLAB:class:SetProhibited");
        end

        function Schema(testCase)
            % Verify the getter method for Schema returns the
            % expected value.
            fieldA = arrow.field("A", arrow.string());
            fieldB = arrow.field("B", arrow.float32());
            expectedSchema = arrow.schema([fieldA fieldB]);
            
            reader = arrow.io.ipc.RecordBatchFileReader(testCase.ZeroBatchFile);
            testCase.verifyEqual(reader.Schema, expectedSchema);

            reader = arrow.io.ipc.RecordBatchFileReader(testCase.OneBatchFile);
            testCase.verifyEqual(reader.Schema, expectedSchema);

            reader = arrow.io.ipc.RecordBatchFileReader(testCase.MultipleBatchFile);
            testCase.verifyEqual(reader.Schema, expectedSchema);
        end

        function SchemaNoSetter(testCase)
            % Verify the Schema property is not settable.
            fieldC = arrow.field("C", arrow.date32());
            schema = arrow.schema(fieldC);
            reader = arrow.io.ipc.RecordBatchFileReader(testCase.ZeroBatchFile);
            testCase.verifyError(@() setfield(reader, "Schema", schema), "MATLAB:class:SetProhibited");
        end

        function readInvalidIndexType(testCase)
            % Verify read throws an exception with the identifier
            % arrow:badsubscript:NonNumeric if the index argument given is
            % not numeric.
            reader = arrow.io.ipc.RecordBatchFileReader(testCase.MultipleBatchFile);

            fcn = @() reader.read("index");
            testCase.verifyError(fcn, "arrow:badsubscript:NonNumeric");
        end

        function readInvalidNumericIndex(testCase)
            % Verify read throws an exception if the index argument given
            % is nonpositive or noninteger number.
            reader = arrow.io.ipc.RecordBatchFileReader(testCase.MultipleBatchFile);
            fcn = @() reader.read(-1);
            testCase.verifyError(fcn, "arrow:badsubscript:NonPositive");
            fcn = @() reader.read(0);
            testCase.verifyError(fcn, "arrow:badsubscript:NonPositive");
            fcn = @() reader.read(1.1);
            testCase.verifyError(fcn, "arrow:badsubscript:NonInteger");
        end

        function readInvalidNumericIndexValue(testCase)
            % Verify read throws an exception with the identifier 
            % arrow:io:ipc:InvalidIndex if the index given is greater
            % than the NumRecordBatches in the file.
            reader = arrow.io.ipc.RecordBatchFileReader(testCase.MultipleBatchFile);
            fcn = @() reader.read(3);
            testCase.verifyError(fcn, "arrow:io:ipc:InvalidIndex");
        end

        function readAtIndex(testCase)
            % Verify read returns the expected RecordBatch for the given
            % index.
            t1 = table(["Row1"; "Row2"], single([1; 2]), VariableNames=["A", "B"]);
            t2 = table(["Row3"; "Row4"], single([3; 4]), VariableNames=["A", "B"]);

            reader = arrow.io.ipc.RecordBatchFileReader(testCase.MultipleBatchFile);
            
            actual1 = reader.read(1);
            expected1 = arrow.recordBatch(t1);
            testCase.verifyEqual(actual1, expected1);

            actual2 = reader.read(2);
            expected2 = arrow.recordBatch(t2);
            testCase.verifyEqual(actual2, expected2);
        end
    end
end
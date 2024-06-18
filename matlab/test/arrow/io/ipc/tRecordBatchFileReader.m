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
            testCase.OneBatchFile = fname;
        end
    end

    methods (Test)
        function ZeroLengthFilenameError(testCase)
            fcn = @() arrow.io.ipc.RecordBatchFileReader("");
            testCase.verifyError(fcn, "MATLAB:validators:mustBeNonzeroLengthText");
        end

        function MissingStringFilenameError(testCase)
            fcn = @() arrow.io.ipc.RecordBatchFileReader(string(missing));
            testCase.verifyError(fcn, "MATLAB:validators:mustBeNonzeroLengthText");
        end

        function FilenameInvalidTypeError(testCase)
            fcn = @() arrow.io.ipc.RecordBatchFileReader(table);
            testCase.verifyError(fcn, "MATLAB:validation:UnableToConvert");
        end
    end
end
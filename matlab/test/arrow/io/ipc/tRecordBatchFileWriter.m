%TRECORDBATCHFILEWRITER Unit tests for arrow.io.ipc.RecordBatchFileWriter.

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

classdef tRecordBatchFileWriter < matlab.unittest.TestCase

    methods
        function folder = setupTemporaryFolder(testCase)
            import matlab.unittest.fixtures.TemporaryFolderFixture
            fixture = testCase.applyFixture(TemporaryFolderFixture);
            folder = string(fixture.Folder);
        end
    end

    methods (Test)
        function ZeroLengthFilenameError(testCase)
            schema = arrow.schema(arrow.field("A", arrow.float64()));
            fcn = @() arrow.io.ipc.RecordBatchFileWriter("", schema);
            testCase.verifyError(fcn, "MATLAB:validators:mustBeNonzeroLengthText");
        end

        function MissingStringFilenameError(testCase)
            schema = arrow.schema(arrow.field("A", arrow.float64()));
            fcn = @() arrow.io.ipc.RecordBatchFileWriter(string(missing), schema);
            testCase.verifyError(fcn, "MATLAB:validators:mustBeNonzeroLengthText");
        end

        function FilenameInvalidTypeError(testCase)
            schema = arrow.schema(arrow.field("A", arrow.float64()));
            fcn = @() arrow.io.ipc.RecordBatchFileWriter(table, schema);
            testCase.verifyError(fcn, "MATLAB:validation:UnableToConvert");
        end

        function InvalidSchemaType(testCase)
            folder = testCase.setupTemporaryFolder();
            fname = fullfile(folder, "data.arrow");
            schema = arrow.field("A", arrow.float64());
            fcn = @() arrow.io.ipc.RecordBatchFileWriter(fname, schema);
            testCase.verifyError(fcn, "MATLAB:validation:UnableToConvert");
        end

        function writeRecordBatchInvalidType(testCase)
            folder = testCase.setupTemporaryFolder();
            fname = fullfile(folder, "data.arrow");
            schema = arrow.schema(arrow.field("A", arrow.float64()));
            writer = arrow.io.ipc.RecordBatchFileWriter(fname, schema);
            arrowTable = arrow.table(table([1 2 3 4]', VariableNames="A"));
            fcn = @() writer.writeRecordBatch(arrowTable);
            testCase.verifyError(fcn, "MATLAB:validation:UnableToConvert");
        end

        function writeTableInvalidType(testCase)
            folder = testCase.setupTemporaryFolder();
            fname = fullfile(folder, "data.arrow");
            schema = arrow.schema(arrow.field("A", arrow.float64()));
            writer = arrow.io.ipc.RecordBatchFileWriter(fname, schema);
            arrowRecordBatch = arrow.recordBatch(table([1 2 3 4]', VariableNames="A"));
            fcn = @() writer.writeTable(arrowRecordBatch);
            testCase.verifyError(fcn, "MATLAB:validation:UnableToConvert");
        end

        function writeInvalidType(testCase)
            folder = testCase.setupTemporaryFolder();
            fname = fullfile(folder, "data.arrow");
            schema = arrow.schema(arrow.field("A", arrow.float64()));
            writer = arrow.io.ipc.RecordBatchFileWriter(fname, schema);
            fcn = @() writer.write(schema);
            testCase.verifyError(fcn, "arrow:matlab:ipc:write:InvalidType");
        end
    end
end
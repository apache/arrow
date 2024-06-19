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
            % Verify RecordBatchFileWriter throws an exception with the
            % identifier MATLAB:validators:mustBeNonzeroLengthText if the
            % filename input argument given is a zero length string.
            schema = arrow.schema(arrow.field("A", arrow.float64()));
            fcn = @() arrow.io.ipc.RecordBatchFileWriter("", schema);
            testCase.verifyError(fcn, "MATLAB:validators:mustBeNonzeroLengthText");
        end

        function MissingStringFilenameError(testCase)
            % Verify RecordBatchFileWriter throws an exception with the
            % identifier MATLAB:validators:mustBeNonzeroLengthText if the
            % filename input argument given is  a missing string.
            schema = arrow.schema(arrow.field("A", arrow.float64()));
            fcn = @() arrow.io.ipc.RecordBatchFileWriter(string(missing), schema);
            testCase.verifyError(fcn, "MATLAB:validators:mustBeNonzeroLengthText");
        end

        function FilenameInvalidTypeError(testCase)
            % Verify RecordBatchFileWriter throws an exception with the
            % identifier MATLAB:validators:UnableToConvert if the filename
            % input argument is neither a scalar string nor a char vector.
            schema = arrow.schema(arrow.field("A", arrow.float64()));
            fcn = @() arrow.io.ipc.RecordBatchFileWriter(table, schema);
            testCase.verifyError(fcn, "MATLAB:validation:UnableToConvert");
        end

        function InvalidSchemaType(testCase)
            % Verify RecordBatchFileWriter throws an exception with the
            % identifier MATLAB:validators:UnableToConvert if the schema
            % input argument is not an arrow.tabular.Schema instance.
            folder = testCase.setupTemporaryFolder();
            fname = fullfile(folder, "data.arrow");
            schema = arrow.field("A", arrow.float64());
            fcn = @() arrow.io.ipc.RecordBatchFileWriter(fname, schema);
            testCase.verifyError(fcn, "MATLAB:validation:UnableToConvert");
        end

        function writeRecordBatchInvalidType(testCase)
            % Verify writeRecordBatch throws an exception with the
            % identifier MATLAB:validators:UnableToConvert if the
            % recordBatch input argument given is not an
            % arrow.tabular.RecordBatch instance.
            folder = testCase.setupTemporaryFolder();
            fname = fullfile(folder, "data.arrow");
            schema = arrow.schema(arrow.field("A", arrow.float64()));
            writer = arrow.io.ipc.RecordBatchFileWriter(fname, schema);
            arrowTable = arrow.table(table([1 2 3 4]', VariableNames="A"));
            fcn = @() writer.writeRecordBatch(arrowTable);
            testCase.verifyError(fcn, "MATLAB:validation:UnableToConvert");
        end

        function writeTableInvalidType(testCase)
            % Verify writeTable throws an exception with the
            % identifier MATLAB:validators:UnableToConvert if the table 
            % input argument given is not an arrow.tabular.Table instance.
            folder = testCase.setupTemporaryFolder();
            fname = fullfile(folder, "data.arrow");
            schema = arrow.schema(arrow.field("A", arrow.float64()));
            writer = arrow.io.ipc.RecordBatchFileWriter(fname, schema);
            arrowRecordBatch = arrow.recordBatch(table([1 2 3 4]', VariableNames="A"));
            fcn = @() writer.writeTable(arrowRecordBatch);
            testCase.verifyError(fcn, "MATLAB:validation:UnableToConvert");
        end

        function writeInvalidType(testCase)
            % Verify writeTable throws an exception with the
            % identifier arrow:matlab:ipc:write:InvalidType if the 
            % tabularObj input argument given is neither an
            % arrow.tabular.Table or an arrow.tabular.RecordBatch.
            folder = testCase.setupTemporaryFolder();
            fname = fullfile(folder, "data.arrow");
            schema = arrow.schema(arrow.field("A", arrow.float64()));
            writer = arrow.io.ipc.RecordBatchFileWriter(fname, schema);
            fcn = @() writer.write(schema);
            testCase.verifyError(fcn, "arrow:matlab:ipc:write:InvalidType");
        end

        function writeRecordBatchInvalidSchema(testCase)
            % Verify writeRecordBatch throws an exception with the
            % identifier arrow:io:ipc:FailedToWriteRecordBatch if the
            % schema of the given record batch does match the expected 
            % schema.
            folder = testCase.setupTemporaryFolder();
            fname = fullfile(folder, "data.arrow");
            schema = arrow.schema(arrow.field("A", arrow.float64()));
            writer = arrow.io.ipc.RecordBatchFileWriter(fname, schema);

            arrowRecordBatch = arrow.recordBatch(table([1 2 3 4]', VariableNames="B"));
            fcn = @() writer.writeRecordBatch(arrowRecordBatch);
            testCase.verifyError(fcn, "arrow:io:ipc:FailedToWriteRecordBatch");
        end

         function writeTableInvalidSchema(testCase)
            % Verify writeTable throws an exception with the
            % identifier arrow:io:ipc:FailedToWriteRecordBatch if the
            % schema of the given table does match the expected schema.
            folder = testCase.setupTemporaryFolder();
            fname = fullfile(folder, "data.arrow");
            schema = arrow.schema(arrow.field("A", arrow.float64()));
            writer = arrow.io.ipc.RecordBatchFileWriter(fname, schema);

            arrowTable = arrow.table(table([1 2 3 4]', VariableNames="B"));
            fcn = @() writer.writeTable(arrowTable);
            testCase.verifyError(fcn, "arrow:io:ipc:FailedToWriteRecordBatch");
         end

         function writeInvalidSchema(testCase)
            % Verify write throws an exception with the
            % identifier arrow:io:ipc:FailedToWriteRecordBatch if the
            % schema of the given record batch or table does match the 
            % expected schema.
            folder = testCase.setupTemporaryFolder();
            fname = fullfile(folder, "data.arrow");
            schema = arrow.schema(arrow.field("A", arrow.float64()));
            writer = arrow.io.ipc.RecordBatchFileWriter(fname, schema);

            arrowTable = arrow.table(table([1 2 3 4]', VariableNames="B"));
            fcn = @() writer.write(arrowTable);
            testCase.verifyError(fcn, "arrow:io:ipc:FailedToWriteRecordBatch");

            arrowRecordBatch = arrow.recordBatch(table([1 2 3 4]', VariableNames="B"));
            fcn = @() writer.write(arrowRecordBatch);
            testCase.verifyError(fcn, "arrow:io:ipc:FailedToWriteRecordBatch");
         end

         function writeRecordBatchSmoke(testCase)
            % Verify writeRecordBatch does not error or issue a warning
            % if it successfully writes the record batch to the file.
            folder = testCase.setupTemporaryFolder();
            fname = fullfile(folder, "data.arrow");
            schema = arrow.schema(arrow.field("A", arrow.float64()));
            writer = arrow.io.ipc.RecordBatchFileWriter(fname, schema);
            arrowRecordBatch = arrow.recordBatch(table([1 2 3 4]', VariableNames="A"));

            fcn = @() writer.writeRecordBatch(arrowRecordBatch);
            testCase.verifyWarningFree(fcn);
         end

        function writeTableBatchSmoke(testCase)
            % Verify writeTable does not error or issue a warning
            % if it successfully writes the table to the file.
            folder = testCase.setupTemporaryFolder();
            fname = fullfile(folder, "data.arrow");
            schema = arrow.schema(arrow.field("A", arrow.float64()));
            writer = arrow.io.ipc.RecordBatchFileWriter(fname, schema);
            arrowTable = arrow.table(table([1 2 3 4]', VariableNames="A"));

            fcn = @() writer.writeTable(arrowTable);
            testCase.verifyWarningFree(fcn);
        end

        function writeSmoke(testCase)
            % Verify write does not error or issue a warning if it
            % successfully writes the record batch or table to the file.
            folder = testCase.setupTemporaryFolder();
            fname = fullfile(folder, "data.arrow");
            schema = arrow.schema(arrow.field("A", arrow.float64()));
            writer = arrow.io.ipc.RecordBatchFileWriter(fname, schema);
            arrowRecordBatch = arrow.recordBatch(table([1 2 3 4]', VariableNames="A"));

            fcn = @() writer.write(arrowRecordBatch);
            testCase.verifyWarningFree(fcn);

            arrowTable = arrow.table(table([1 2 3 4]', VariableNames="A"));
            fcn = @() writer.write(arrowTable);
            testCase.verifyWarningFree(fcn);
        end

        function closeSmoke(testCase)
            % Verify close does not error or issue a warning if it was
            % successful.
            folder = testCase.setupTemporaryFolder();
            fname = fullfile(folder, "data.arrow");
            schema = arrow.schema(arrow.field("A", arrow.float64()));
            writer = arrow.io.ipc.RecordBatchFileWriter(fname, schema);
            arrowTable = arrow.table(table([1 2 3 4]', VariableNames="A"));
            writer.write(arrowTable);
            fcn = @() writer.close();
            testCase.verifyWarningFree(fcn);
        end
    end
end
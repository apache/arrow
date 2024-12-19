%TRECORDBATCHSTREAMREADER Unit tests for arrow.io.ipc.RecordBatchStreamReader.

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
classdef tRecordBatchStreamReader < matlab.unittest.TestCase

    properties
        DataFolder
        ZeroBatchStreamFile
        OneBatchStreamFile
        MultipleBatchStreamFile
        RandomAccessFile
    end

    properties (TestParameter)
        RecordBatchReadFcn = {@read, @readRecordBatch}
    end

    methods(TestClassSetup)

        function setupDataFolder(testCase)
            import matlab.unittest.fixtures.TemporaryFolderFixture
            fixture = testCase.applyFixture(TemporaryFolderFixture);
            testCase.DataFolder = string(fixture.Folder);
        end

        function setupRandomAccessFile(testCase)
            fieldA = arrow.field("A", arrow.string());
            fieldB = arrow.field("B", arrow.float32());
            schema = arrow.schema([fieldA, fieldB]);
            fname = fullfile(testCase.DataFolder, "RandomAccessFile.arrow");
            writer = arrow.io.ipc.RecordBatchFileWriter(fname, schema);
            writer.close();
            testCase.RandomAccessFile = fname;
        end

        function setupZeroBatchStreamFile(testCase)
            fieldA = arrow.field("A", arrow.string());
            fieldB = arrow.field("B", arrow.float32());
            schema = arrow.schema([fieldA, fieldB]);
            fname = fullfile(testCase.DataFolder, "ZeroBatchStreamFile.arrows");
            writer = arrow.io.ipc.RecordBatchStreamWriter(fname, schema);
            writer.close();
            testCase.ZeroBatchStreamFile = fname;
        end

        function setupOneBatchStreamFile(testCase)
            t = table(["Row1"; "Row2"], single([1; 2]), VariableNames=["A", "B"]);
            recordBatch = arrow.recordBatch(t);
            fname = fullfile(testCase.DataFolder, "OneBatchFile.arrows");
            writer = arrow.io.ipc.RecordBatchStreamWriter(fname, recordBatch.Schema);
            writer.writeRecordBatch(recordBatch);
            writer.close();
            testCase.OneBatchStreamFile = fname;
        end

        function setupMultipleBatchStreamFile(testCase)
            t1 = table(["Row1"; "Row2"], single([1; 2]), VariableNames=["A", "B"]);
            t2 = table(["Row3"; "Row4"], single([3; 4]), VariableNames=["A", "B"]);
            recordBatch1 = arrow.recordBatch(t1);
            recordBatch2 = arrow.recordBatch(t2);
            fname = fullfile(testCase.DataFolder, "MultipleBatchStreamFile.arrows");
            writer = arrow.io.ipc.RecordBatchStreamWriter(fname, recordBatch1.Schema);
            writer.writeRecordBatch(recordBatch1);
            writer.writeRecordBatch(recordBatch2);
            writer.close();
            testCase.MultipleBatchStreamFile = fname;
        end
    end

    methods (Test)

        function ZeroLengthFilenameError(testCase)
            % Verify RecordBatchStreamReader throws an exception with the
            % identifier MATLAB:validators:mustBeNonzeroLengthText if the
            % filename input argument given is a zero length string.
            fcn = @() arrow.io.ipc.RecordBatchStreamReader("");
            testCase.verifyError(fcn, "MATLAB:validators:mustBeNonzeroLengthText");
        end

        function MissingStringFilenameError(testCase)
            % Verify RecordBatchStreamReader throws an exception with the
            % identifier MATLAB:validators:mustBeNonzeroLengthText if the
            % filename input argument given is a missing string.
            fcn = @() arrow.io.ipc.RecordBatchStreamReader(string(missing));
            testCase.verifyError(fcn, "MATLAB:validators:mustBeNonzeroLengthText");
        end

        function FilenameInvalidTypeError(testCase)
            % Verify RecordBatchStreamReader throws an exception with the
            % identifier MATLAB:validators:UnableToConvert if the filename
            % input argument is neither a scalar string nor a char vector.
            fcn = @() arrow.io.ipc.RecordBatchStreamReader(table);
            testCase.verifyError(fcn, "MATLAB:validation:UnableToConvert");
        end

        function Schema(testCase)
            % Verify the getter method for Schema returns the
            % expected value.
            fieldA = arrow.field("A", arrow.string());
            fieldB = arrow.field("B", arrow.float32());
            expectedSchema = arrow.schema([fieldA fieldB]);

            reader = arrow.io.ipc.RecordBatchStreamReader(testCase.ZeroBatchStreamFile);
            testCase.verifyEqual(reader.Schema, expectedSchema);

            reader = arrow.io.ipc.RecordBatchStreamReader(testCase.OneBatchStreamFile);
            testCase.verifyEqual(reader.Schema, expectedSchema);

            reader = arrow.io.ipc.RecordBatchStreamReader(testCase.MultipleBatchStreamFile);
            testCase.verifyEqual(reader.Schema, expectedSchema);
        end

        function SchemaNoSetter(testCase)
            % Verify the Schema property is not settable.
            fieldC = arrow.field("C", arrow.date32());
            schema = arrow.schema(fieldC);
            reader = arrow.io.ipc.RecordBatchStreamReader(testCase.ZeroBatchStreamFile);
            testCase.verifyError(@() setfield(reader, "Schema", schema), "MATLAB:class:SetProhibited");
        end

        function ReadErrorIfEndOfStream(testCase, RecordBatchReadFcn)
            % Verify read throws an execption with the identifier arrow:io:ipc:EndOfStream
            % on an Arrow IPC Stream file containing zero batches.
            reader = arrow.io.ipc.RecordBatchStreamReader(testCase.ZeroBatchStreamFile);
            fcn = @() RecordBatchReadFcn(reader);
            testCase.verifyError(fcn, "arrow:io:ipc:EndOfStream");
        end

        function ReadOneBatchStreamFile(testCase, RecordBatchReadFcn)
            % Verify read can successfully read an Arrow IPC Stream file
            % containing one batch.
            reader = arrow.io.ipc.RecordBatchStreamReader(testCase.OneBatchStreamFile);

            expectedMatlabTable = table(["Row1"; "Row2"], single([1; 2]), VariableNames=["A", "B"]);
            expected = arrow.recordBatch(expectedMatlabTable);
            actual = RecordBatchReadFcn(reader);
            testCase.verifyEqual(actual, expected);

            fcn = @() RecordBatchReadFcn(reader);
            testCase.verifyError(fcn, "arrow:io:ipc:EndOfStream");
        end

        function ReadMultipleBatchStreamFile(testCase, RecordBatchReadFcn)
            % Verify read can successfully read an Arrow IPC Stream file
            % containing mulitple batches.
            reader = arrow.io.ipc.RecordBatchStreamReader(testCase.MultipleBatchStreamFile);

            expectedMatlabTable1 = table(["Row1"; "Row2"], single([1; 2]), VariableNames=["A", "B"]);
            expected1 = arrow.recordBatch(expectedMatlabTable1);
            actual1 = RecordBatchReadFcn(reader);
            testCase.verifyEqual(actual1, expected1);

            expectedMatlabTable2 = table(["Row3"; "Row4"], single([3; 4]), VariableNames=["A", "B"]);
            expected2 = arrow.recordBatch(expectedMatlabTable2);
            actual2 = RecordBatchReadFcn(reader);
            testCase.verifyEqual(actual2, expected2);

            fcn = @() RecordBatchReadFcn(reader);
            testCase.verifyError(fcn, "arrow:io:ipc:EndOfStream");
        end

        function HasNext(testCase, RecordBatchReadFcn)
            % Verify that the hasnext method returns true the correct
            % number of times depending on the number of record
            % batches in an Arrow IPC Stream format.

            reader = arrow.io.ipc.RecordBatchStreamReader(testCase.ZeroBatchStreamFile);
            % hasnext should return true 0 times for a 0 batch file.
            iterations = 0;
            while reader.hasnext()
                RecordBatchReadFcn(reader);
                iterations = iterations + 1;
            end
            testCase.verifyEqual(iterations, 0);

            reader = arrow.io.ipc.RecordBatchStreamReader(testCase.OneBatchStreamFile);
            % hasnext should return true 1 time for a 1 batch file.
            iterations = 0;
            while reader.hasnext()
                RecordBatchReadFcn(reader);
                iterations = iterations + 1;
            end
            testCase.verifyEqual(iterations, 1);

            reader = arrow.io.ipc.RecordBatchStreamReader(testCase.MultipleBatchStreamFile);
            % hasnext should return true 2 times for a 2 batch file.
            iterations = 0;
            while reader.hasnext()
                RecordBatchReadFcn(reader);
                iterations = iterations + 1;
            end
            testCase.verifyEqual(iterations, 2);
        end

        function Done(testCase, RecordBatchReadFcn)
            % Verify that the done method returns false the correct
            % number of times depending on the number of record
            % batches in an Arrow IPC Stream format.

            reader = arrow.io.ipc.RecordBatchStreamReader(testCase.ZeroBatchStreamFile);
            % done should return false 0 times for a 0 batch file.
            iterations = 0;
            while ~reader.done()
                RecordBatchReadFcn(reader);
                iterations = iterations + 1;
            end
            testCase.verifyEqual(iterations, 0);

            reader = arrow.io.ipc.RecordBatchStreamReader(testCase.OneBatchStreamFile);
            % done should return false 1 time for a 1 batch file.
            iterations = 0;
            while ~reader.done()
                RecordBatchReadFcn(reader);
                iterations = iterations + 1;
            end
            testCase.verifyEqual(iterations, 1);

            reader = arrow.io.ipc.RecordBatchStreamReader(testCase.MultipleBatchStreamFile);
            % done should return false 2 times for a 2 batch file.
            iterations = 0;
            while ~reader.done()
                RecordBatchReadFcn(reader);
                iterations = iterations + 1;
            end
            testCase.verifyEqual(iterations, 2);
        end

        function ReadTableZeroBatchStreamFile(testCase)
            % Verify read can successfully read an Arrow IPC Stream file
            % containing zero batches as an arrow.tabular.Table.
            reader = arrow.io.ipc.RecordBatchStreamReader(testCase.ZeroBatchStreamFile);
            matlabTable = table('Size', [0, 2], 'VariableTypes', ["string", "single"], 'VariableNames', ["A", "B"]);
            expected = arrow.table(matlabTable);
            actual = reader.readTable();
            testCase.verifyEqual(actual, expected);
        end

        function ReadTableOneBatchStreamFile(testCase)
            % Verify read can successfully read an Arrow IPC Stream file
            % containing one batch as an arrow.tabular.Table.
            reader = arrow.io.ipc.RecordBatchStreamReader(testCase.OneBatchStreamFile);
            matlabTable = table(["Row1"; "Row2"], single([1; 2]), VariableNames=["A", "B"]);
            expected = arrow.table(matlabTable);
            actual = reader.readTable();
            testCase.verifyEqual(actual, expected);
        end

        function ReadTableMultipleBatchStreamFile(testCase)
            % Verify read can successfully read an Arrow IPC Stream file
            % containing multiple batches as an arrow.tabular.Table.
            reader = arrow.io.ipc.RecordBatchStreamReader(testCase.MultipleBatchStreamFile);
            matlabTable = table(["Row1"; "Row2"; "Row3"; "Row4"], single([1; 2; 3; 4]), VariableNames=["A", "B"]);
            expected = arrow.table(matlabTable);
            actual = reader.readTable();
            testCase.verifyEqual(actual, expected);
        end

        function ReadTableAfterReadRecordBatch(testCase, RecordBatchReadFcn)
            % Verify readTable returns only the remaining record batches
            % in an Arrow IPC Stream file after calling readRecordBatch first.
            reader = arrow.io.ipc.RecordBatchStreamReader(testCase.MultipleBatchStreamFile);

            testCase.verifyTrue(reader.hasnext());
            testCase.verifyFalse(reader.done());

            expectedRecordBatch = arrow.recordBatch(...
                table(["Row1"; "Row2"], single([1; 2]), VariableNames=["A", "B"]) ...
            );
            actualRecordBatch = RecordBatchReadFcn(reader);
            testCase.verifyEqual(actualRecordBatch, expectedRecordBatch);

            expectedTable = arrow.table(...
                table(["Row3"; "Row4"], single([3; 4]), VariableNames=["A", "B"]) ...
            );
            actualTable = reader.readTable();
            testCase.verifyEqual(actualTable, expectedTable);

            testCase.verifyFalse(reader.hasnext());
            testCase.verifyTrue(reader.done());
        end

        function ReadTableMultipleCalls(testCase)
            % Verify readTable returns an empty table if it is called
            % multiple times in a row.
            reader = arrow.io.ipc.RecordBatchStreamReader(testCase.MultipleBatchStreamFile);

            expected = arrow.table(...
                table(["Row1"; "Row2"; "Row3"; "Row4"], single([1; 2; 3; 4]), VariableNames=["A", "B"]) ...
            );
            actual = reader.readTable();
            testCase.verifyEqual(actual, expected);

            testCase.verifyFalse(reader.hasnext());
            testCase.verifyTrue(reader.done());

            expectedEmpty = arrow.table(...
                table('Size', [0, 2], 'VariableTypes', ["string", "single"], 'VariableNames', ["A", "B"]) ...
            );

            actualEmpty = reader.readTable();
            testCase.verifyEqual(actualEmpty, expectedEmpty);

            testCase.verifyFalse(reader.hasnext());
            testCase.verifyTrue(reader.done());

            actualEmpty = reader.readTable();
            testCase.verifyEqual(actualEmpty, expectedEmpty);

            testCase.verifyFalse(reader.hasnext());
            testCase.verifyTrue(reader.done());
        end

        function ErrorIfNotIpcStreamFile(testCase)
            % Verify RecordBatchStreamReader throws an exception with the
            % identifier arrow:io:ipc:FailedToOpenRecordBatchReader if
            % the provided file is not an Arrow IPC Stream file.
            fcn = @() arrow.io.ipc.RecordBatchStreamReader(testCase.RandomAccessFile);
            testCase.verifyError(fcn, "arrow:io:ipc:FailedToOpenRecordBatchReader");
        end

    end

end

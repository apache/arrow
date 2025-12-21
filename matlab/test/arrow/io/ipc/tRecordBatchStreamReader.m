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
        RecordBatchStreamReaderConstructorFcn = {@tRecordBatchStreamReader.FromBytes, @arrow.io.ipc.RecordBatchStreamReader.fromFile}
        RecordBatchReadFcn = {@read, @readRecordBatch}
    end

    methods (Static)

        % Read the given file into memory as an array of bytes (uint8).
        function bytes = readBytes(filename)
            if ismissing(filename)
                % Simulate the behavior of fromFile when a filename
                % that is a missing string value is supplied.
                error(message("MATLAB:validators:mustBeNonzeroLengthText", ""))
            end
            fid = fopen(filename, "r");
            try
                bytes = fread(fid, "uint8=>uint8");
            catch e
                % Simulate the behavior of fromFile when an invalid
                % filename is supplied.
                error(message("MATLAB:validators:mustBeNonzeroLengthText", ""))
            end
            fclose(fid);
        end

        % Read the given file into memory as bytes and then construct a
        % RecordBatchStreamReader from the bytes.
        function reader = FromBytes(filename)
            bytes = tRecordBatchStreamReader.readBytes(filename);
            reader = arrow.io.ipc.RecordBatchStreamReader.fromBytes(bytes);
        end

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

        function ZeroLengthFilenameError(testCase, RecordBatchStreamReaderConstructorFcn)
            % Verify RecordBatchStreamReader throws an exception with the
            % identifier MATLAB:validators:mustBeNonzeroLengthText if the
            % filename input argument given is a zero length string.
            fcn = @() RecordBatchStreamReaderConstructorFcn("");
            testCase.verifyError(fcn, "MATLAB:validators:mustBeNonzeroLengthText");
        end

        function MissingStringFilenameError(testCase, RecordBatchStreamReaderConstructorFcn)
            % Verify RecordBatchStreamReader throws an exception with the
            % identifier MATLAB:validators:mustBeNonzeroLengthText if the
            % filename input argument given is a missing string.
            fcn = @() RecordBatchStreamReaderConstructorFcn(string(missing));
            testCase.verifyError(fcn, "MATLAB:validators:mustBeNonzeroLengthText");
        end

        function FilenameInvalidTypeError(testCase)
            % Verify RecordBatchStreamReader throws an exception with the
            % identifier MATLAB:validators:UnableToConvert if the filename
            % input argument is neither a scalar string nor a char vector.
            fcn = @() arrow.io.ipc.RecordBatchStreamReader(table);
            testCase.verifyError(fcn, "MATLAB:validation:UnableToConvert");
        end

        function Schema(testCase, RecordBatchStreamReaderConstructorFcn)
            % Verify the getter method for Schema returns the
            % expected value.
            fieldA = arrow.field("A", arrow.string());
            fieldB = arrow.field("B", arrow.float32());
            expectedSchema = arrow.schema([fieldA fieldB]);

            reader = RecordBatchStreamReaderConstructorFcn(testCase.ZeroBatchStreamFile);
            testCase.verifyEqual(reader.Schema, expectedSchema);

            reader = RecordBatchStreamReaderConstructorFcn(testCase.OneBatchStreamFile);
            testCase.verifyEqual(reader.Schema, expectedSchema);

            reader = RecordBatchStreamReaderConstructorFcn(testCase.MultipleBatchStreamFile);
            testCase.verifyEqual(reader.Schema, expectedSchema);
        end

        function SchemaNoSetter(testCase, RecordBatchStreamReaderConstructorFcn)
            % Verify the Schema property is not settable.
            fieldC = arrow.field("C", arrow.date32());
            schema = arrow.schema(fieldC);
            reader = RecordBatchStreamReaderConstructorFcn(testCase.ZeroBatchStreamFile);
            testCase.verifyError(@() setfield(reader, "Schema", schema), "MATLAB:class:SetProhibited");
        end

        function ReadErrorIfEndOfStream(testCase, RecordBatchStreamReaderConstructorFcn, RecordBatchReadFcn)
            % Verify read throws an execption with the identifier arrow:io:ipc:EndOfStream
            % on an Arrow IPC Stream file containing zero batches.
            reader = RecordBatchStreamReaderConstructorFcn(testCase.ZeroBatchStreamFile);
            fcn = @() RecordBatchReadFcn(reader);
            testCase.verifyError(fcn, "arrow:io:ipc:EndOfStream");
        end

        function ReadOneBatchStreamFile(testCase, RecordBatchStreamReaderConstructorFcn, RecordBatchReadFcn)
            % Verify read can successfully read an Arrow IPC Stream file
            % containing one batch.
            reader = RecordBatchStreamReaderConstructorFcn(testCase.OneBatchStreamFile);

            expectedMatlabTable = table(["Row1"; "Row2"], single([1; 2]), VariableNames=["A", "B"]);
            expected = arrow.recordBatch(expectedMatlabTable);
            actual = RecordBatchReadFcn(reader);
            testCase.verifyEqual(actual, expected);

            fcn = @() RecordBatchReadFcn(reader);
            testCase.verifyError(fcn, "arrow:io:ipc:EndOfStream");
        end

        function ReadMultipleBatchStreamFile(testCase, RecordBatchStreamReaderConstructorFcn, RecordBatchReadFcn)
            % Verify read can successfully read an Arrow IPC Stream file
            % containing mulitple batches.
            reader = RecordBatchStreamReaderConstructorFcn(testCase.MultipleBatchStreamFile);

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

        function HasNext(testCase, RecordBatchStreamReaderConstructorFcn, RecordBatchReadFcn)
            % Verify that the hasnext method returns true the correct
            % number of times depending on the number of record
            % batches in an Arrow IPC Stream format.

            reader = RecordBatchStreamReaderConstructorFcn(testCase.ZeroBatchStreamFile);
            % hasnext should return true 0 times for a 0 batch file.
            iterations = 0;
            while reader.hasnext()
                RecordBatchReadFcn(reader);
                iterations = iterations + 1;
            end
            testCase.verifyEqual(iterations, 0);

            reader = RecordBatchStreamReaderConstructorFcn(testCase.OneBatchStreamFile);
            % hasnext should return true 1 time for a 1 batch file.
            iterations = 0;
            while reader.hasnext()
                RecordBatchReadFcn(reader);
                iterations = iterations + 1;
            end
            testCase.verifyEqual(iterations, 1);

            reader = RecordBatchStreamReaderConstructorFcn(testCase.MultipleBatchStreamFile);
            % hasnext should return true 2 times for a 2 batch file.
            iterations = 0;
            while reader.hasnext()
                RecordBatchReadFcn(reader);
                iterations = iterations + 1;
            end
            testCase.verifyEqual(iterations, 2);
        end

        function Done(testCase, RecordBatchStreamReaderConstructorFcn, RecordBatchReadFcn)
            % Verify that the done method returns false the correct
            % number of times depending on the number of record
            % batches in an Arrow IPC Stream format.

            reader = RecordBatchStreamReaderConstructorFcn(testCase.ZeroBatchStreamFile);
            % done should return false 0 times for a 0 batch file.
            iterations = 0;
            while ~reader.done()
                RecordBatchReadFcn(reader);
                iterations = iterations + 1;
            end
            testCase.verifyEqual(iterations, 0);

            reader = RecordBatchStreamReaderConstructorFcn(testCase.OneBatchStreamFile);
            % done should return false 1 time for a 1 batch file.
            iterations = 0;
            while ~reader.done()
                RecordBatchReadFcn(reader);
                iterations = iterations + 1;
            end
            testCase.verifyEqual(iterations, 1);

            reader = RecordBatchStreamReaderConstructorFcn(testCase.MultipleBatchStreamFile);
            % done should return false 2 times for a 2 batch file.
            iterations = 0;
            while ~reader.done()
                RecordBatchReadFcn(reader);
                iterations = iterations + 1;
            end
            testCase.verifyEqual(iterations, 2);
        end

        function ReadTableZeroBatchStreamFile(testCase, RecordBatchStreamReaderConstructorFcn)
            % Verify read can successfully read an Arrow IPC Stream file
            % containing zero batches as an arrow.tabular.Table.
            reader = RecordBatchStreamReaderConstructorFcn(testCase.ZeroBatchStreamFile);
            matlabTable = table('Size', [0, 2], 'VariableTypes', ["string", "single"], 'VariableNames', ["A", "B"]);
            expected = arrow.table(matlabTable);
            actual = reader.readTable();
            testCase.verifyEqual(actual, expected);
        end

        function ReadTableOneBatchStreamFile(testCase, RecordBatchStreamReaderConstructorFcn)
            % Verify read can successfully read an Arrow IPC Stream file
            % containing one batch as an arrow.tabular.Table.
            reader = RecordBatchStreamReaderConstructorFcn(testCase.OneBatchStreamFile);
            matlabTable = table(["Row1"; "Row2"], single([1; 2]), VariableNames=["A", "B"]);
            expected = arrow.table(matlabTable);
            actual = reader.readTable();
            testCase.verifyEqual(actual, expected);
        end

        function ReadTableMultipleBatchStreamFile(testCase, RecordBatchStreamReaderConstructorFcn)
            % Verify read can successfully read an Arrow IPC Stream file
            % containing multiple batches as an arrow.tabular.Table.
            reader = RecordBatchStreamReaderConstructorFcn(testCase.MultipleBatchStreamFile);
            matlabTable = table(["Row1"; "Row2"; "Row3"; "Row4"], single([1; 2; 3; 4]), VariableNames=["A", "B"]);
            expected = arrow.table(matlabTable);
            actual = reader.readTable();
            testCase.verifyEqual(actual, expected);
        end

        function ReadTableAfterReadRecordBatch(testCase, RecordBatchStreamReaderConstructorFcn, RecordBatchReadFcn)
            % Verify readTable returns only the remaining record batches
            % in an Arrow IPC Stream file after calling readRecordBatch first.
            reader = RecordBatchStreamReaderConstructorFcn(testCase.MultipleBatchStreamFile);

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

        function ReadTableMultipleCalls(testCase, RecordBatchStreamReaderConstructorFcn)
            % Verify readTable returns an empty table if it is called
            % multiple times in a row.
            reader = RecordBatchStreamReaderConstructorFcn(testCase.MultipleBatchStreamFile);

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

        function ErrorIfNotIpcStreamFile(testCase, RecordBatchStreamReaderConstructorFcn)
            % Verify RecordBatchStreamReader throws an exception with the
            % identifier arrow:io:ipc:FailedToOpenRecordBatchReader if
            % the provided file is not an Arrow IPC Stream file.
            fcn = @() RecordBatchStreamReaderConstructorFcn(testCase.RandomAccessFile);
            testCase.verifyError(fcn, "arrow:io:ipc:FailedToOpenRecordBatchReader");
        end

        function ErrorIfNotProxy(testCase)
            % Verify the RecordBatchStreamReader constructor throws an exception
            % with the identifier MATLAB:validation:UnableToConvert if the input
            % is not a Proxy object.
            fcn = @() arrow.io.ipc.RecordBatchStreamReader(testCase.RandomAccessFile);
            testCase.verifyError(fcn, "MATLAB:validation:UnableToConvert");
        end

    end

end

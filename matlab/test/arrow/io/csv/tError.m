%TERROR Error tests for CSV.

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
classdef tError < CSVTest

    methods(Test)

        function EmptyFile(testCase)
            import arrow.io.csv.*

            arrowTableWrite = arrow.table();

            writer = TableWriter(testCase.Filename);
            reader = TableReader(testCase.Filename);

            writer.write(arrowTableWrite);
            fcn = @() reader.read();
            testCase.verifyError(fcn, "arrow:io:csv:FailedToReadTable");
        end

        function InvalidWriterFilenameType(testCase)
            import arrow.io.csv.*
            fcn = @() TableWriter(table);
            testCase.verifyError(fcn, "MATLAB:validation:UnableToConvert");
            fcn = @() TableWriter(["a", "b"]);
            testCase.verifyError(fcn, "MATLAB:validation:IncompatibleSize");
        end

        function InvalidReaderFilenameType(testCase)
            import arrow.io.csv.*
            fcn = @() TableReader(table);
            testCase.verifyError(fcn, "MATLAB:validation:UnableToConvert");
            fcn = @() TableReader(["a", "b"]);
            testCase.verifyError(fcn, "MATLAB:validation:IncompatibleSize");
        end

        function InvalidWriterWriteType(testCase)
            import arrow.io.csv.*
            writer = TableWriter(testCase.Filename);
            fcn = @() writer.write("text");
            testCase.verifyError(fcn, "MATLAB:validation:UnableToConvert");
        end

        function WriterFilenameNoSetter(testCase)
            import arrow.io.csv.*
            writer = TableWriter(testCase.Filename);
            fcn = @() setfield(writer, "Filename", "filename.csv");
            testCase.verifyError(fcn, "MATLAB:class:SetProhibited");
        end

        function ReaderFilenameNoSetter(testCase)
            import arrow.io.csv.*
            reader = TableReader(testCase.Filename);
            fcn = @() setfield(reader, "Filename", "filename.csv");
            testCase.verifyError(fcn, "MATLAB:class:SetProhibited");
        end

    end

end
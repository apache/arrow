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
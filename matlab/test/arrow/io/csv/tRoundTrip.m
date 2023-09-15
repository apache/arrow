%TROUNDTRIP Round trip tests for CSV.

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

    properties
        MatlabTableNumeric
        MatlabTableString
        ArrowTableNumeric
        ArrowTableString
        Filename
    end

    methods (TestClassSetup)

        function initializeProperties(testCase)
            % Seed the random number generator.
            rng(1);

            testCase.MatlabTableNumeric = array2table(rand(10000, 5), ...
                VariableNames=["😀", "🌲", "🥭", " ", "ABC"]);
            testCase.ArrowTableNumeric = arrow.table(testCase.MatlabTableNumeric);
            testCase.MatlabTableString = table(["A"; "B"; "C"], ...
                                               [""; " "; "   "], ...
                                               ["😀"; "🌲"; "🥭"]);
            testCase.ArrowTableString = arrow.table(testCase.MatlabTableString);
        end

    end

    methods (TestMethodSetup)

        function setupTempFile(testCase)
            import matlab.unittest.fixtures.TemporaryFolderFixture
            fixture = testCase.applyFixture(TemporaryFolderFixture);
            testCase.Filename = fullfile(fixture.Folder, "temp.csv");
        end

    end

    methods(Test)

        function Numeric(testCase)
            import arrow.io.csv.*

            arrowTableWrite = testCase.ArrowTableNumeric;

            writer = TableWriter(testCase.Filename);
            reader = TableReader(testCase.Filename);

            writer.write(arrowTableWrite);
            arrowTableRead = reader.read();

            testCase.verifyEqual(arrowTableRead, arrowTableWrite);
        end

        function String(testCase)
            import arrow.io.csv.*

            arrowTableWrite = testCase.ArrowTableString;

            writer = TableWriter(testCase.Filename);
            reader = TableReader(testCase.Filename);

            writer.write(arrowTableWrite);
            arrowTableRead = reader.read();

            testCase.verifyEqual(arrowTableRead, arrowTableWrite);
        end

    end

end
%CSVTEST Super class for CSV related tests.

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
classdef CSVTest < matlab.unittest.TestCase

    properties
        Filename
    end

    methods (TestClassSetup)

        function initializeProperties(~)
            % Seed the random number generator.
            rng(1);
        end

    end

    methods (TestMethodSetup)

        function setupTestFilename(testCase)
            import matlab.unittest.fixtures.TemporaryFolderFixture
            fixture = testCase.applyFixture(TemporaryFolderFixture);
            testCase.Filename = fullfile(fixture.Folder, "filename.csv");
        end

    end

    methods

        function verifyRoundTrip(testCase, arrowTable)
            import arrow.io.csv.*

            writer = TableWriter(testCase.Filename);
            reader = TableReader(testCase.Filename);

            writer.write(arrowTable);
            arrowTableRead = reader.read();

            testCase.verifyEqual(arrowTableRead, arrowTable);
        end

        function arrowTable = makeArrowTable(testCase, opts)
            arguments
                testCase
                opts.Type
                opts.ColumnNames
                opts.NumRows
                opts.WithNulls (1, 1) logical = false
            end

            if opts.Type == "numeric"
                matlabTable = array2table(rand(opts.NumRows, numel(opts.ColumnNames)));
            elseif opts.Type == "string"
                matlabTable = array2table("A" + rand(opts.NumRows, numel(opts.ColumnNames)) + "B");
            end

            if opts.WithNulls
                matlabTable = testCase.setNullValues(matlabTable, NullPercentage=0.2);
            end

            arrays = cell(1, width(matlabTable));
            for ii = 1:width(matlabTable)
                arrays{ii} = arrow.array(matlabTable.(ii));
            end
            arrowTable = arrow.tabular.Table.fromArrays(arrays{:}, ColumnNames=opts.ColumnNames);
        end

        function tWithNulls = setNullValues(testCase, t, opts)
            arguments
                testCase %#ok<INUSA>
                t table
                opts.NullPercentage (1, 1) double {mustBeGreaterThanOrEqual(opts.NullPercentage, 0)} = 0.5
            end

            tWithNulls = t;
            for ii = 1:width(t)
                temp = tWithNulls.(ii);
                numValues = numel(temp);
                numNulls = uint64(opts.NullPercentage * numValues);
                nullIndices = randperm(numValues, numNulls);
                temp(nullIndices) = missing;
                tWithNulls.(ii) = temp;
            end
        end

    end

end

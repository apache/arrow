%TROUNDTRIPRECORDBATCH Tests for roundtripping RecordBatches using 
% the C Data Interface format.

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
classdef tRoundTripRecordBatch < matlab.unittest.TestCase

    methods (Test)
        function ZeroColumnRecordBatch(testCase)
            expected = arrow.recordBatch(table());
           
            cArray = arrow.c.Array();
            cSchema = arrow.c.Schema();
            expected.export(cArray.Address, cSchema.Address);
            actual = arrow.tabular.RecordBatch.import(cArray, cSchema);

            testCase.verifyEqual(actual, expected);
        end

        function ZeroRowRecordBatch(testCase)
            doubleArray = arrow.array([]);
            stringArray = arrow.array(string.empty(0, 0));
            expected = arrow.tabular.RecordBatch.fromArrays(doubleArray, stringArray);
            
            cArray = arrow.c.Array();
            cSchema = arrow.c.Schema();
            expected.export(cArray.Address, cSchema.Address);
            actual = arrow.tabular.RecordBatch.import(cArray, cSchema);

            testCase.verifyEqual(actual, expected);
        end

        function OneRowRecordBatch(testCase)
            varNames = ["Col1" "Col2" "Col3"];
            t = table(1, "A", false, VariableNames=varNames);
            expected = arrow.recordBatch(t);

            cArray = arrow.c.Array();
            cSchema = arrow.c.Schema();
            expected.export(cArray.Address, cSchema.Address);
            actual = arrow.tabular.RecordBatch.import(cArray, cSchema);

            testCase.verifyEqual(actual, expected);
        end

        function MultiRowRecordBatch(testCase)
            varNames = ["Col1" "Col2" "Col3"];
            t = table((1:3)', ["A"; "B"; "C"], [false; true; false],...
                VariableNames=varNames);
            expected = arrow.recordBatch(t);

            cArray = arrow.c.Array();
            cSchema = arrow.c.Schema();
            expected.export(cArray.Address, cSchema.Address);
            actual = arrow.tabular.RecordBatch.import(cArray, cSchema);

            testCase.verifyEqual(actual, expected);
        end

    end

end
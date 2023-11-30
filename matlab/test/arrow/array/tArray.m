%TARRAY Unit tests for arrow.array function. 

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

classdef tArray < matlab.unittest.TestCase

    properties(TestParameter)
        MATLABDataArrayTypePair = { ...
            {[true false],         "arrow.array.BooleanArray"}, ...
            {int8([1 2]),          "arrow.array.Int8Array"}, ...
            {uint8([1 2]),         "arrow.array.UInt8Array"}, ...
            {int16([1 2]),         "arrow.array.Int16Array"}, ...
            {uint16([1 2]),        "arrow.array.UInt16Array"}, ...
            {int32([1 2]),         "arrow.array.Int32Array"}, ...
            {uint32([1 2]),        "arrow.array.UInt32Array"}, ...
            {int64([1 2]),         "arrow.array.Int64Array"}, ...
            {uint64([1 2]),        "arrow.array.UInt64Array"}, ...
            {single([1 2]),        "arrow.array.Float32Array"}, ...
            {[1 2],                "arrow.array.Float64Array"}, ...
            {datetime(2022, 1, 1), "arrow.array.TimestampArray"}, ...
            {seconds([1 2]),       "arrow.array.Time64Array"}, ...
            {["A" "B"],            "arrow.array.StringArray"}, ...
            {table(["A" "B"]'),    "arrow.array.StructArray"}, ...
            {{[1, 2, 3], [4, 5]},  "arrow.array.ListArray"}};
    end

    methods(Test)        
        function ArrowArrayOutputType(testCase, MATLABDataArrayTypePair)
        % Verify arrow.array returns the expected arrow.array.<Type>Array
        % with respect to the input data array's MATLAB class type.
            matlabArray = MATLABDataArrayTypePair{1};
            expectedClassName = MATLABDataArrayTypePair{2};
            arrowArray = arrow.array(matlabArray);
            actualClassName = string(class(arrowArray));
            testCase.verifyEqual(actualClassName, expectedClassName);
        end

        function UnsupportedMATLABTypeError(testCase)
        % Verify arrow.array throws an error with the identifier
        % "arrow:array:UnsupportedMATLABType" if the input array is not one
        % we support converting into an Arrow array.
            matlabArray = calmonths(12);
            fcn = @() arrow.array(matlabArray);
            errID = "arrow:array:UnsupportedMATLABType";
            testCase.verifyError(fcn, errID);
        end

        function InferNullsDefault(testCase)
        % Verify InferNulls is true by default.
            matlabArray = [1 2 NaN 3];
            arrowArray = arrow.array(matlabArray);
            testCase.verifyEqual(arrowArray.Valid, [true; true; false; true]);
        end

        function InferNullsTrue(testCase)
        % Verify InferNulls is true by default.
            matlabArray = [1 2 NaN 3];
            arrowArray = arrow.array(matlabArray, InferNulls=true);
            testCase.verifyEqual(arrowArray.Valid, [true; true; false; true]);
        end

        function InferNullsFalse(testCase)
        % Verify Valid is the expected logical vector when
        % InferNulls=false.
            matlabArray = [1 2 NaN 3];
            arrowArray = arrow.array(matlabArray, InferNulls=false);
            testCase.verifyEqual(arrowArray.Valid, [true; true; true; true]);
        end

        function ValidNameValuePair(testCase)
        % Verify Valid is the expected vector when the Valid
        % name-value pair is supplied.
            matlabArray = [1 NaN NaN 3];
            arrowArray = arrow.array(matlabArray, Valid=[1 2]);
            testCase.verifyEqual(arrowArray.Valid, [true; true; false; false]);
        end
    end
end
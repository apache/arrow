%TDATE32ARRAY Unit tests for arrow.array.Date32Array

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

classdef tDate32Array < matlab.unittest.TestCase

    properties
        ArrowArrayConstructorFcn = @arrow.array.Date32Array.fromMATLAB
    end

    properties (Constant)
        unixEpoch = datetime(0, ConvertFrom="posixtime");
        missingDates = [datetime(2023, 1, 1), NaT, NaT, datetime(2022, 1, 1), NaT];
    end

    methods (Test)

        function TestBasic(testCase)
            dates = testCase.unixEpoch + days(1:10);
            array = testCase.ArrowArrayConstructorFcn(dates);
            testCase.verifyInstanceOf(array, "arrow.array.Date32Array");
        end

        function TestTypeIsDate32(testCase)
            dates = testCase.unixEpoch + days(1:10);
            array = testCase.ArrowArrayConstructorFcn(dates);
            testCase.verifyDate32Type(array.Type);
        end

        function TestLength(testCase)
            dates = datetime.empty(0, 1);
            array = testCase.ArrowArrayConstructorFcn(dates);
            testCase.verifyEqual(array.Length, int64(0));

            dates = datetime(2023, 1, 1);
            array = testCase.ArrowArrayConstructorFcn(dates);
            testCase.verifyEqual(array.Length, int64(1));

            dates = testCase.unixEpoch + days(1:10);
            array = testCase.ArrowArrayConstructorFcn(dates);
            testCase.verifyEqual(array.Length, int64(10));
        end

        function TestToMATLAB(testCase)
            % Verify toMATLAB() round-trips the original datetime array.
            dates = testCase.unixEpoch + days(1:10);
            array = testCase.ArrowArrayConstructorFcn(dates);
            values = toMATLAB(array);
            testCase.verifyEqual(values, dates');
        end

        function TestDatetime(testCase)
            % Verify datetime() round-trips the original datetime array.
            dates = testCase.unixEpoch + days(1:10);
            array = testCase.ArrowArrayConstructorFcn(dates);
            values = datetime(array);
            testCase.verifyEqual(values, dates');
        end

        function TestValid(testCase)
            % Verify the Valid property returns the expected logical vector.
            dates = testCase.missingDates;
            array = testCase.ArrowArrayConstructorFcn(dates);
            testCase.verifyEqual(array.Valid, [true; false; false; true; false]);
            testCase.verifyEqual(toMATLAB(array), dates');
            testCase.verifyEqual(datetime(array), dates');
        end

        function InferNullsTrueNVPair(testCase)
            % Verify arrow.array.Date32Array.fromMATLAB() behaves as
            % expected when InferNulls=true is provided.
            dates = testCase.missingDates;
            array = testCase.ArrowArrayConstructorFcn(dates, InferNulls=true);
            expectedValid = [true; false; false; true; false];
            testCase.verifyEqual(array.Valid, expectedValid);
            testCase.verifyEqual(toMATLAB(array), dates');
            testCase.verifyEqual(datetime(array), dates');
        end

        function InferNullsFalseNVPair(testCase)
            % Verify arrow.array.Date32Array.fromMATLAB() behaves as
            % expected when InferNulls=false is provided.
            dates = testCase.missingDates;
            array = testCase.ArrowArrayConstructorFcn(dates, InferNulls=false);
            expectedValid = [true; true; true; true; true];
            testCase.verifyEqual(array.Valid, expectedValid);

            % If NaT datetimes were not considered null values, then they
            % are treated like int32(0) - i.e. the Unix epoch.
            expectedDates = dates';
            expectedDates([2, 3, 5]) = testCase.unixEpoch;
            testCase.verifyEqual(toMATLAB(array), expectedDates);
            testCase.verifyEqual(datetime(array), expectedDates);
        end

        function TestValidNVPair(testCase)
            % Verify arrow.array.Date32Array.fromMATLAB() accepts the Valid
            % nv-pair, and it behaves as expected.
            dates = testCase.missingDates;

            % Supply the Valid name-value pair as vector of indices.
            array = testCase.ArrowArrayConstructorFcn(dates, Valid=[1, 2, 3]);
            testCase.verifyEqual(array.Valid, [true; true; true; false; false]);
            expectedDates = dates';
            expectedDates([2, 3]) = testCase.unixEpoch;
            expectedDates([4, 5]) = NaT;
            testCase.verifyEqual(toMATLAB(array), expectedDates);

            % Supply the Valid name-value pair as a logical scalar.
            array = testCase.ArrowArrayConstructorFcn(dates, Valid=false);
            testCase.verifyEqual(array.Valid, [false; false; false; false; false]);
            expectedDates(:) = NaT;
            testCase.verifyEqual(toMATLAB(array), expectedDates);
        end

        function EmptyDatetimeVector(testCase)
            % Verify arrow.array.Date32Array.fromMATLAB() accepts any
            % empty-shaped datetime as input.

            dates = datetime.empty(0, 0);
            array = testCase.ArrowArrayConstructorFcn(dates);
            testCase.verifyEqual(array.Length, int64(0));
            testCase.verifyEqual(array.Valid, logical.empty(0, 1));
            testCase.verifyEqual(toMATLAB(array), datetime.empty(0, 1));

            % Test with an N-Dimensional empty array
            dates = datetime.empty(0, 1, 0);
            array = testCase.ArrowArrayConstructorFcn(dates);
            testCase.verifyEqual(array.Length, int64(0));
            testCase.verifyEqual(array.Valid, logical.empty(0, 1));
            testCase.verifyEqual(toMATLAB(array), datetime.empty(0, 1));
        end

        function ErrorIfNonVector(testCase)
            % Verify arrow.array.Date32Array.fromMATLAB() throws an error
            % if the input provided is not a vector.

            dates = datetime(2023, 1, 1) + days(1:12);
            dates = reshape(dates, 2, 6);
            fcn = @() testCase.ArrowArrayConstructorFcn(dates);
            testCase.verifyError(fcn, "arrow:array:InvalidShape");

            dates = reshape(dates, 3, 2, 2);
            fcn = @() testCase.ArrowArrayConstructorFcn(dates);
            testCase.verifyError(fcn, "arrow:array:InvalidShape");
        end

        function ErrorIfNonDatetime(testCase)
            % Verify arrow.array.Date32Array.fromMATLAB() throws an error
            % if not given a datetime as input.

            dates = duration(1, 2, 3);
            fcn = @() testCase.ArrowArrayConstructorFcn(dates);
            testCase.verifyError(fcn, "arrow:array:InvalidType");

            numbers = [1; 2; 3; 4];
            fcn = @() testCase.ArrowArrayConstructorFcn(numbers);
            testCase.verifyError(fcn, "arrow:array:InvalidType");
        end

    end

    methods

        function verifyDate32Type(testCase, actual)
            testCase.verifyInstanceOf(actual, "arrow.type.Date32Type");
            testCase.verifyEqual(actual.ID, arrow.type.ID.Date32);
            testCase.verifyEqual(actual.DateUnit, arrow.type.DateUnit.Day);
        end

    end

end
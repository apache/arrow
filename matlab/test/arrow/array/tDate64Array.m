%TDATE64ARRAY Unit tests for arrow.array.Date64Array

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

classdef tDate64Array < matlab.unittest.TestCase

    properties
        ArrowArrayConstructorFcn = @arrow.array.Date64Array.fromMATLAB
    end

    properties (Constant)
        UnixEpoch = datetime(0, ConvertFrom="posixtime");
        MissingDates = [datetime(2023, 1, 1), NaT, NaT, datetime(2022, 1, 1), NaT];
    end

    methods (Test)

        function TestBasic(testCase)
            dates = testCase.UnixEpoch + days(1:10);
            array = testCase.ArrowArrayConstructorFcn(dates);
            testCase.verifyInstanceOf(array, "arrow.array.Date64Array");
        end

        function TestTypeIsDate64(testCase)
            dates = testCase.UnixEpoch + days(1:10);
            array = testCase.ArrowArrayConstructorFcn(dates);
            testCase.verifyDate64Type(array.Type);
        end

        function TestNumElements(testCase)
            dates = datetime.empty(0, 1);
            array = testCase.ArrowArrayConstructorFcn(dates);
            testCase.verifyEqual(array.NumElements, int64(0));

            dates = datetime(2023, 1, 1);
            array = testCase.ArrowArrayConstructorFcn(dates);
            testCase.verifyEqual(array.NumElements, int64(1));

            dates = testCase.UnixEpoch + days(1:10);
            array = testCase.ArrowArrayConstructorFcn(dates);
            testCase.verifyEqual(array.NumElements, int64(10));
        end

        function TestToMATLAB(testCase)
            % Verify toMATLAB() round-trips the original datetime array.
            dates = testCase.UnixEpoch + days(1:10);
            array = testCase.ArrowArrayConstructorFcn(dates);
            values = toMATLAB(array);
            testCase.verifyEqual(values, dates');
        end

        function TestDatetime(testCase)
            % Verify datetime() round-trips the original datetime array.
            dates = testCase.UnixEpoch + days(1:10);
            array = testCase.ArrowArrayConstructorFcn(dates);
            values = datetime(array);
            testCase.verifyEqual(values, dates');
        end

        function TestValid(testCase)
            % Verify the Valid property returns the expected logical vector.
            dates = testCase.MissingDates;
            array = testCase.ArrowArrayConstructorFcn(dates);
            testCase.verifyEqual(array.Valid, [true; false; false; true; false]);
            testCase.verifyEqual(toMATLAB(array), dates');
            testCase.verifyEqual(datetime(array), dates');
        end

        function TestInferNullsTrueNVPair(testCase)
            % Verify arrow.array.Date64Array.fromMATLAB() behaves as
            % expected when InferNulls=true is provided.
            dates = testCase.MissingDates;
            array = testCase.ArrowArrayConstructorFcn(dates, InferNulls=true);
            expectedValid = [true; false; false; true; false];
            testCase.verifyEqual(array.Valid, expectedValid);
            testCase.verifyEqual(toMATLAB(array), dates');
            testCase.verifyEqual(datetime(array), dates');
        end

        function TestInferNullsFalseNVPair(testCase)
            % Verify arrow.array.Date64Array.fromMATLAB() behaves as
            % expected when InferNulls=false is provided.
            dates = testCase.MissingDates;
            array = testCase.ArrowArrayConstructorFcn(dates, InferNulls=false);
            expectedValid = [true; true; true; true; true];
            testCase.verifyEqual(array.Valid, expectedValid);

            % If NaT datetimes were not considered null values, then they
            % are treated like int64(0) - i.e. the Unix epoch.
            expectedDates = dates';
            expectedDates([2, 3, 5]) = testCase.UnixEpoch;
            testCase.verifyEqual(toMATLAB(array), expectedDates);
            testCase.verifyEqual(datetime(array), expectedDates);
        end

        function TestValidNVPair(testCase)
            % Verify arrow.array.Date64Array.fromMATLAB() accepts the Valid
            % nv-pair, and it behaves as expected.
            dates = testCase.MissingDates;

            % Supply the Valid name-value pair as vector of indices.
            array = testCase.ArrowArrayConstructorFcn(dates, Valid=[1, 2, 3]);
            testCase.verifyEqual(array.Valid, [true; true; true; false; false]);
            expectedDates = dates';
            expectedDates([2, 3]) = testCase.UnixEpoch;
            expectedDates([4, 5]) = NaT;
            testCase.verifyEqual(toMATLAB(array), expectedDates);

            % Supply the Valid name-value pair as a logical scalar.
            array = testCase.ArrowArrayConstructorFcn(dates, Valid=false);
            testCase.verifyEqual(array.Valid, [false; false; false; false; false]);
            expectedDates(:) = NaT;
            testCase.verifyEqual(toMATLAB(array), expectedDates);
        end

        function TestEmptyDatetimeVector(testCase)
            % Verify arrow.array.Date64Array.fromMATLAB() accepts any
            % empty-shaped datetime as input.

            dates = datetime.empty(0, 0);
            array = testCase.ArrowArrayConstructorFcn(dates);
            testCase.verifyEqual(array.NumElements, int64(0));
            testCase.verifyEqual(array.Valid, logical.empty(0, 1));
            testCase.verifyEqual(toMATLAB(array), datetime.empty(0, 1));

            % Test with an N-Dimensional empty array
            dates = datetime.empty(0, 1, 0);
            array = testCase.ArrowArrayConstructorFcn(dates);
            testCase.verifyEqual(array.NumElements, int64(0));
            testCase.verifyEqual(array.Valid, logical.empty(0, 1));
            testCase.verifyEqual(toMATLAB(array), datetime.empty(0, 1));
        end

        function TestErrorIfNonVector(testCase)
            % Verify arrow.array.Date64Array.fromMATLAB() throws an error
            % if the input provided is not a vector.

            dates = datetime(2023, 1, 1) + days(1:12);
            dates = reshape(dates, 2, 6);
            fcn = @() testCase.ArrowArrayConstructorFcn(dates);
            testCase.verifyError(fcn, "arrow:array:InvalidShape");

            dates = reshape(dates, 3, 2, 2);
            fcn = @() testCase.ArrowArrayConstructorFcn(dates);
            testCase.verifyError(fcn, "arrow:array:InvalidShape");
        end

        function TestErrorIfNonDatetime(testCase)
            % Verify arrow.array.Date64Array.fromMATLAB() throws an error
            % if not given a datetime as input.

            dates = duration(1, 2, 3);
            fcn = @() testCase.ArrowArrayConstructorFcn(dates);
            testCase.verifyError(fcn, "arrow:array:InvalidType");

            numbers = [1; 2; 3; 4];
            fcn = @() testCase.ArrowArrayConstructorFcn(numbers);
            testCase.verifyError(fcn, "arrow:array:InvalidType");
        end

        function TestInt64MaxMilliseconds(testCase)
            % Verify that no precision is lost when trying to round-trip a
            % datetime value that is within abs(intmin("int64")) milliseconds before
            % and intmax("int64") days after the UNIX epoch.

            ticksPerSecond = 1e3;

            numMillisecondsBefore = intmin("int64");
            numMillisecondsAfter = intmax("int64");

            expectedBefore = datetime(numMillisecondsBefore, ConvertFrom="epochtime", ...
                Epoch=testCase.UnixEpoch, TicksPerSecond=ticksPerSecond);
            expectedAfter = datetime(numMillisecondsAfter, ConvertFrom="epochtime", ...
                Epoch=testCase.UnixEpoch, TicksPerSecond=ticksPerSecond);

            array = testCase.ArrowArrayConstructorFcn(expectedBefore);
            actualBefore = array.toMATLAB();
            testCase.verifyEqual(actualBefore, expectedBefore);

            array = testCase.ArrowArrayConstructorFcn(expectedAfter);
            actualAfter = array.toMATLAB();
            testCase.verifyEqual(actualAfter, expectedAfter);
        end

        function TestZonedDatetime(testCase)
            % Verify that zoned datetimes are supported as inputs to the
            % fromMATLAB method and that the output datetime returned by
            % the toMATLAB method is unzoned.
            zoned = testCase.UnixEpoch + days(10);
            zoned.TimeZone = "America/New_York";

            expected = zoned;
            % Set the TimeZone of the expected datetime value to "UTC"
            % to trigger a time adjustment to UTC-relative time.
            % Date64Array values are implicitly relative to UTC time, so
            % adjusting the expected datetime value to UTC will ensure that
            % the actual and expected datetimes represent the same "instant-in-time".
            expected.TimeZone = "UTC";
            % datetimes returned by the toMATLAB method of Date64Array are unzoned.
            % Set the TimeZone of the expected datetime to a 0x0 empty char array
            % in order to make the datetime unzoned.
            expected.TimeZone = char.empty(0, 0);

            array = testCase.ArrowArrayConstructorFcn(zoned);
            actual = array.toMATLAB();
            testCase.verifyEqual(actual, expected);
        end

        function TestInt64MaxMillisecondsZoned(testCase)
            % Verify that zoned datetimes which are within abs(intmin("int64")) days
            % before and intmax("int64") days after the UNIX epoch are round-tripped
            % (not including the TimeZone).

            ticksPerSecond = 1e3;

            numMillisecondsBefore = intmin("int64");
            numMillisecondsAfter = intmax("int64");

            expectedBefore = datetime(numMillisecondsBefore, ConvertFrom="epochtime", ...
                Epoch=testCase.UnixEpoch, TicksPerSecond=ticksPerSecond);
            expectedAfter = datetime(numMillisecondsAfter, ConvertFrom="epochtime", ...
                Epoch=testCase.UnixEpoch, TicksPerSecond=ticksPerSecond);

            expectedZonedBefore = expectedBefore;
            expectedZonedBefore.TimeZone = "UTC";

            expectedZonedAfter = expectedAfter;
            expectedZonedAfter.TimeZone = "UTC";

            array = testCase.ArrowArrayConstructorFcn(expectedZonedBefore);
            actualBefore = array.toMATLAB();
            testCase.verifyEqual(actualBefore, expectedBefore);

            array = testCase.ArrowArrayConstructorFcn(expectedZonedAfter);
            actualAfter = array.toMATLAB();
            testCase.verifyEqual(actualAfter, expectedAfter);
        end

        function TestIsEqualTrue(tc)
            % Verifies arrays are considered equal if:
            %
            %  1. Their Type properties are equal
            %  2. They have the same number of elements (i.e. their NumElements properties are equal)
            %  3. They have the same validity bitmap (i.e. their Valid properties are equal)
            %  4. All corresponding valid elements have the same values

            dates1 = datetime(2023, 6, 22)  + days(0:4);
            dates2 = datetime(2023, 6, 22) + days([0 1 5 3 4]);

            array1 = tc.ArrowArrayConstructorFcn(dates1, Valid=[1 2 4]);
            array2 = tc.ArrowArrayConstructorFcn(dates1, Valid=[1 2 4]);
            array3 = tc.ArrowArrayConstructorFcn(dates2, Valid=[1 2 4]);

            tc.verifyTrue(isequal(array1, array2));
            tc.verifyTrue(isequal(array1, array3));

            % Test supplying more than two arrays to isequal
            tc.verifyTrue(isequal(array1, array2, array3));
        end

        function TestIsEqualFalse(tc)
            % Verify isequal returns false when expected.
            dates1 = datetime(2023, 6, 22) + days(0:4);
            dates2 = datetime(2023, 6, 22) + days([1 1 2 3 4]);
            dates3 = datetime(2023, 6, 22) + days(0:5);
            array1 = tc.ArrowArrayConstructorFcn(dates1, Valid=[1 2 4]);
            array2 = tc.ArrowArrayConstructorFcn(dates1, Valid=[1 4]);
            array3 = tc.ArrowArrayConstructorFcn(dates2, Valid=[1 2 4]);
            array4 = arrow.array([true false true false]);
            array5 = tc.ArrowArrayConstructorFcn(dates3, Valid=[1 2 4]);

            % Their validity bitmaps are not equal
            tc.verifyFalse(isequal(array1, array2));

            % Not all corresponding valid elements are equal
            tc.verifyFalse(isequal(array1, array3));

            % Their Type properties are not equal
            tc.verifyFalse(isequal(array1, array4));

            % Their NumElements properties are not equal
            tc.verifyFalse(isequal(array1, array5));

            % Comparing an arrow.array.Array to a MATLAB double
            tc.verifyFalse(isequal(array1, 1));

            % Test supplying more than two arrays to isequal
            tc.verifyFalse(isequal(array1, array1, array3, array4, array5));
        end

    end

    methods

        function verifyDate64Type(testCase, actual)
            testCase.verifyInstanceOf(actual, "arrow.type.Date64Type");
            testCase.verifyEqual(actual.ID, arrow.type.ID.Date64);
            testCase.verifyEqual(actual.DateUnit, arrow.type.DateUnit.Millisecond);
        end

    end

end

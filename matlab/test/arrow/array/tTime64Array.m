%TTIME64ARRAY Unit tests for arrow.array.Time64Array

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

classdef tTime64Array < matlab.unittest.TestCase

    properties
        ArrowArrayConstructorFcn = @arrow.array.Time64Array.fromMATLAB
    end

    properties(TestParameter)
        Unit = {arrow.type.TimeUnit.Microsecond, arrow.type.TimeUnit.Nanosecond}
    end

    methods (Test)
        function Basic(tc)
            times = seconds(1:4);
            array = tc.ArrowArrayConstructorFcn(times);
            tc.verifyInstanceOf(array, "arrow.array.Time64Array");
            tc.verifyEqual(array.toMATLAB, times');
        end

        function TimeUnitDefaultValue(tc)
            % Verify that the default value of "TimeUnit" is "Microsecond".
            matlabTimes = seconds([1; ...
                                   0.001; ...
                                   2.004521; ...
                                   3.1234564; ...
                                   4.1234566; ...
                                   5.000000123]);
            arrowArray = tc.ArrowArrayConstructorFcn(matlabTimes);
            tc.verifyEqual(arrowArray.Type.TimeUnit, arrow.type.TimeUnit.Microsecond);
            tc.verifyEqual(arrowArray.toMATLAB(), ...
                           seconds([1;...
                                    0.001; ...
                                    2.004521; ...
                                    3.123456; ...
                                    4.123457; ...
                                    5]));
        end

        function TypeIsTime64(tc)
            times = seconds(1:4);
            array = tc.ArrowArrayConstructorFcn(times);
            tc.verifyTime64Type(array.Type, arrow.type.TimeUnit.Microsecond);
        end

        function SupportedTimeUnit(tc)
            import arrow.type.TimeUnit
            times = seconds(1:4);
            
            array = tc.ArrowArrayConstructorFcn(times, TimeUnit="Microsecond");
            tc.verifyTime64Type(array.Type, arrow.type.TimeUnit.Microsecond);

            array = tc.ArrowArrayConstructorFcn(times, TimeUnit=TimeUnit.Microsecond);
            tc.verifyTime64Type(array.Type, arrow.type.TimeUnit.Microsecond);

            array = tc.ArrowArrayConstructorFcn(times, TimeUnit="Nanosecond");
            tc.verifyTime64Type(array.Type, arrow.type.TimeUnit.Nanosecond);

            array = tc.ArrowArrayConstructorFcn(times, TimeUnit=TimeUnit.Nanosecond);
            tc.verifyTime64Type(array.Type, arrow.type.TimeUnit.Nanosecond);
        end

        function UnsupportedTimeUnitError(tc)
            % Verify arrow.array.Time64Array.fromMATLAB() errors if 
            % supplied an unsupported TimeUnit (Second or Millisecond).
            import arrow.type.TimeUnit
            times = seconds(1:4);
            fcn = @() tc.ArrowArrayConstructorFcn(times, TimeUnit="Second");
            tc.verifyError(fcn, "arrow:validate:temporal:UnsupportedTime64TimeUnit");

            fcn = @() tc.ArrowArrayConstructorFcn(times, TimeUnit=TimeUnit.Second);
            tc.verifyError(fcn, "arrow:validate:temporal:UnsupportedTime64TimeUnit");

            fcn = @() tc.ArrowArrayConstructorFcn(times, TimeUnit="Millisecond");
            tc.verifyError(fcn, "arrow:validate:temporal:UnsupportedTime64TimeUnit");

            fcn = @() tc.ArrowArrayConstructorFcn(times, TimeUnit=TimeUnit.Millisecond);
            tc.verifyError(fcn, "arrow:validate:temporal:UnsupportedTime64TimeUnit");
        end

        function TestNumElements(testCase)
            % Verify the NumElements property.

            times = duration.empty(0, 1);
            array = testCase.ArrowArrayConstructorFcn(times);
            testCase.verifyEqual(array.NumElements, int64(0));

            times = duration(1, 2, 3);
            array = testCase.ArrowArrayConstructorFcn(times);
            testCase.verifyEqual(array.NumElements, int64(1));

            times = duration(1, 2, 3) + hours(0:4);
            array = testCase.ArrowArrayConstructorFcn(times);
            testCase.verifyEqual(array.NumElements, int64(5));
        end

        function TestToMATLAB(testCase, Unit)
            % Verify toMATLAB() round-trips the original duration array.
            times = seconds([100 200 355 400]);
            array = testCase.ArrowArrayConstructorFcn(times, TimeUnit=Unit);
            values = toMATLAB(array);
            testCase.verifyEqual(values, times');
        end

        function TestDuration(testCase, Unit)
            % Verify duration() round-trips the original duration array.
            times = seconds([100 200 355 400]);
            array = testCase.ArrowArrayConstructorFcn(times, TimeUnit=Unit);
            values = duration(array);
            testCase.verifyEqual(values, times');
        end

        function TestValid(testCase, Unit)
            % Verify the Valid property returns the expected logical vector.
            times = seconds([100 200 NaN 355 NaN 400]);
            array = testCase.ArrowArrayConstructorFcn(times, TimeUnit=Unit);
            testCase.verifyEqual(array.Valid, [true; true; false; true; false; true]);
            testCase.verifyEqual(toMATLAB(array), times');
            testCase.verifyEqual(duration(array), times');
        end

        function InferNullsTrueNVPair(testCase, Unit)
            % Verify arrow.array.Time64Array.fromMATLAB() behaves as
            % expected when InferNulls=true is provided.

            times = seconds([1 2 NaN 4 5 NaN 7]);
            array = testCase.ArrowArrayConstructorFcn(times, InferNulls=true, TimeUnit=Unit);
            expectedValid = [true; true; false; true; true; false; true];
            testCase.verifyEqual(array.Valid, expectedValid);
            testCase.verifyEqual(toMATLAB(array), times');
            testCase.verifyEqual(duration(array), times');
        end

        function InferNullsFalseNVPair(testCase, Unit)
            % Verify arrow.array.Time64Array.fromMATLAB() behaves as
            % expected when InferNulls=false is provided.

            times = seconds([1 2 NaN 4 5 NaN 7]);
            array = testCase.ArrowArrayConstructorFcn(times, InferNulls=false, TimeUnit=Unit);
            expectedValid = true([7 1]);
            testCase.verifyEqual(array.Valid, expectedValid);

            % If NaN durations were not considered null values, then they
            % are treated like int64(0) values.
            expectedTime = times';
            expectedTime([3 6]) = 0;
            testCase.verifyEqual(toMATLAB(array), expectedTime);
            testCase.verifyEqual(duration(array), expectedTime);
        end

        function TestValidNVPair(testCase, Unit)
            % Verify arrow.array.Time64Array.fromMATLAB() accepts the Valid
            % nv-pair, and it behaves as expected.

            times = seconds([1 2 NaN 4 5 NaN 7]);
            
            % Supply the Valid name-value pair as vector of indices.
            array = testCase.ArrowArrayConstructorFcn(times, TimeUnit=Unit, Valid=[1 2 3 5]);
            testCase.verifyEqual(array.Valid, [true; true; true; false; true; false; false]);
            expectedTimes = times';
            expectedTimes(3) = 0;
            expectedTimes([4 6 7]) = NaN;
            testCase.verifyEqual(toMATLAB(array), expectedTimes);

            % Supply the Valid name-value pair as a logical scalar.
            array = testCase.ArrowArrayConstructorFcn(times, TimeUnit=Unit, Valid=false);
            testCase.verifyEqual(array.Valid, false([7 1]));
            expectedTimes(:) = NaN;
            testCase.verifyEqual(toMATLAB(array), expectedTimes);
        end

        function EmptyDurationVector(testCase)
            % Verify arrow.array.Time64Array.fromMATLAB() accepts any
            % empty-shaped duration as input.

            times = duration.empty(0, 0);
            array = testCase.ArrowArrayConstructorFcn(times);
            testCase.verifyEqual(array.NumElements, int64(0));
            testCase.verifyEqual(array.Valid, logical.empty(0, 1));
            testCase.verifyEqual(toMATLAB(array), duration.empty(0, 1));

            % Test with an N-Dimensional empty array
            times = duration.empty(0, 1, 0);
            array = testCase.ArrowArrayConstructorFcn(times);
            testCase.verifyEqual(array.NumElements, int64(0));
            testCase.verifyEqual(array.Valid, logical.empty(0, 1));
            testCase.verifyEqual(toMATLAB(array), duration.empty(0, 1));
        end

        function ErrorIfNonVector(testCase)
            % Verify arrow.array.Time64Array.fromMATLAB() throws an error
            % if the input provided is not a vector.

            times = duration(200, 45, 34) + hours(0:11);
            times = reshape(times, 2, 6);
            fcn = @() testCase.ArrowArrayConstructorFcn(times);
            testCase.verifyError(fcn, "arrow:array:InvalidShape");

            times = reshape(times, 3, 2, 2);
            fcn = @() testCase.ArrowArrayConstructorFcn(times);
            testCase.verifyError(fcn, "arrow:array:InvalidShape");
        end

        function ErrorIfNonDuration(testCase)
            % Verify arrow.array.Time64Array.fromMATLAB() throws an error
            % if not given a duration as input.

            dates = datetime(2023, 4, 6);
            fcn = @() testCase.ArrowArrayConstructorFcn(dates);
            testCase.verifyError(fcn, "arrow:array:InvalidType");

            numbers = [1; 2; 3; 4];
            fcn = @() testCase.ArrowArrayConstructorFcn(numbers);
            testCase.verifyError(fcn, "arrow:array:InvalidType");
        end

        function MicrosecondPrecision(testCase)
            % Verify microsecond precision when TimeUnit="Microsecond" 
            original = milliseconds(1.0033);
            array = testCase.ArrowArrayConstructorFcn(original, TimeUnit="Microsecond");
            expected = milliseconds(1.003);
            testCase.verifyEqual(duration(array), expected);
        end

        function NanosecondPrecision(testCase)
            % Verify nanosecond precision when TimeUnit="Nanosecond"
            original = milliseconds(1.0030031);
            array = testCase.ArrowArrayConstructorFcn(original, TimeUnit="Nanosecond");
            expected = milliseconds(1.0030030);
            testCase.verifyEqual(duration(array), expected);
        end

        function TestIsEqualTrue(tc, Unit)
            % Verifies arrays are considered equal if:
            %
            %  1. Their Type properties are equal
            %  2. They have the same number of elements (i.e. their NumElements properties are equal)
            %  3. They have the same validity bitmap (i.e. their Valid properties are equal)
            %  4. All corresponding valid elements have the same values
            
            times1 = seconds([1 2 3 4]);
            times2 = seconds([1 2 10 4]);

            array1 = tc.ArrowArrayConstructorFcn(times1, TimeUnit=Unit, Valid=[1 2 4]);
            array2 = tc.ArrowArrayConstructorFcn(times1, TimeUnit=Unit, Valid=[1 2 4]);
            array3 = tc.ArrowArrayConstructorFcn(times2, TimeUnit=Unit, Valid=[1 2 4]);

            tc.verifyTrue(isequal(array1, array2));
            tc.verifyTrue(isequal(array1, array3));

            % Test supplying more than two arrays to isequal
            tc.verifyTrue(isequal(array1, array2, array3)); 
        end

        function TestIsEqualFalse(tc, Unit)
            % Verify isequal returns false when expected.

            times1 = seconds([1 2 3 4]);
            times2 = seconds([1 1 2 3]);
            times3 = seconds([1 2 3 4 5]);

            array1 = tc.ArrowArrayConstructorFcn(times1, TimeUnit=Unit, Valid=[1 2 4]);
            array2 = tc.ArrowArrayConstructorFcn(times1, TimeUnit=Unit, Valid=[1 4]);
            array3 = tc.ArrowArrayConstructorFcn(times2,  TimeUnit=Unit, Valid=[1 2 4]);
            array4 = arrow.array([true false true false]);
            array5 = tc.ArrowArrayConstructorFcn(times3, Valid=[1 2 4]);

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

        function TestIsEqualFalseTimeUnitMismatch(tc)
            % Verify two Time64Arrays are not considered equal if they have
            % different TimeUnit values.
            times1 = seconds([1 2 3 4]);

            array1 = tc.ArrowArrayConstructorFcn(times1, TimeUnit="Nanosecond");
            array2 = tc.ArrowArrayConstructorFcn(times1, TimeUnit="Microsecond");

            % arrays are not equal
            tc.verifyFalse(isequal(array1, array2));
        end

        function RoundTimeBySpecifiedTimeUnit(tc)
            % Verify that the input parameter "TimeUnit" is used to specify
            % the time resolution. The value is rounded off based on the
            % specified "TimeUnit".

            % TimeUnit="Microsecond"
            matlabTimes = seconds([1.000001, ...
                                   2.999999, ...
                                   0.0002004, ...
                                   0.0000035, ...
                                   10.123456499, ...
                                   9.999999543]);
            arrowTimes = tc.ArrowArrayConstructorFcn(matlabTimes, TimeUnit="Microsecond");
            tc.verifyEqual(arrowTimes.toMATLAB(), ...
                           seconds([1.000001, ...
                                    2.999999, ...
                                    0.0002, ...
                                    0.000004, ...
                                    10.123456, ...
                                    10])', ...
                          'AbsTol',seconds(1e-14));

            % TimeUnit="Nanosecond"
            matlabTimes = seconds([1, ...
                                   1.123, ...
                                   1.12345, ...
                                   1.123456, ...
                                   1.1234567, ...
                                   1.12345678, ...
                                   1.123456789, ...
                                   1.1234567894, ...
                                   1.1234567895, ...
                                   1.123456789009]);
            arrowTimes = tc.ArrowArrayConstructorFcn(matlabTimes, TimeUnit="Nanosecond");
            tc.verifyEqual(arrowTimes.toMATLAB(),...
                           seconds([1, ...
                                    1.123, ...
                                    1.12345, ...
                                    1.123456, ...
                                    1.1234567, ...
                                    1.12345678, ...
                                    1.123456789, ...
                                    1.123456789, ...
                                    1.123456790, ...
                                    1.123456789])',...
                          'AbsTol',seconds(1e-15));
        end

        function TimeUnitIsReadOnly(tc)
            % Verify that arrowArray.Type.TimeUnit cannot be changed.

            matlabTimes = seconds([1.000001, 2.999999, 0.0002004]);
            arrowArray = tc.ArrowArrayConstructorFcn(matlabTimes);
            tc.verifyError(@()setfield(arrowArray.Type,"TimeUnit", "Nanosecond"),'MATLAB:class:SetProhibited');
        end
    end

    methods
        function verifyTime64Type(testCase, actual, expectedTimeUnit)
            testCase.verifyInstanceOf(actual, "arrow.type.Time64Type");
            testCase.verifyEqual(actual.ID, arrow.type.ID.Time64);
            testCase.verifyEqual(actual.TimeUnit, expectedTimeUnit);
        end
    end
end
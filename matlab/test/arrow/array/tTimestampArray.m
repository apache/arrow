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

classdef tTimestampArray < matlab.unittest.TestCase
% Tests for arrow.array.TimestampArray

    properties
        ArrowArrayConstructorFcn = @arrow.array.TimestampArray.fromMATLAB
    end

    properties(TestParameter)
        TimeZone = {"" "America/New_York"}
        TimeUnit = {arrow.type.TimeUnit.Second arrow.type.TimeUnit.Millisecond
                    arrow.type.TimeUnit.Microsecond arrow.type.TimeUnit.Nanosecond}
    end

    methods(Test)
        function Basic(tc, TimeZone)
            dates = datetime(2023, 6, 22, TimeZone=TimeZone) + days(0:4);
            arrowArray = tc.ArrowArrayConstructorFcn(dates);
            className = string(class(arrowArray));
            tc.verifyEqual(className, "arrow.array.TimestampArray");
        end

        function TestNumElements(testCase, TimeZone)
        % Verify the NumElements property.

            dates = datetime.empty(0, 1);
            dates.TimeZone = TimeZone;
            arrowArray = testCase.ArrowArrayConstructorFcn(dates);
            testCase.verifyEqual(arrowArray.NumElements, int64(0));

            dates = datetime(2023, 6, 22, TimeZone=TimeZone);
            arrowArray = testCase.ArrowArrayConstructorFcn(dates);
            testCase.verifyEqual(arrowArray.NumElements, int64(1));

            dates = datetime(2023, 6, 22, TimeZone=TimeZone) + days(0:4);
            arrowArray = testCase.ArrowArrayConstructorFcn(dates);
            testCase.verifyEqual(arrowArray.NumElements, int64(5));
        end

        function TestDefaultTimestampType(testCase, TimeZone)
        % Verify the TimestampArray's units is Microsecond by default and
        % its TimeZone value is taken from the input datetime.
            import arrow.array.TimestampArray

            dates = datetime(2023, 6, 22, TimeZone=TimeZone) + days(0:4);
            arrowArray = testCase.ArrowArrayConstructorFcn(dates);
            testCase.verifyTimestampType(arrowArray.Type, arrow.type.TimeUnit.Microsecond, TimeZone);
        end

        function TestSupplyTimeUnit(testCase, TimeZone)
        % Supply the TimeUnit name-value pair at construction.

            dates = datetime(2023, 6, 22, TimeZone=TimeZone) + days(0:4);

            arrowArray = testCase.ArrowArrayConstructorFcn(dates, TimeUnit="Second");
            testCase.verifyTimestampType(arrowArray.Type, arrow.type.TimeUnit.Second, TimeZone);

            arrowArray = testCase.ArrowArrayConstructorFcn(dates, TimeUnit="Millisecond");
            testCase.verifyTimestampType(arrowArray.Type, arrow.type.TimeUnit.Millisecond, TimeZone);

            arrowArray = testCase.ArrowArrayConstructorFcn(dates, TimeUnit="Microsecond");
            testCase.verifyTimestampType(arrowArray.Type, arrow.type.TimeUnit.Microsecond, TimeZone);

            arrowArray = testCase.ArrowArrayConstructorFcn(dates, TimeUnit="Nanosecond");
            testCase.verifyTimestampType(arrowArray.Type, arrow.type.TimeUnit.Nanosecond, TimeZone);
        end

        function TestToMATLAB(testCase, TimeUnit, TimeZone)
        % Verify toMATLAB() round-trips the original datetime array.

            dates = datetime(2023, 6, 22, TimeZone=TimeZone) + days(0:4);

            arrowArray = testCase.ArrowArrayConstructorFcn(dates, TimeUnit=TimeUnit);
            values = toMATLAB(arrowArray);
            testCase.verifyEqual(values, dates');
        end

        function TestDatetime(testCase, TimeUnit, TimeZone)
        % Verify datetime() round-trips the original datetime array.

            dates = datetime(2023, 6, 22, TimeZone=TimeZone) + days(0:4);
            arrowArray = testCase.ArrowArrayConstructorFcn(dates, TimeUnit=TimeUnit);
            values = datetime(arrowArray);
            testCase.verifyEqual(values, dates');
        end

        function TestValid(testCase, TimeZone)
        % Verify the Valid property returns the expected logical vector.

            dates = datetime(2023, 6, 22, TimeZone=TimeZone) + days(0:4);
            dates([2 4]) = NaT;
            arrowArray = testCase.ArrowArrayConstructorFcn(dates);
            testCase.verifyEqual(arrowArray.Valid, [true; false; true; false; true]);
            testCase.verifyEqual(toMATLAB(arrowArray), dates');
            testCase.verifyEqual(datetime(arrowArray), dates');
        end

        function TestInferNulls(testCase, TimeUnit, TimeZone)

            dates = datetime(2023, 6, 22, TimeZone=TimeZone) + days(0:4);
            dates([2 4]) = NaT;

            % Verify NaT is treated as a null value if InferNulls=true.
            expectedDates = dates';
            arrowArray = testCase.ArrowArrayConstructorFcn(dates, TimeUnit=TimeUnit, InferNulls=true);
            testCase.verifyEqual(arrowArray.Valid, [true; false; true; false; true]);
            testCase.verifyEqual(toMATLAB(arrowArray), expectedDates);

            % Verify NaT is not treated as a null value if InferNulls=false.
            % The NaT values are mapped to int64(0).
            arrowArray = testCase.ArrowArrayConstructorFcn(dates, TimeUnit=TimeUnit, InferNulls=false);
            testCase.verifyEqual(arrowArray.Valid, [true; true; true; true; true]);
            
            % If the TimestampArray is zoned, int64(0) may not correspond
            % to Jan-1-1970. getFillValue takes into account the TimeZone.
            fill = getFillValue(TimeZone);
            expectedDates([2 4]) = fill;
            testCase.verifyEqual(toMATLAB(arrowArray), expectedDates);
        end

        function TestValidNVPair(testCase, TimeUnit, TimeZone)

            dates = datetime(2023, 6, 22, TimeZone=TimeZone) + days(0:4);
            dates([2 4]) = NaT;
            
            % Supply the Valid name-value pair as vector of indices.
            arrowArray = testCase.ArrowArrayConstructorFcn(dates, TimeUnit=TimeUnit, Valid=[1 2 5]);
            testCase.verifyEqual(arrowArray.Valid, [true; true; false; false; true]);
            expectedDates = dates';
            expectedDates(2) = getFillValue(TimeZone);
            expectedDates([3 4]) = NaT;
            testCase.verifyEqual(toMATLAB(arrowArray), expectedDates);

            % Supply the Valid name-value pair as a logical scalar.
            arrowArray = testCase.ArrowArrayConstructorFcn(dates, TimeUnit=TimeUnit, Valid=false);
            testCase.verifyEqual(arrowArray.Valid, [false; false; false; false; false]);
            expectedDates(:) = NaT;
            testCase.verifyEqual(toMATLAB(arrowArray), expectedDates);
        end

        function ErrorIfNonVector(testCase)

            dates = datetime(2023, 6, 2) + days(0:11);
            dates = reshape(dates, 2, 6);
            fcn = @() testCase.ArrowArrayConstructorFcn(dates);
            testCase.verifyError(fcn, "arrow:array:InvalidShape");

            dates = reshape(dates, 3, 2, 2);
            fcn = @() testCase.ArrowArrayConstructorFcn(dates);
            testCase.verifyError(fcn, "arrow:array:InvalidShape");
        end

        function ErrorIfNotDatetime(testCase)
            data = seconds(1:4);
            fcn = @() testCase.ArrowArrayConstructorFcn(data);
            testCase.verifyError(fcn, "arrow:array:InvalidType");
        end

        function EmptyDatetimeVector(testCase)
            import arrow.array.TimestampArray

            dates = datetime.empty(0, 0);
            arrowArray = testCase.ArrowArrayConstructorFcn(dates);
            testCase.verifyEqual(arrowArray.NumElements, int64(0));
            testCase.verifyEqual(arrowArray.Valid, logical.empty(0, 1));
            testCase.verifyEqual(toMATLAB(arrowArray), datetime.empty(0, 1));

            % test with NDimensional empty array
            dates = datetime.empty(0, 1, 0);
            arrowArray = testCase.ArrowArrayConstructorFcn(dates);
            testCase.verifyEqual(arrowArray.NumElements, int64(0));
            testCase.verifyEqual(arrowArray.Valid, logical.empty(0, 1));
            testCase.verifyEqual(toMATLAB(arrowArray), datetime.empty(0, 1));
        end

        function TestIsEqualTrue(tc, TimeZone)
            % Verifies arrays are considered equal if:
            %
            %  1. Their Type properties are equal
            %  2. They have the same number of elements (i.e. their NumElements properties are equal)
            %  3. They have the same validity bitmap (i.e. their Valid properties are equal)
            %  4. All corresponding valid elements have the same values
            
            dates1 = datetime(2023, 6, 22, TimeZone=TimeZone) + days(0:4);
            dates2 = datetime(2023, 6, 22, TimeZone=TimeZone) + days([0 1 5 3 4]);

            array1 = tc.ArrowArrayConstructorFcn(dates1, Valid=[1 2 4]);
            array2 = tc.ArrowArrayConstructorFcn(dates1, Valid=[1 2 4]);
            array3 = tc.ArrowArrayConstructorFcn(dates2, Valid=[1 2 4]);

            tc.verifyTrue(isequal(array1, array2));
            tc.verifyTrue(isequal(array1, array3));

            % Test supplying more than two arrays to isequal
            tc.verifyTrue(isequal(array1, array2, array3)); 
        end

        function TestIsEqualFalse(tc, TimeZone)
            % Verify isequal returns false when expected.
            
            dates1 = datetime(2023, 6, 22, TimeZone=TimeZone) + days(0:4);
            dates2 = datetime(2023, 6, 22, TimeZone=TimeZone) + days([1 1 2 3 4]);
            dates3 = datetime(2023, 6, 22, TimeZone=TimeZone) + days(0:5);
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

        function TestIsEqualFalseTimeZoneMismatch(tc)
            % Verify two TimestampArrays are not considered equal if one
            % has a TimeZone and one does not. 
            dates1 = datetime(2023, 6, 22, TimeZone="America/Anchorage") + days(0:4);
            dates2 = dates1 - tzoffset(dates1);
            dates2.TimeZone = '';

            array1 = tc.ArrowArrayConstructorFcn(dates1);
            array2 = tc.ArrowArrayConstructorFcn(dates2);

            % arrays are not equal
            tc.verifyFalse(isequal(array1, array2));
        end

        function TestIsEqualSameInstantDifferentTimeZone(tc)
            % Verify two TimestampArrays are not considered equal if
            % they represent the same "instant in time", but have different
            % TimeZone values.
            dates1 = datetime(2023, 6, 22, TimeZone="America/Anchorage") + days(0:4);
            dates2 = dates1;
            dates2.TimeZone = "America/New_York";

            array1 = tc.ArrowArrayConstructorFcn(dates1);
            array2 = tc.ArrowArrayConstructorFcn(dates2);

            % arrays are not equal
            tc.verifyFalse(isequal(array1, array2));
        end

        function TestIsEqualFalseTimeUnitMismatch(tc, TimeZone)
            % Verify two TimestampArrays are not considered equal if their
            % TimeUnit values differ.
            dates1 = datetime(2023, 6, 22, TimeZone=TimeZone) + days(0:4);
            array1 = tc.ArrowArrayConstructorFcn(dates1, TimeUnit="Millisecond");
            array2 = tc.ArrowArrayConstructorFcn(dates1, TimeUnit="Microsecond");
            tc.verifyFalse(isequal(array1, array2));
        end
    end

    methods 
        function verifyTimestampType(testCase, type, timeUnit, timeZone)
            testCase.verifyTrue(isa(type, "arrow.type.TimestampType"));
            testCase.verifyEqual(type.TimeUnit, timeUnit);
            testCase.verifyEqual(type.TimeZone, timeZone);
        end
    end
end

function fill = getFillValue(timezone)
    fill = datetime(1970, 1, 1, TimeZone=timezone);
    offset = tzoffset(fill);
    if ~isnan(offset)
        fill = fill + offset;
    end
end
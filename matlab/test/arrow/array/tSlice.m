%TSLICE Unit tests verifying the behavior of arrow.array.Array's slice
%method.

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

classdef tSlice < matlab.unittest.TestCase
    
    methods(Test)
        function BooleanArray(testCase)
            % Verify the slice method returns the expected array when
            % called on a Boolean Array.
            boolArray = arrow.array([true true false true true false], Valid=[1 2 3 6]);
            slice = boolArray.slice(int64(2), int64(4));
            testCase.verifyEqual(slice.NumElements, int64(4));
            testCase.verifyEqual(slice.Valid, [true; true; false; false]);
            testCase.verifyEqual(toMATLAB(slice), [true; false; false; false]);
        end

        function NumericArray(testCase)
            % Verify the slice method returns the expected array when
            % called on a Numeric Array.
            float64Array = arrow.array(1:10, Valid=[2 3 4 5 8 10]);
            slice = float64Array.slice(int64(4), int64(5));
            testCase.verifyEqual(slice.NumElements, int64(5));
            testCase.verifyEqual(slice.Valid, [true; true; false; false; true]);
            testCase.verifyEqual(toMATLAB(slice), [4; 5; NaN; NaN; 8]);
        end

        function DateArray(testCase)
            % Verify the slice method returns the expected array when
            % called on a Date Array.
            import arrow.array.Date32Array
            dates = datetime(2023, 11, 8:16);
            date32Array = Date32Array.fromMATLAB(dates, Valid=[4 5 6 9]);
            slice = date32Array.slice(int64(3), int64(4));
            testCase.verifyEqual(slice.NumElements, int64(4));
            testCase.verifyEqual(slice.Valid, [false; true; true; true]);
            expected = [NaT; dates(4:6)'];
            testCase.verifyEqual(toMATLAB(slice), expected);
        end

        function TimeArray(testCase)
            % Verify the slice method returns the expected array when
            % called on a Time Array.
            times = seconds(10:20);
            time64Array = arrow.array(times, Valid=[2 4 6 7 8 10]);
            slice = time64Array.slice(int64(5), int64(6));
            testCase.verifyEqual(slice.NumElements, int64(6));
            testCase.verifyEqual(slice.Valid, [false; true; true; true; false; true]);
            expected = [NaN; times(6:8)'; NaN; times(10)];
            testCase.verifyEqual(toMATLAB(slice), expected);
        end

        function TimestampArray(testCase)
            % Verify the slice method returns the expected array when
            % called on a TimestampArray.
            dates = datetime(2023, 11, 8:16);
            date32Array = arrow.array(dates, Valid=[1 2 4 5 6 8]);
            slice = date32Array.slice(int64(5), int64(3));
            testCase.verifyEqual(slice.NumElements, int64(3));
            testCase.verifyEqual(slice.Valid, [true; true; false]);
            expected = [dates(5:6)'; NaT];
            testCase.verifyEqual(toMATLAB(slice), expected);
        end

        function StringArray(testCase)
            % Verify the slice method returns the expected array when
            % called on a StringArray.
            stringArray = arrow.array(["a" "b" "c" "d" "e" "f" "g"], Valid=[1 3 4 5 6]);
            slice = stringArray.slice(int64(2), int64(3));
            testCase.verifyEqual(slice.NumElements, int64(3));
            testCase.verifyEqual(slice.Valid, [false; true; true]);
            expected = [missing; "c"; "d"];
            testCase.verifyEqual(toMATLAB(slice), expected);
        end

        function ListArray(testCase)
            % Verify the slice method returns the expected array when
            % called on a ListArray.
            cellArray = {missing, [1, 2, 3], missing, [4, NaN], [6, 7, 8], missing};
            listArray = arrow.array(cellArray);
            slice = listArray.slice(int64(2), int64(4));
            testCase.verifyEqual(slice.NumElements, int64(4));
            testCase.verifyEqual(slice.Valid, [true; false; true; true]);
            expected = {[1; 2; 3]; missing; [4; NaN]; [6; 7; 8]};
            testCase.verifyEqual(toMATLAB(slice), expected);
        end

        function StructArray(testCase)
            % Verify the slice method returns the expected array when
            % called on a StructArray.
            numbers = [NaN; 2; 3; 4; 5; 6; 7; NaN; 9; 10];
            text = ["a"; missing; "c"; "d"; "e"; missing; "g"; "h"; "i"; "j"];
            t = table(numbers, text);
            structArray = arrow.array(t, Valid=[1 2 3 6 7 8 10]);
            slice = structArray.slice(int64(5), int64(4));
            testCase.verifyEqual(slice.NumElements, int64(4));
            testCase.verifyEqual(slice.Valid, [false; true; true; true]);
            expected = t(5:8, :);
            expected.numbers(1) = NaN;
            expected.text(1) = missing;
            testCase.verifyEqual(toMATLAB(slice), expected);
        end

        function NonPositiveOffsetError(testCase)
            % Verify the slice method throws an error whose identifier is
            % "arrow:array:slice:NonPositiveOffset" if given a non-positive
            % value as the offset.
            array = arrow.array(1:10);
            fcn = @() array.slice(int64(0), int64(2));
            testCase.verifyError(fcn, "arrow:array:slice:NonPositiveOffset");
            fcn = @() array.slice(int64(-1), int64(2));
            testCase.verifyError(fcn, "arrow:array:slice:NonPositiveOffset");
        end

        function NegativeLengthError(testCase)
            % Verify the slice method throws an error whose identifier is
            % "arrow:array:slice:NegativeLength" if given a negative value
            % as the length.
            array = arrow.array(1:10);
            fcn = @() array.slice(int64(1), int64(-1));
            testCase.verifyError(fcn, "arrow:array:slice:NegativeLength");
        end
    end    
end
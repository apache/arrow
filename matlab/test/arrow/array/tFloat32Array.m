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

classdef tFloat32Array < hNumericArray
% Tests for arrow.array.Float32rray

    properties
        ArrowArrayClassName = "arrow.array.Float32Array"
        ArrowArrayConstructorFcn = @arrow.array.Float32Array.fromMATLAB
        MatlabConversionFcn = @single % single method on class
        MatlabArrayFcn = @single % single function
        MaxValue = realmax("single")
        MinValue = realmin("single")
        NullSubstitutionValue = single(NaN)
        ArrowType = arrow.float32
    end

    methods(Test)
        function InfValues(testCase)
            A1 = testCase.ArrowArrayConstructorFcn(single([Inf -Inf]));
            data = single(A1);
            testCase.verifyEqual(data, single([Inf -Inf]'));
        end

        function ValidBasic(testCase)
            % Create a MATLAB array with one null value (i.e. one NaN).
            % Verify NaN is considered a null value by default.
            matlabArray = single([1, NaN, 3]');
            arrowArray = testCase.ArrowArrayConstructorFcn(matlabArray);
            expectedValid = [true, false, true]';
            testCase.verifyEqual(arrowArray.Valid, expectedValid);
        end

        function InferNulls(testCase)
            matlabArray = single([1, NaN, 3]);

            % Verify NaN is treated as a null value when InferNulls=true.
            arrowArray1 = testCase.ArrowArrayConstructorFcn(matlabArray, InferNulls=true);
            expectedValid1 = [true false true]';
            testCase.verifyEqual(arrowArray1.Valid, expectedValid1);
            testCase.verifyEqual(toMATLAB(arrowArray1), matlabArray');

            % Verify NaN is not treated as a null value when InferNulls=false.
            arrowArray2 = testCase.ArrowArrayConstructorFcn(matlabArray, InferNulls=false);
            expectedValid2 = [true true true]';
            testCase.verifyEqual(arrowArray2.Valid, expectedValid2);
            testCase.verifyEqual(toMATLAB(arrowArray2), matlabArray');
        end

        function ValidNoNulls(testCase)
            % Create a MATLAB array with no null values (i.e. no NaNs).
            matlabArray = single([1, 2, 3]');
            arrowArray = testCase.ArrowArrayConstructorFcn(matlabArray);
            expectedValid = [true, true, true]';
            testCase.verifyEqual(arrowArray.Valid, expectedValid);
        end

        function ValidAllNulls(testCase)
            % Create a MATLAB array with all null values (i.e. all NaNs).
            matlabArray = single([NaN, NaN, NaN]');
            arrowArray = testCase.ArrowArrayConstructorFcn(matlabArray);
            expectedValid = [false, false, false]';
            testCase.verifyEqual(arrowArray.Valid, expectedValid);
        end

        function EmptyArrayValidBitmap(testCase)
            % Create an empty 0x0 MATLAB array.
            matlabArray = single.empty(0, 0);
            arrowArray = testCase.ArrowArrayConstructorFcn(matlabArray);
            expectedValid = logical.empty(0, 1);
            testCase.verifyEqual(arrowArray.Valid, expectedValid);

            % Create an empty 0x1 MATLAB array.
            matlabArray = single.empty(0, 1);
            arrowArray = testCase.ArrowArrayConstructorFcn(matlabArray);
            testCase.verifyEqual(arrowArray.Valid, expectedValid);

            % Create an empty 1x0 MATLAB array.
            matlabArray = single.empty(1, 0);
            arrowArray = testCase.ArrowArrayConstructorFcn(matlabArray);
            testCase.verifyEqual(arrowArray.Valid, expectedValid);
        end

        function LogicalValidNVPair(testCase)
            matlabArray = single([1 2 3]); 

            % Supply a logical vector for Valid
            arrowArray = testCase.ArrowArrayConstructorFcn(matlabArray, Valid=[false; true; true]);
            testCase.verifyEqual(arrowArray.Valid, [false; true; true]);
            testCase.verifyEqual(toMATLAB(arrowArray), single([NaN; 2; 3]));
        end

        function NumericValidNVPair(testCase)
            matlabArray = single([1 2 3]); 

            % Supply a numeric vector for Valid 
            arrowArray = testCase.ArrowArrayConstructorFcn(matlabArray, Valid=[1 3]);
            testCase.verifyEqual(arrowArray.Valid, [true; false; true]);
            testCase.verifyEqual(toMATLAB(arrowArray), single([1; NaN; 3]));
        end

        function TestNanIsEqualFalse(testCase)
            % Verify corresponding NaN values are not considered equal.
            matlabArray = single([1 2 NaN 4]);
            arrowArray = testCase.ArrowArrayConstructorFcn(matlabArray, InferNulls=false);
            testCase.verifyFalse(isequal(arrowArray, arrowArray));
        end
    end
end

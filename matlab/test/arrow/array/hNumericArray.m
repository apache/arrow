classdef hNumericArray < matlab.unittest.TestCase
    % Test class containing shared tests for numeric arrays.

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
    properties (Abstract)
        ArrowArrayClassName(1, 1) string
        ArrowArrayConstructor
        MatlabArrayFcn
        MatlabConversionFcn
        MaxValue (1, 1)
        MinValue (1, 1)
    end

    properties (TestParameter)
        MakeDeepCopy = {true false}
    end

    methods(TestClassSetup)
        function verifyOnMatlabPath(tc)
            % Verify the arrow array class is on the MATLAB Search Path.
            tc.assertTrue(~isempty(which(tc.ArrowArrayClassName)), ...
                """" + tc.ArrowArrayClassName + """must be on the MATLAB path. " + ...
                "Use ""addpath"" to add folders to the MATLAB path.");
        end
    end

    methods(Test)
        function BasicTest(tc, MakeDeepCopy)
            A = tc.ArrowArrayConstructor(tc.MatlabArrayFcn([1 2 3]), DeepCopy=MakeDeepCopy);
            className = string(class(A));
            tc.verifyEqual(className, tc.ArrowArrayClassName);
        end

        function ShallowCopyTest(tc)
            % By default, NumericArrays do not create a deep copy on
            % construction when constructed from a MATLAB array. Instead,
            % it stores a shallow copy of the array keep the memory alive.
            A = tc.ArrowArrayConstructor(tc.MatlabArrayFcn([1, 2, 3]));
            tc.verifyEqual(A.MatlabArray, tc.MatlabArrayFcn([1, 2, 3]));
            tc.verifyEqual(toMATLAB(A), tc.MatlabArrayFcn([1 2 3]'));

            A = tc.ArrowArrayConstructor(tc.MatlabArrayFcn([1, 2, 3]), DeepCopy=false);
            tc.verifyEqual(A.MatlabArray, tc.MatlabArrayFcn([1 2 3]));
            tc.verifyEqual(toMATLAB(A), tc.MatlabArrayFcn([1 2 3]'));
        end

        function DeepCopyTest(tc)
            % Verify NumericArrays does not store shallow copy of the 
            % MATLAB array if DeepCopy=true was supplied.
            A = tc.ArrowArrayConstructor(tc.MatlabArrayFcn([1, 2, 3]), DeepCopy=true);
            tc.verifyEqual(A.MatlabArray, tc.MatlabArrayFcn([]));
            tc.verifyEqual(toMATLAB(A), tc.MatlabArrayFcn([1 2 3]'));
        end

        function ToMATLAB(tc, MakeDeepCopy)
            % Create array from a scalar
            A1 = tc.ArrowArrayConstructor(tc.MatlabArrayFcn(100), DeepCopy=MakeDeepCopy);
            data = toMATLAB(A1);
            tc.verifyEqual(data, tc.MatlabArrayFcn(100));

            % Create array from a vector
            A2 = tc.ArrowArrayConstructor(tc.MatlabArrayFcn([1 2 3]), DeepCopy=MakeDeepCopy);
            data = toMATLAB(A2);
            tc.verifyEqual(data, tc.MatlabArrayFcn([1 2 3]'));

            % Create a Float64Array from an empty double vector
            A3 = tc.ArrowArrayConstructor(tc.MatlabArrayFcn([]), DeepCopy=MakeDeepCopy);
            data = toMATLAB(A3);
            tc.verifyEqual(data, tc.MatlabArrayFcn(reshape([], 0, 1)));
        end

        function MatlabConversion(tc, MakeDeepCopy)
            % Tests the type-specific conversion methods, e.g. single for
            % arrow.array.Float32Array, double for array.array.Float64Array

            % Create array from a scalar
            A1 = tc.ArrowArrayConstructor(tc.MatlabArrayFcn(100), DeepCopy=MakeDeepCopy);
            data = tc.MatlabConversionFcn(A1);
            tc.verifyEqual(data, tc.MatlabArrayFcn(100));

            % Create array from a vector
            A2 = tc.ArrowArrayConstructor(tc.MatlabArrayFcn([1 2 3]), DeepCopy=MakeDeepCopy);
            data = tc.MatlabConversionFcn(A2);
            tc.verifyEqual(data, tc.MatlabArrayFcn([1 2 3]'));

            % Create an array from an empty vector
            A3 = tc.ArrowArrayConstructor(tc.MatlabArrayFcn([]), DeepCopy=MakeDeepCopy);
            data = tc.MatlabConversionFcn(A3);
            tc.verifyEqual(data, tc.MatlabArrayFcn(reshape([], 0, 1)));
        end

        function MinValueTest(tc, MakeDeepCopy)
            A = tc.ArrowArrayConstructor(tc.MinValue, DeepCopy=MakeDeepCopy);
            tc.verifyEqual(toMATLAB(A), tc.MinValue);
        end

        function MaxValueTest(tc, MakeDeepCopy)
            A1 = tc.ArrowArrayConstructor(tc.MaxValue, DeepCopy=MakeDeepCopy);
            tc.verifyEqual(toMATLAB(A1), tc.MaxValue);
        end

        function ErrorIfComplex(tc, MakeDeepCopy)
            fcn = @() tc.ArrowArrayConstructor(tc.MatlabArrayFcn([10 + 1i, 4]), DeepCopy=MakeDeepCopy);
            tc.verifyError(fcn, "MATLAB:expectedReal");
        end

        function ErrorIfNonVector(tc, MakeDeepCopy)
            data = tc.MatlabArrayFcn([1 2 3 4 5 6 7 8 9]);
            data = reshape(data, 3, 1, 3);
            fcn = @() tc.ArrowArrayConstructor(tc.MatlabArrayFcn(data), DeepCopy=MakeDeepCopy);
            tc.verifyError(fcn, "MATLAB:expectedVector");
        end

        function ErrorIfEmptyArrayIsNotTwoDimensional(tc, MakeDeepCopy)
            data = tc.MatlabArrayFcn(reshape([], [1 0 0]));
            fcn = @() tc.ArrowArrayConstructor(data, DeepCopy=MakeDeepCopy);
            tc.verifyError(fcn, "MATLAB:expected2D");
        end
    end
end
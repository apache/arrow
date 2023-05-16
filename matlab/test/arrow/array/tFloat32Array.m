classdef tFloat32Array < matlab.unittest.TestCase
    % Tests for arrow.array.Float32rray

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
    
    properties (TestParameter)
        MakeDeepCopy = {true false}
    end

    methods(TestClassSetup)
        function verifyOnMatlabPath(testCase)
            % arrow.array.Float32Array must be on the MATLAB path.
            testCase.assertTrue(~isempty(which('arrow.array.Float32Array')), ...
                '''arrow.array.Float32Array'' must be on the MATLAB path. Use ''addpath'' to add folders to the MATLAB path.');
        end
    end
    
    methods(Test)
        function Basic(testCase, MakeDeepCopy)
            A = arrow.array.Float32Array(single([1, 2, 3]), DeepCopy=MakeDeepCopy);
            className = string(class(A));
            testCase.verifyEqual(className, "arrow.array.Float32Array");
        end

        function ShallowCopy(testCase)
            % By default, Float32Array does not create a deep copy on
            % construction when constructed from a MATLAB array. Instead,
            % it stores a shallow copy of the array keep the memory alive.
            A = arrow.array.Float32Array(single([1, 2, 3]));
            testCase.verifyEqual(A.MatlabArray, single([1 2 3]));
            testCase.verifyEqual(single(A), single([1 2 3]'));

            A = arrow.array.Float32Array(single([1, 2, 3]), DeepCopy=false);
            testCase.verifyEqual(A.MatlabArray, single([1 2 3]));
            testCase.verifyEqual(single(A), single([1 2 3]'));
        end

        function DeepCopy(testCase)
          % Verify Float32Array does not store shallow copy of the MATLAB
          % array if DeepCopy=true was supplied.
            A = arrow.array.Float32Array(single([1, 2, 3]), DeepCopy=true);
            testCase.verifyEqual(A.MatlabArray, single([]));
            testCase.verifyEqual(single(A), single([1 2 3]'));
        end

        function Single(testCase, MakeDeepCopy)
            % Create a Float32Array from a scalar double
            A1 = arrow.array.Float32Array(single(100), DeepCopy=MakeDeepCopy);
            data = single(A1);
            testCase.verifyEqual(data, single(100));

            % Create a Float32Array from a double vector 
            A2 = arrow.array.Float32Array(single([1 2 3]), DeepCopy=MakeDeepCopy);
            data = single(A2);
            testCase.verifyEqual(data, single([1 2 3]'));
        
            % Create a Float32Array from an empty double vector
            A3 = arrow.array.Float32Array(single([]), DeepCopy=MakeDeepCopy);
            data = single(A3);
            testCase.verifyEqual(data, single.empty(0, 1));
        end

        function MinValue(testCase, MakeDeepCopy)
            A1 = arrow.array.Float32Array(realmin("single"), DeepCopy=MakeDeepCopy);
            data = single(A1);
            testCase.verifyEqual(data, realmin("single"));
        end

        function MaxValue(testCase, MakeDeepCopy)
            A1 = arrow.array.Float32Array(realmax('single'), DeepCopy=MakeDeepCopy);
            data = single(A1);
            testCase.verifyEqual(data, realmax('single'));
        end

        function InfValues(testCase, MakeDeepCopy)
            A1 = arrow.array.Float32Array(single([Inf -Inf]), DeepCopy=MakeDeepCopy);
            data = single(A1);
            testCase.verifyEqual(data, single([Inf -Inf]'));
        end

        function ErrorIfComplex(testCase, MakeDeepCopy)
            fcn = @() arrow.array.Float32Array(single([10 + 1i, 4]), DeepCopy=MakeDeepCopy);
            testCase.verifyError(fcn, "MATLAB:expectedReal");
        end

        function toMATLAB(testCase, MakeDeepCopy)
            A1 = arrow.array.Float32Array(single(100), DeepCopy=MakeDeepCopy);
            data = toMATLAB(A1);
            testCase.verifyEqual(data, single(100));
        end
    end
end

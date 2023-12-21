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

classdef tFloat64Type < hFixedWidthType
% Test class for arrow.type.Float64Type

    properties
        ArrowType = arrow.float64
        TypeID = arrow.type.ID.Float64
        BitWidth = int32(64)
        ClassName = "arrow.type.Float64Type"
    end

    methods(Test)
        function IsEqualTrue(testCase)
            % Verifies isequal method of arrow.type.Float64Type returns true if
            % these conditions are met:
            %
            % 1. All input arguments have a class type arrow.type.Float64Type
            % 2. All inputs have the same size

            % Scalar Float64Type arrays
            float64Type1 = arrow.float64();
            float64Type2 = arrow.float64();
            testCase.verifyTrue(isequal(float64Type1, float64Type2));

            % Non-scalar Float64Type arrays
            typeArray1 = [float64Type1 float64Type1];
            typeArray2 = [float64Type2 float64Type2];
            testCase.verifyTrue(isequal(typeArray1, typeArray2));
        end

        function IsEqualFalse(testCase)
            % Verifies the isequal method of arrow.type.Float64Type returns
            % false when expected.
            
            % Pass a different arrow.type.Type subclass to isequal
            float64Type = arrow.float64();
            int32Type = arrow.int32();
            testCase.verifyFalse(isequal(float64Type, int32Type));
            testCase.verifyFalse(isequal([float64Type float64Type], [int32Type int32Type]));

            % Float64Type arrays have different sizes
            typeArray1 = [float64Type float64Type];
            typeArray2 = [float64Type float64Type]';
            testCase.verifyFalse(isequal(typeArray1, typeArray2));
        end
    end
end
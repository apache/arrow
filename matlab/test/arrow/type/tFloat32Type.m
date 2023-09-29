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

classdef tFloat32Type < hFixedWidthType
% Test class for arrow.type.Float32Type

    properties
        ArrowType = arrow.float32
        TypeID = arrow.type.ID.Float32
        BitWidth = int32(32)
        ClassName = "arrow.type.Float32Type"
    end

    methods(Test)
        function IsEqualTrue(testCase)
            % Verifies isequal method of arrow.type.Float32Type returns true if
            % these conditions are met:
            %
            % 1. All input arguments have a class type arrow.type.Float32Type
            % 2. All inputs have the same size

            % Scalar Float32Type arrays
            float32Type1 = arrow.float32();
            float32Type2 = arrow.float32();
            testCase.verifyTrue(isequal(float32Type1, float32Type2));

            % Non-scalar Float32Type arrays
            typeArray1 = [float32Type1 float32Type1];
            typeArray2 = [float32Type2 float32Type2];
            testCase.verifyTrue(isequal(typeArray1, typeArray2));
        end

        function IsEqualFalse(testCase)
            % Verifies the isequal method of arrow.type.Float32Type returns
            % false when expected.
            
            % Pass a different arrow.type.Type subclass to isequal
            float32Type = arrow.float32();
            int32Type = arrow.int32();
            testCase.verifyFalse(isequal(float32Type, int32Type));
            testCase.verifyFalse(isequal([float32Type float32Type], [int32Type int32Type]));

            % Float32Type arrays have different sizes
            typeArray1 = [float32Type float32Type];
            typeArray2 = [float32Type float32Type]';
            testCase.verifyFalse(isequal(typeArray1, typeArray2));
        end
    end
end
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

classdef tInt8Type < hFixedWidthType
% Test class for arrow.type.Int8Type

    properties
        ArrowType = arrow.int8
        TypeID = arrow.type.ID.Int8
        BitWidth = int32(8)
        ClassName = "arrow.type.Int8Type"
    end

    methods(Test)
        function IsEqualTrue(testCase)
            % Verifies isequal method of arrow.type.Int8Type returns true if
            % these conditions are met:
            %
            % 1. All input arguments have a class type arrow.type.Int8Type
            % 2. All inputs have the same size

            % Scalar Int8Type arrays
            int8Type1 = arrow.int8();
            int8Type2 = arrow.int8();
            testCase.verifyTrue(isequal(int8Type1, int8Type2));

            % Non-scalar Int8Type arrays
            typeArray1 = [int8Type1 int8Type1];
            typeArray2 = [int8Type2 int8Type2];
            testCase.verifyTrue(isequal(typeArray1, typeArray2));
        end

        function IsEqualFalse(testCase)
            % Verifies the isequal method of arrow.type.Int8Type returns
            % false when expected.
            
            % Pass a different arrow.type.Type subclass to isequal
            int8Type = arrow.int8();
            int32Type = arrow.int32();
            testCase.verifyFalse(isequal(int8Type, int32Type));
            testCase.verifyFalse(isequal([int8Type int8Type], [int32Type int32Type]));

            % Int8Type arrays have different sizes
            typeArray1 = [int8Type int8Type];
            typeArray2 = [int8Type int8Type]';
            testCase.verifyFalse(isequal(typeArray1, typeArray2));
        end
    end
end
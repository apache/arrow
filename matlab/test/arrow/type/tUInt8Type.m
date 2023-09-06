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

classdef tUInt8Type < hFixedWidthType
% Test class for arrow.type.UInt64Type

    properties
        ArrowType = arrow.uint8
        TypeID = arrow.type.ID.UInt8
        BitWidth = int32(8)
        ClassName = "arrow.type.UInt8Type"
    end

    methods(Test)
        function IsEqualTrue(testCase)
            % Verifies isequal method of arrow.type.UInt8Type returns true if
            % these conditions are met:
            %
            % 1. All input arguments have a class type arrow.type.UInt8Type
            % 2. All inputs have the same size

            % Scalar UInt8Type arrays
            uint8Type1 = arrow.uint8();
            uint8Type2 = arrow.uint8();
            testCase.verifyTrue(isequal(uint8Type1, uint8Type2));

            % Non-scalar UInt8Type arrays
            typeArray1 = [uint8Type1 uint8Type1];
            typeArray2 = [uint8Type2 uint8Type2];
            testCase.verifyTrue(isequal(typeArray1, typeArray2));
        end

        function IsEqualFalse(testCase)
            % Verifies the isequal method of arrow.type.UInt8Type returns
            % false when expected.
            
            % Pass a different arrow.type.Type subclass to isequal
            uint8Type = arrow.uint8();
            int32Type = arrow.int32();
            testCase.verifyFalse(isequal(uint8Type, int32Type));
            testCase.verifyFalse(isequal([uint8Type uint8Type], [int32Type int32Type]));

            % UInt8Type arrays have different sizes
            typeArray1 = [uint8Type uint8Type];
            typeArray2 = [uint8Type uint8Type]';
            testCase.verifyFalse(isequal(typeArray1, typeArray2));
        end
    end
end
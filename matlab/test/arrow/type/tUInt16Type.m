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

classdef tUInt16Type < hFixedWidthType
% Test class for arrow.type.UInt16Type

    properties
        ArrowType = arrow.uint16
        TypeID = arrow.type.ID.UInt16
        BitWidth = int32(16)
        ClassName = "arrow.type.UInt16Type"
    end

    methods(Test)
        function IsEqualTrue(testCase)
            % Verifies isequal method of arrow.type.UInt16Type returns true if
            % these conditions are met:
            %
            % 1. All input arguments have a class type arrow.type.UInt16Type
            % 2. All inputs have the same size

            % Scalar UInt16Type arrays
            uint16Type1 = arrow.uint16();
            uint16Type2 = arrow.uint16();
            testCase.verifyTrue(isequal(uint16Type1, uint16Type2));

            % Non-scalar UInt16Type arrays
            typeArray1 = [uint16Type1 uint16Type1];
            typeArray2 = [uint16Type2 uint16Type2];
            testCase.verifyTrue(isequal(typeArray1, typeArray2));
        end

        function IsEqualFalse(testCase)
            % Verifies the isequal method of arrow.type.UInt16Type returns
            % false when expected.
            
            % Pass a different arrow.type.Type subclass to isequal
            uint16Type = arrow.uint16();
            int32Type = arrow.int32();
            testCase.verifyFalse(isequal(uint16Type, int32Type));
            testCase.verifyFalse(isequal([uint16Type uint16Type], [int32Type int32Type]));

            % UInt16Type arrays have different sizes
            typeArray1 = [uint16Type uint16Type];
            typeArray2 = [uint16Type uint16Type]';
            testCase.verifyFalse(isequal(typeArray1, typeArray2));
        end
    end
end
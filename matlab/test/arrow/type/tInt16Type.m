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

classdef tInt16Type < hFixedWidthType
% Test class for arrow.type.Int16Type

    properties
        ArrowType = arrow.int16
        TypeID = arrow.type.ID.Int16
        BitWidth = int32(16)
        ClassName = "arrow.type.Int16Type"
    end

    methods(Test)
        function IsEqualTrue(testCase)
            % Verifies isequal method of arrow.type.Int16Type returns true if
            % these conditions are met:
            %
            % 1. All input arguments have a class type arrow.type.Int16Type
            % 2. All inputs have the same size

            % Scalar Int16Type arrays
            int16Type1 = arrow.int16();
            int16Type2 = arrow.int16();
            testCase.verifyTrue(isequal(int16Type1, int16Type2));

            % Non-scalar Int16Type arrays
            typeArray1 = [int16Type1 int16Type1];
            typeArray2 = [int16Type2 int16Type2];
            testCase.verifyTrue(isequal(typeArray1, typeArray2));
        end

        function IsEqualFalse(testCase)
            % Verifies the isequal method of arrow.type.Int16Type returns
            % false when expected.
            
            % Pass a different arrow.type.Type subclass to isequal
            int16Type = arrow.int16();
            int32Type = arrow.int32();
            testCase.verifyFalse(isequal(int16Type, int32Type));
            testCase.verifyFalse(isequal([int16Type int16Type], [int32Type int32Type]));

            % Int16Type arrays have different sizes
            typeArray1 = [int16Type int16Type];
            typeArray2 = [int16Type int16Type]';
            testCase.verifyFalse(isequal(typeArray1, typeArray2));
        end
    end
end
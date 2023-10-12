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

classdef tInt32Type < hFixedWidthType
% Test class for arrow.type.Int32Type

    properties
        ArrowType = arrow.int32
        TypeID = arrow.type.ID.Int32
        BitWidth = int32(32)
        ClassName = "arrow.type.Int32Type"
    end

    methods(Test)
        function IsEqualTrue(testCase)
            % Verifies isequal method of arrow.type.Int32Type returns true if
            % these conditions are met:
            %
            % 1. All input arguments have a class type arrow.type.Int32Type
            % 2. All inputs have the same size

            % Scalar Int32Type arrays
            int32Type1 = arrow.int32();
            int32Type2 = arrow.int32();
            testCase.verifyTrue(isequal(int32Type1, int32Type2));

            % Non-scalar Int32Type arrays
            typeArray1 = [int32Type1 int32Type1];
            typeArray2 = [int32Type2 int32Type2];
            testCase.verifyTrue(isequal(typeArray1, typeArray2));
        end

        function IsEqualFalse(testCase)
            % Verifies the isequal method of arrow.type.Int32Type returns
            % false when expected.
            
            % Pass a different arrow.type.Type subclass to isequal
            int32Type = arrow.int32();
            int64Type = arrow.int64();
            testCase.verifyFalse(isequal(int32Type, int64Type));
            testCase.verifyFalse(isequal([int32Type int32Type], [int64Type int64Type]));

            % Int32Type arrays have different sizes
            typeArray1 = [int32Type int32Type];
            typeArray2 = [int32Type int32Type]';
            testCase.verifyFalse(isequal(typeArray1, typeArray2));
        end
    end
end
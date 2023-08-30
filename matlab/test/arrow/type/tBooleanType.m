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

classdef tBooleanType < hFixedWidthType
% Test class for arrow.type.BooleanType

    properties
        ArrowType = arrow.boolean
        TypeID = arrow.type.ID.Boolean
        BitWidth = int32(1)
        ClassName = "arrow.type.BooleanType"
    end

    methods(Test)
        function IsEqualTrue(testCase)
            % Verifies isequal method of arrow.type.BooleanType returns true if
            % these conditions are met:
            %
            % 1. All input arguments have a class type arrow.type.BooleanType
            % 2. All inputs have the same size

            % Scalar BooleanType arrays
            boolType1 = arrow.boolean();
            boolType2 = arrow.boolean();
            testCase.verifyTrue(isequal(boolType1, boolType2));

            % Non-scalar BooleanType arrays
            typeArray1 = [boolType1 boolType1];
            typeArray2 = [boolType2 boolType2];
            testCase.verifyTrue(isequal(typeArray1, typeArray2));
        end

        function IsEqualFalse(testCase)
            % Verifies the isequal method of arrow.type.BooleanType returns
            % false when expected.
            
            % Pass a different arrow.type.Type subclass to isequal
            boolType = arrow.boolean();
            int32Type = arrow.int32();
            testCase.verifyFalse(isequal(boolType, int32Type));
            testCase.verifyFalse(isequal([boolType boolType], [int32Type int32Type]));

            % BooleanType arrays have different sizes
            typeArray1 = [boolType boolType];
            typeArray2 = [boolType boolType]';
            testCase.verifyFalse(isequal(typeArray1, typeArray2));
        end
    end
end
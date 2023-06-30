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

classdef hPrimitiveType < matlab.unittest.TestCase
% Test class that defines shared unit tests for classes that inherit from
% arrow.type.PrimitiveType

    properties(Abstract)
        ArrowType
        TypeID
        BitWidth
    end

    methods(Test)
        function TestTypeID(testCase)
        % Verify ID is set to the appropriate arrow.type.ID value.
            arrowType = testCase.ArrowType;
            testCase.verifyEqual(arrowType.ID, testCase.TypeID);
        end

        function TestBitWidth(testCase)
        % Verify the BitWidth value.
            arrowType = testCase.ArrowType;
            testCase.verifyEqual(arrowType.BitWidth, testCase.BitWidth);
        end

        function TestNumFields(testCase)
        % Verify NumFields is set to 0 for primitive types.
            arrowType = testCase.ArrowType;
            testCase.verifyEqual(arrowType.NumFields, 0);
        end

        function TestNumBuffers(testCase)
        % Verify NumBuffers is set to 2 for primitive types.
            arrowType = testCase.ArrowType;
            testCase.verifyEqual(arrowType.NumBuffers, 2);
        end
    end
end
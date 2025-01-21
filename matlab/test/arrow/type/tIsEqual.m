%TISEQUAL Verifies the behavior of arrow.type.Type's isequal method on
%nonscalar heterogeneous arrays.

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

classdef tIsEqual < matlab.unittest.TestCase

    methods (Test)
        % Test methods

        function IsEqualTrue(testCase)
            float32Type = arrow.float32();
            timestampType = arrow.timestamp(TimeUnit="Second");
            typeArray = [float32Type, timestampType];
            testCase.verifyTrue(isequal(typeArray, typeArray));
        end

        function IsEqualFalse(testCase)
            float32Type = arrow.float32();
            timestampType1 = arrow.timestamp(TimeUnit="Second");
            typeArray1 = [float32Type, timestampType1];

            % Their dimensions are not equal
            testCase.verifyFalse(isequal(typeArray1, typeArray1'));

            % Corresponding elements are not equal
            timestampType2 = arrow.timestamp(TimeUnit="Millisecond");
            typeArray2 = [float32Type, timestampType2];
            testCase.verifyFalse(isequal(typeArray1, typeArray2));

            % Corresponding elements have different class types
            stringType = arrow.string();
            typeArray3 = [float32Type, stringType];
            testCase.verifyFalse(isequal(typeArray1, typeArray3));
        end

    end

end
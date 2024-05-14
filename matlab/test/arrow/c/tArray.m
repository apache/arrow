%TARRAY Defines unit tests for arrow.c.Array.

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
classdef tArray < matlab.unittest.TestCase

    methods (Test)
        function TestClassStructure(testCase)
            array = arrow.c.Array();
            
            % Verify array is an instance of arrow.c.Array.
            testCase.verifyInstanceOf(array, "arrow.c.Array");
            
            % Verify array has one public property named Address.
            props = properties(array);
            testCase.verifyEqual(props, {'Address'});
        end

        function TestAddressProperty(testCase)
            array = arrow.c.Array();

            % It's impossible to know what the value of Address will be.
            % Just verify Address is a scalar uint64.
            address = array.Address;
            testCase.verifyInstanceOf(address, "uint64");
            testCase.verifyTrue(isscalar(address));
        end

        function TestAddressNoSetter(testCase)
            % Verify the Address property is read-only.
            array = arrow.c.Array();
            fcn = @() setfield(array, "Address", uint64(10));
            testCase.verifyError(fcn, "MATLAB:class:SetProhibited");
        end
    end
end
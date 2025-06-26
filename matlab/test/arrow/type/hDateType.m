% Test class for the arrow.type.DateType interface.

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

classdef hDateType < hFixedWidthType

    properties(Abstract)
        DefaultDateUnit(1, 1) arrow.type.DateUnit
        ClassConstructorFcn
    end

    methods (Test)
        function TestClass(testCase)
            % Verify ArrowType is an object of the expected class type.
            name = string(class(testCase.ArrowType()));
            testCase.verifyEqual(name, testCase.ClassName);
        end

        function DateUnitNoSetter(testCase)
            % Verify that an error is thrown when trying to set the value
            % of the DateUnit property.
            type = testCase.ConstructionFcn();
            testCase.verifyError(@() setfield(type, "DateUnit", "Millisecond"), "MATLAB:class:SetProhibited");
        end

        function InvalidProxy(testCase)
            % Verify that an error is thrown when a Proxy of an unexpected
            % type is passed to its constructor.
            array = arrow.array([1, 2, 3]);
            proxy = array.Proxy;
            testCase.verifyError(@() testCase.ClassConstructorFcn(proxy), "arrow:proxy:ProxyNameMismatch");
        end
    end

end
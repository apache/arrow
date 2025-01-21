%TVALIDATE Unit tests for arrow.internal.proxy.validate

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

classdef tValidate < matlab.unittest.TestCase

    methods(Test)
        function ProxyNameMatches(testCase)
            % Verify arrow.internal.proxy.validate() does not throw an 
            % error if the Name property of the Proxy provided matches
            % the expected proxy name.
            import arrow.internal.proxy.validate
            a = arrow.array(1);
            fcn = @() validate(a.Proxy, a.Proxy.Name);
            testCase.verifyWarningFree(fcn);
        end

        function ProxyNameMismatchError(testCase)
            % Verify arrow.internal.proxy.validate() throws an error 
            % whose identifier is "arrow:proxy:ProxyNameMismatch" if the
            % Name property of the Proxy provided does not match the 
            % expected proxy name.
            import arrow.internal.proxy.validate
            a = arrow.array(1);
            fcn = @() validate(a.Proxy, "NotAProxyName");
            testCase.verifyError(fcn, "arrow:proxy:ProxyNameMismatch");
        end
    end
end
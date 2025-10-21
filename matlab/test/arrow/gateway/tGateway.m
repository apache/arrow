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

classdef tGateway < matlab.unittest.TestCase
    % Tests for libmexclass.proxy.gateway error conditions.

    methods (Test)
        function UnknownProxyError(testCase)
            % Verify the gateway function errors if given the name of an
            % unknown proxy class.
            id = "arrow:matlab:proxy:UnknownProxy";
            fcn = @()libmexclass.proxy.gateway("Create", "NotAProxyClass", {});
            testCase.verifyError(fcn, id);
        end

        function TimestampUnknownTimeUnit(testCase)
            % Verify the proxy constructor throws an error with the
            % expected ID when given an unknown TimeUnit. Not hittable
            % from arrow.array.TimestampArray.
            proxyName = "arrow.array.proxy.TimestampArray";
            args = struct(MatlabArray=int64(0), Valid=true, TimeZone="", TimeUnit="bad");
            fcn = @() libmexclass.proxy.Proxy(Name=proxyName, ConstructorArguments={args});
            testCase.verifyError(fcn, "arrow:matlab:UnknownTimeUnit");
        end

        function TimeZoneUnicodeError(testCase)
            % Verify the proxy constructor throws an error with the
            % expected ID when given an invalid UTF-16 string as the 
            % TimeZone. Not hittable from  arrow.array.TimestampArray.
            proxyName = "arrow.array.proxy.TimestampArray";
            args = struct(MatlabArray=int64(0), Valid=true, TimeZone=string(char(0xD83D)), TimeUnit="Second");
            fcn = @() libmexclass.proxy.Proxy(Name=proxyName, ConstructorArguments={args});
            testCase.verifyError(fcn, "arrow:matlab:unicode:UnicodeConversion");
        end
    end
end
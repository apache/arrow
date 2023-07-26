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

classdef tStringType < matlab.unittest.TestCase
%TSTRINGTYPE Test class for arrow.type.StringType

    methods (Test)

        function Basic(tc)
            type = arrow.string;
            className = string(class(type));
            tc.verifyEqual(className, "arrow.type.StringType");
            tc.verifyEqual(type.ID, arrow.type.ID.String);
        end

        function NumFields(tc)
            type = arrow.string;
            tc.verifyEqual(type.NumFields, int32(0));
        end

    end

end


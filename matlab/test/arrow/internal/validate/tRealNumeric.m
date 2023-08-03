%TREALNUMERIC Unit tests for arrow.internal.validate.realnumeric.

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

classdef tRealNumeric < matlab.unittest.TestCase

    properties(TestParameter)
        NumericType = struct(uint8=@uint8, ...
                             uint16=@uint16, ...
                             uint32=@uint32, ...
                             uint64=@uint64, ...
                             int8=@int8,...
                             int16=@int16, ...
                             int32=@int32, ... 
                             int64=@int64, ...
                             single=@single, ...
                             double=@double);
    end
    

    methods(Test)
        function ErrorIfComplex(testCase, NumericType)
            complexValue = NumericType(10 + 1i);
            fcn = @() arrow.internal.validate.realnumeric(complexValue);
            errID = "arrow:array:ComplexNumeric";
            testCase.verifyError(fcn, errID);
        end
    end
    
end
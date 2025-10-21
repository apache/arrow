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

classdef tInt8Array < hNumericArray
% Tests for arrow.array.Int8Array
    
    properties
        ArrowArrayClassName = "arrow.array.Int8Array"
        ArrowArrayConstructorFcn = @arrow.array.Int8Array.fromMATLAB
        MatlabConversionFcn = @int8 % int8 method on class
        MatlabArrayFcn = @int8 % int8 function
        MaxValue = intmax("int8")
        MinValue = intmin("int8")
        NullSubstitutionValue = int8(0)
        ArrowType = arrow.int8
    end

end

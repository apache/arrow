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

classdef tInt64Array < hNumericArray
% Tests for arrow.array.Int64Array

    properties
        ArrowArrayClassName = "arrow.array.Int64Array"
        ArrowArrayConstructorFcn = @arrow.array.Int64Array.fromMATLAB
        MatlabConversionFcn = @int64 % int64 method on class
        MatlabArrayFcn = @int64 % int64 function
        MaxValue = intmax("int64")
        MinValue = intmin("int64")
        NullSubstitutionValue = int64(0)
        ArrowType = arrow.int64
    end
end

classdef tFloat32Array < hNumericArray
    % Tests for arrow.array.Float32rray

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
    
    properties
        ArrowArrayClassName = "arrow.array.Float32Array"
        ArrowArrayConstructor = @arrow.array.Float32Array
        MatlabConversionFcn = @single % single method on class
        MatlabArrayFcn = @single % single function
        MaxValue = realmax("single")
        MinValue = realmin("single")
    end

    methods(Test)
        function InfValues(testCase, MakeDeepCopy)
            A1 = arrow.array.Float32Array(single([Inf -Inf]), DeepCopy=MakeDeepCopy);
            data = single(A1);
            testCase.verifyEqual(data, single([Inf -Inf]'));
        end
    end
end

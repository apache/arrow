classdef tFloat64Array < hNumericArray
    % Tests for arrow.array.Float64Array

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
        ArrowArrayClassName = "arrow.array.Float64Array"
        ArrowArrayConstructor = @arrow.array.Float64Array
        MatlabConversionFcn = @double % double method on class
        MatlabArrayFcn = @double % double function
        MaxValue = realmax("double")
        MinValue = realmin("double")
    end

    methods(Test)
        function InfValues(testCase, MakeDeepCopy)
            A1 = arrow.array.Float64Array([Inf -Inf], DeepCopy=MakeDeepCopy);
            data = double(A1);
            testCase.verifyEqual(data, [Inf -Inf]');
        end

        function ErrorIfSparse(testCase, MakeDeepCopy)
            fcn = @() arrow.array.Float64Array(sparse(ones([10 1])), DeepCopy=MakeDeepCopy);
            testCase.verifyError(fcn, "MATLAB:expectedNonsparse");
        end
    end
end

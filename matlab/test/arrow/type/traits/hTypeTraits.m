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

classdef hTypeTraits < matlab.unittest.TestCase
% Superclass for tests that validate the behavior of "type trait" objects
% like arrow.type.traits.StringTraits.

    properties (Abstract)
        TraitsConstructor
        ArrayConstructor
        ArrayClassName
        ArrayProxyClassName
        ArrayStaticConstructor
        TypeConstructor
        TypeClassName
        TypeProxyClassName
        MatlabConstructor
        MatlabClassName
    end

    properties
        Traits
    end

    methods (TestMethodSetup)
        function setupTraits(testCase)
            testCase.Traits = testCase.TraitsConstructor();
        end
    end

    methods(Test)

        function TestArrayConstructor(testCase)
            testCase.verifyEqual(testCase.Traits.ArrayConstructor, testCase.ArrayConstructor);
        end

        function TestArrayClassName(testCase)
            testCase.verifyEqual(testCase.Traits.ArrayClassName, testCase.ArrayClassName);
        end

        function TestArrayProxyClassName(testCase)
            testCase.verifyEqual(testCase.Traits.ArrayProxyClassName, testCase.ArrayProxyClassName);
        end

        function TestArrayStaticConstructor(testCase)
            testCase.verifyEqual(testCase.Traits.ArrayStaticConstructor, testCase.ArrayStaticConstructor);
        end

        function TestTypeConstructor(testCase)
            testCase.verifyEqual(testCase.Traits.TypeConstructor, testCase.TypeConstructor);
        end
        
        function TestTypeClassName(testCase)
            testCase.verifyEqual(testCase.Traits.TypeClassName, testCase.TypeClassName);
        end

        function TestTypeProxyClassName(testCase)
            testCase.verifyEqual(testCase.Traits.TypeProxyClassName, testCase.TypeProxyClassName);
        end

        function TestMatlabConstructor(testCase)
            testCase.verifyEqual(testCase.Traits.MatlabConstructor, testCase.MatlabConstructor);
        end

        function TestMatlabClassName(testCase)
            testCase.verifyEqual(testCase.Traits.MatlabClassName, testCase.MatlabClassName);
        end
        
    end

end
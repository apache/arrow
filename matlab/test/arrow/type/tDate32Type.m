% Test class for arrow.type.Date32Type and arrow.date32

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

classdef tDate32Type < hDateType

    properties
        ConstructionFcn = @arrow.date32
        ArrowType = arrow.date32
        TypeID = arrow.type.ID.Date32
        BitWidth = int32(32)
        ClassName = "arrow.type.Date32Type"
        DefaultDateUnit = arrow.type.DateUnit.Day
        ClassConstructorFcn = @arrow.type.Date32Type
    end

    methods(Test)

        function IsEqualTrue(testCase)
            % Verifies isequal method of arrow.type.Date32Type returns true if
            % these conditions are met:
            %
            % 1. All input arguments have a class type arrow.type.Date32Type
            % 2. All inputs have the same size

            % Scalar Date32Type arrays
            date32Type1 = arrow.date32();
            date32Type2 = arrow.date32();
            testCase.verifyTrue(isequal(date32Type1, date32Type2));

            % Non-scalar Date32Type arrays
            typeArray1 = [date32Type1 date32Type1];
            typeArray2 = [date32Type2 date32Type2];
            testCase.verifyTrue(isequal(typeArray1, typeArray2));
        end

        function IsEqualFalse(testCase)
            % Verifies the isequal method of arrow.type.Date32Type returns
            % false when expected.
            
            % Pass a different arrow.type.Type subclass to isequal
            date32Type = arrow.date32();
            int32Type = arrow.int32();
            testCase.verifyFalse(isequal(date32Type, int32Type));
            testCase.verifyFalse(isequal([date32Type date32Type], [int32Type int32Type]));

            % Date32Type arrays have different sizes
            typeArray1 = [date32Type date32Type];
            typeArray2 = [date32Type date32Type]';
            testCase.verifyFalse(isequal(typeArray1, typeArray2));
        end
    
    end

end

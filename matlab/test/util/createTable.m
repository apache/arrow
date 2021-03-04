function t = createTable()
% CREATETABLE Helper function for creating test table.

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

variableNames = {'uint8', ...
                 'uint16', ...
                 'uint32', ...
                 'uint64', ...
                 'int8', ...
                 'int16', ...
                 'int32', ...
                 'int64', ...
                 'single', ...
                 'double'};

variableTypes = {'uint8', ...
                 'uint16', ...
                 'uint32', ...
                 'uint64', ...
                 'int8', ...
                 'int16', ...
                 'int32', ...
                 'int64', ...
                 'single', ...
                 'double'};

uint8Data  = uint8([1; 2; 3]);
uint16Data = uint16([1; 2; 3]);
uint32Data = uint32([1; 2; 3]);
uint64Data = uint64([1; 2; 3]);
int8Data   = int8([1; 2; 3]);
int16Data  = int16([1; 2; 3]);
int32Data  = int32([1; 2; 3]);
int64Data  = int64([1; 2; 3]);
singleData = single([1/2; 1/4; 1/8]);
doubleData = double([1/10; 1/100; 1/1000]);

numRows = 3;
numVariables = 10;

t = table('Size', [numRows, numVariables], 'VariableTypes', variableTypes, 'VariableNames', variableNames);

t.uint8  = uint8Data;
t.uint16 = uint16Data;
t.uint32 = uint32Data;
t.uint64 = uint64Data;
t.int8   = int8Data;
t.int16  = int16Data;
t.int32  = int32Data;
t.int64  = int64Data;
t.single = singleData;
t.double = doubleData;

end
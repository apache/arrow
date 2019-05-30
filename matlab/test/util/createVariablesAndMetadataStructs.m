function [variables, metadata] = createVariablesAndMetadataStructs()
% CREATEVARIABLESANDMETADATASTRUCTS Helper function for creating
% Feather MEX variables and metadata structs.

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

import mlarrow.util.*;

type = 'uint8';
data = uint8([1; 2; 3]);
valid = logical([0; 1; 0]);
name = 'uint8';
uint8Variable = createVariableStruct(type, data, valid, name);

type = 'uint16';
data = uint16([1; 2; 3]);
valid = logical([0; 1; 0]);
name = 'uint16';
uint16Variable = createVariableStruct(type, data, valid, name);

type = 'uint32';
data = uint32([1; 2; 3]);
valid = logical([0; 1; 0]);
name = 'uint32';
uint32Variable = createVariableStruct(type, data, valid, name);

type = 'uint64';
data = uint64([1; 2; 3]);
valid = logical([0; 1; 0]);
name = 'uint64';
uint64Variable = createVariableStruct(type, data, valid, name);

type = 'int8';
data = int8([1; 2; 3]);
valid = logical([0; 1; 0]);
name = 'int8';
int8Variable = createVariableStruct(type, data, valid, name);

type = 'int16';
data = int16([1; 2; 3]);
valid = logical([0; 1; 0]);
name = 'int16';
int16Variable = createVariableStruct(type, data, valid, name);

type = 'int32';
data = int32([1; 2; 3]);
valid = logical([0; 1; 0]);
name = 'int32';
int32Variable = createVariableStruct(type, data, valid, name);

type = 'int64';
data = int64([1; 2; 3]);
valid = logical([0; 1; 0]);
name = 'int64';
int64Variable = createVariableStruct(type, data, valid, name);

type = 'single';
data = single([1; 2; 3]);
valid = logical([0; 1; 0]);
name = 'single';
singleVariable = createVariableStruct(type, data, valid, name);

type = 'double';
data = double([1; 2; 3]);
valid = logical([0; 1; 0]);
name = 'double';
doubleVariable = createVariableStruct(type, data, valid, name);

variables = [uint8Variable, ...
             uint16Variable, ...
             uint32Variable, ...
             uint64Variable, ...
             int8Variable, ...
             int16Variable, ...
             int32Variable, ...
             int64Variable, ...
             singleVariable, ...
             doubleVariable];

description = 'test';
numRows = 3;
numVariables = length(variables);

metadata = createMetadataStruct(description, numRows, numVariables);
end

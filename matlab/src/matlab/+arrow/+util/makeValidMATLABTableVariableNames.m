function [variableNames, variableDescriptions] = makeValidMATLABTableVariableNames(columnNames)
% makeValidMATLABTableVariableNames Makes valid MATLAB table variable names
% from a set of Feather table column names.
% 
% [variableNames, variableDescriptions] = makeValidMATLABTableVariableNames(columnNames)
% Modifies the input Feather table columnNames to be valid MATLAB table
% variable names if they are not already. If any of the Feather table columnNames
% are invalid MATLAB table variable names, then the original columnNames are returned
% in variableDescriptions to be stored in the table.Properties.VariableDescriptions
% property.

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

    variableNames = string(columnNames);
    variableDescriptions = strings(0, 0);
    
    validVariableNames = false(1, length(variableNames));
    for ii = 1:length(variableNames)
        validVariableNames(ii) = isvarname(variableNames(ii));
    end
    
    if ~all(validVariableNames)
        variableDescriptions = strings(1, length(columnNames));
        variableDescriptions(validVariableNames) = "";
        variableDescriptions(~validVariableNames) = compose("Original variable name: '%s'", ...
                                                          variableNames(~validVariableNames));
        variableNames(~validVariableNames) = matlab.lang.makeValidName(variableNames(~validVariableNames));
    end
end

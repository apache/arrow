%MAKEVALIDVARIABLENAMES Makes valid table variable names.

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
function [varnames, modified] = makeValidVariableNames(varnames)
    arguments
        varnames(1, :) string
    end

    reservedNames = ["Properties", "VariableNames", "RowNames", ":"];

    [varnames, replacedVars] = replaceEmptyVariableNames(varnames);
    [varnames, madeUnique] = matlab.lang.makeUniqueStrings(varnames, reservedNames, 63);
    
    modified = replacedVars || any(madeUnique);
end

function [varnames, modified] = replaceEmptyVariableNames(varnames)
    emptyIndices = find(varnames == "");
    modified = any(emptyIndices);
    if modified
        varnames(emptyIndices) = compose("Var%d", emptyIndices);
    end
end

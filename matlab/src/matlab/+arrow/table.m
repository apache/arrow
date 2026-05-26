%TABLE Creates an arrow.tabular.Table from a MATLAB table.

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
function arrowTable = table(matlabTable)
    arguments
        % Use istable instead of the table type specifier here to avoid
        % ambiguous name parsing issue with MATLAB table type and arrow.table.
        matlabTable {istable} = table.empty(0, 0)
    end
    arrowArrays = arrow.tabular.internal.decompose(matlabTable);
    columnNames = string(matlabTable.Properties.VariableNames);
    arrowTable = arrow.tabular.Table.fromArrays(arrowArrays{:}, ColumnNames=columnNames);
end

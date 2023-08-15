%VAIDATECOLUMNNAMES Validates the column names array (if provided) has 
%the expected number of elements. Otherwise returns a string array
%whose values are "Column1", "Column2", etc. up to the number of columns in
%record batch.

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

function columnNames = validateColumnNames(opts, numColumns)
    if ~isfield(opts, "ColumnNames")
        columnNames = compose("Column%d", 1:numColumns);
    elseif numel(opts.ColumnNames) ~= numColumns
        errid = "arrow:tabular:WrongNumberColumnNames";
        msg = compose("Expected ColumnNames to have %d values.", numColumns);
        error(errid, msg);
    else
        columnNames = opts.ColumnNames;
    end
end

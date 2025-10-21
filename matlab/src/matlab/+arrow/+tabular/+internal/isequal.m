%ISEQUAL Utility function used by both arrow.tabular.RecordBatch and
%arrow.tabular.Table to implement the isequal method.

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

function tf = isequal(tabularObj, varargin)
    narginchk(2, inf);
    tf = false;

    classType = string(class(tabularObj));

    schemasToCompare = cell([1 numel(varargin)]);
    for ii = 1:numel(varargin)
        element = varargin{ii};
        if ~isa(element, classType)
            % If element is not an instance of classType, then it cannot
            % be equal to tabularObj. Return false early. 
            return;
        end
        schemasToCompare{ii} = element.Schema;
    end

    if ~isequal(tabularObj.Schema, schemasToCompare{:})
        % If the schemas are not equal, then the record batches (or tables)
        % are not equal. Return false early.
        return;
    end

    % Function that extracts the column stored at colIndex from the
    % record batch (or table) stored at tabularIndex in varargin.
    getColumnFcn = @(tabularIndex, colIndex) varargin{tabularIndex}.column(colIndex);

    tabularObjIndices = 1:numel(varargin);
    for ii = 1:tabularObj.NumColumns
        colIndices = repmat(ii, [1 numel(tabularObjIndices)]);
        % Gather all columns at index ii across the record batches (or
        % tables) stored in varargin. Compare these columns with the
        % corresponding column in obj. If they are not equal, then the
        % record batches (or tables) are not equal. Return false.
        columnsToCompare = arrayfun(getColumnFcn, tabularObjIndices, colIndices, UniformOutput=false);
        if ~isequal(tabularObj.column(ii), columnsToCompare{:})
            return;
        end
    end
    tf = true;
end


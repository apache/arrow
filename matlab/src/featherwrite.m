function featherwrite(filename, t)
%FEATHERWRITE Write a table to a Feather file.
%   Use the FEATHERWRITE function to write a table to
%   a Feather file as column-oriented data.
%
%   FEATHERWRITE(FILENAME,T) writes the table T to a Feather
%   file FILENAME as column-oriented data.

% Licensed to the Apache Software Foundation (ASF) under one
% or more contributor license agreements.  See the NOTICE file
% distributed with this work for additional information
% regarding copyright ownership.  The ASF licenses this file
% to you under the Apache License, Version 2.0 (the
% "License"); you may not use this file except in compliance
% with the License.  You may obtain a copy of the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing,
% software distributed under the License is distributed on an
% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
% KIND, either express or implied.  See the License for the
% specific language governing permissions and limitations
% under the License.

% Validate input arguments.
narginchk(2, 2);
filename = convertStringsToChars(filename);
if ~ischar(filename)
    error('MATLAB:arrow:InvalidFilenameDatatype', ...
        'Filename must be a character vector or string scalar.');
end
if ~istable(t)
    error('MATLAB:arrow:InvalidInputTable', 't must be a table.');
end

% Currently, featherwrite only supports Version 2 Feather files.
featherVersion = 2;

% Struct array representing the underlying data of each variable
% in the given table.
variables = repmat(struct('Type', '', ...
                          'Data', [], ...
                          'Valid', [], ...
                          'Name', ''), 1, width(t));

% Struct representing table-level metadata.
metadata = struct('Version', featherVersion, ...
                  'Description', t.Properties.Description, ...
                  'NumRows', height(t), ...
                  'NumVariables', width(t));

% Iterate over each variable in the given table,
% extracting the underlying array data.
for ii = 1:width(t)
    data = t{:, ii};
    % Multi-column table variables are unsupported.
    if ~isvector(data)
        error('MATLAB:arrow:MultiColumnVariablesUnsupported', ...
              'Multi-column table variables are unsupported by featherwrite.');
    end
    % Get the datatype of the current variable's underlying array.
    variables(ii).Type = class(data);
    % Break the datatype down into its constituent components, if appropriate.
    switch variables(ii).Type
        % For numeric variables, the underlying array data can
        % be passed to the C++ layer directly.
        case {'uint8', 'uint16', 'uint32', 'uint64', ...
              'int8', 'int16', 'int32', 'int64', ...
              'single', 'double'}
            variables(ii).Data = data;
        otherwise
            error('MATLAB:arrow:UnsupportedVariableType', ...
                 ['Type ' variables(ii).Type ' is unsupported by featherwrite.']);
    end
    variables(ii).Valid = ~ismissing(data);
    variables(ii).Name = t.Properties.VariableNames{ii};
end

% Write the table to a Feather file.
featherwritemex(filename, variables, metadata);

end

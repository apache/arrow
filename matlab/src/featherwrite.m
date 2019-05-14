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

import mlarrow.util.table2mlarrow;

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

[variables, metadata] = table2mlarrow(t);

% Write the table to a Feather file.
featherwritemex(filename, variables, metadata);

end

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

toolboxFolder = string(getenv("ARROW_MATLAB_TOOLBOX_FOLDER"));
outputFolder = string(getenv("ARROW_MATLAB_TOOLBOX_OUTPUT_FOLDER"));
toolboxVersionRaw = string(getenv("ARROW_MATLAB_TOOLBOX_VERSION"));

% Output folder must exist.
mkdir(outputFolder);

disp("Toolbox Folder: " + toolboxFolder);
disp("Output Folder: " + outputFolder);
disp("Toolbox Version Raw: " + toolboxVersionRaw);

% Note: This string processing heuristic may not be robust to future
% changes in the Arrow versioning scheme.
dotIdx = strfind(toolboxVersionRaw, ".");
numDots = numel(dotIdx);
if numDots >= 3
    toolboxVersion = extractBefore(toolboxVersionRaw, dotIdx(3));
else
    toolboxVersion = toolboxVersionRaw;
end

disp("Toolbox Version:" + toolboxVersion);

identifier = "ARROW-MATLAB-TOOLBOX";

opts = matlab.addons.toolbox.ToolboxOptions(toolboxFolder, identifier);
opts.ToolboxName = "MATLAB Interface to Arrow";
opts.ToolboxVersion = toolboxVersion;

% Set the SupportedPlatforms
opts.SupportedPlatforms.Win64 = true;
opts.SupportedPlatforms.Maci64 = true;
opts.SupportedPlatforms.Glnxa64 = true;
opts.SupportedPlatforms.MatlabOnline = true;

% TODO: Determine what to set the min/max release to
opts.MinimumMatlabRelease = "R2023a";
opts.MaximumMatlabRelease = "";

opts.OutputFile = fullfile(outputFolder, compose("matlab-arrow-%s.mltbx", toolboxVersionRaw));
disp("Output File: " + opts.OutputFile);
matlab.addons.toolbox.packageToolbox(opts);

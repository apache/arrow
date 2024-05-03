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

appendLicenseText(fullfile(toolboxFolder, "LICENSE.txt"));
appendNoticeText(fullfile(toolboxFolder, "NOTICE.txt"));

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

identifier = "ad1d0fe6-22d1-4969-9e6f-0ab5d0f12ce3";
opts = matlab.addons.toolbox.ToolboxOptions(toolboxFolder, identifier);
opts.ToolboxName = "MATLAB Arrow Interface";
opts.ToolboxVersion = toolboxVersion;
opts.AuthorName = "The Apache Software Foundation";
opts.AuthorEmail = "dev@arrow.apache.org";

% Set the SupportedPlatforms
opts.SupportedPlatforms.Win64 = true;
opts.SupportedPlatforms.Maci64 = true;
opts.SupportedPlatforms.Glnxa64 = true;
opts.SupportedPlatforms.MatlabOnline = true;

% MEX files use run-time libraries shipped with MATLAB (e.g. libmx, libmex,
% etc.). MEX files linked against earlier versions of MALTAB run-time libraries
% will most likely work on newer versions of MATLAB. However, this may not
% always be the case.
% 
% For now, set the earliest and latest compatible releases of MATLAB to 
% the release of MATLAB used to build and package the MATLAB Arrow Interface.
% 
% See: https://www.mathworks.com/help/matlab/matlab_external/version-compatibility.html
currentRelease = matlabRelease.Release;
opts.MinimumMatlabRelease = currentRelease;
opts.MaximumMatlabRelease = currentRelease;

opts.OutputFile = fullfile(outputFolder, compose("matlab-arrow-%s.mltbx", toolboxVersionRaw));
disp("Output File: " + opts.OutputFile);
matlab.addons.toolbox.packageToolbox(opts);

function appendLicenseText(filename)
    licenseText = [ ...
        newline + "--------------------------------------------------------------------------------" + newline
        "3rdparty dependency mathworks/libmexclass is redistributed as a dynamically"
        "linked shared library in certain binary distributions, like the MATLAB"
        "distribution." + newline
        "Copyright: 2022-2024 The MathWorks, Inc. All rights reserved."
        "Homepage: https://github.com/mathworks/libmexclass"
        "License: 3-clause BSD" ];
    writelines(licenseText, filename, WriteMode="append");
end

function appendNoticeText(filename)
    noticeText = [ ...
        newline + "---------------------------------------------------------------------------------" + newline 
        "This product includes software from The MathWorks, Inc. (Apache 2.0)"
        "  * Copyright (C) 2024 The MathWorks, Inc."];
    writelines(noticeText, filename, WriteMode="append");
end
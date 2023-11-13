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

% Copy symlinks to package.
dylib = fullfile(toolboxFolder, "+libmexclass", "+proxy", "*.dylib");
so = fullfile(toolboxFolder, "+libmexclass", "+proxy", "*.so");

% Get the filenames and file paths of all dylibs within +proxy
dylibFiles = dir(dylib);
dylibFileNames = convertCharsToStrings({dylibFiles.name});
dylibFilePaths = fullfile({dylibFiles.folder}, dylibFileNames);

% Get the filenames and file paths of all sos within +proxy
soFiles = dir(so);
soFileNames = convertCharsToStrings({soFiles.name});
soFilePaths = fullfile({soFiles.folder}, soFileNames);

sharedLibFileNames = [dylibFileNames soFileNames];
sharedLibFilePaths = [dylibFilePaths soFilePaths];

% Determine which dylibs and sos were not included in the MLTBX file
[~, name, ext] = fileparts(opts.ToolboxFiles);
idx = ~ismember(sharedLibFileNames, name + ext);
sharedLibrariesToCopy = sharedLibFilePaths(idx);

tmpFolder = fullfile(tempdir, "arrow-matlab");
unzip(opts.OutputFile, tmpFolder);

% Get top-level directories and files
dirContents = dir(tmpFolder);
dotOrDotDotIdx = ismember({dirContents.name}, [".", ".."]);
dirContents(dotOrDotDotIdx) = [];
rootPaths = fullfile(tmpFolder, {dirContents.name});

delete(opts.OutputFile);

% Copy missing shared libraries to the proper subfolder within fsroot 
sharedLibraryTargetFolder = fullfile(tmpFolder, "fsroot", "+libmexclass", "+proxy");
for ii = 1:numel(sharedLibrariesToCopy)
    copyfile(sharedLibrariesToCopy(ii), sharedLibraryTargetFolder);
end

manifestFilename = fullfile(tmpFolder, "metadata", "filesystemManifest.xml"); 
parser = matlab.io.xml.dom.Parser;
manifestXDoc = parser.parseFile(manifestFilename);

fileEntriesNode = manifestXDoc.Children;
date = datetime("now", TimeZone="UTC", Format="yyyy-MM-dd'T'hh:mm:ss");
date = string(date) + "Z";
permissions = "0644";

% Use the first element as a template for the new nodes
firstElement = fileEntriesNode.getFirstElementChild;

for ii = 1:numel(sharedLibrariesToCopy)
    % Clone the first node because of handle copy-semantics
    fileEntryNode = firstElement.cloneNode(false);
    nameAttributeValue = extractAfter(sharedLibrariesToCopy(ii), toolboxFolder);
    contentAttributeValue = fullfile("/fsroot", nameAttributeValue);
    fileEntryNode.setAttribute("name", nameAttributeValue);
    fileEntryNode.setAttribute("content", contentAttributeValue);
    fileEntryNode.setAttribute("date", date);
    fileEntryNode.setAttribute("permissions", permissions);
    fileEntryNode.setAttribute("type", "File");
    fileEntriesNode.appendChild(fileEntryNode);
end

% Export the updated DOM to an XML file
writer = matlab.io.xml.dom.DOMWriter;
writeToFile(writer, manifestXDoc, manifestFilename);

% Re-zip the MLTBX source files and remove ".zip" from the archive filename
zip(opts.OutputFile, rootPaths);
movefile(opts.OutputFile + ".zip", opts.OutputFile);

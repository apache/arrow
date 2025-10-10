# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Free up disk space on Windows GitHub runners

$ErrorActionPreference = 'Continue'

Write-Output "Disk space before cleanup:"
Get-PSDrive C | Select-Object Used, Free, @{Name="UsedGB";Expression={[math]::Round($_.Used/1GB,2)}}, @{Name="FreeGB";Expression={[math]::Round($_.Free/1GB,2)}}

Write-Output "Removing CodeQL (~5.5GB)..."
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue "C:\hostedtoolcache\CodeQL"

Write-Output "Removing Go (~1.5GB)..."
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue "C:\hostedtoolcache\go"

Write-Output "Removing PyPy (~500MB)..."
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue "C:\hostedtoolcache\PyPy"

Write-Output "Removing .NET (~2GB)..."
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue "C:\Program Files\dotnet"

Write-Output "Removing Azure SDKs (~1GB)..."
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue "C:\Program Files (x86)\Microsoft SDKs\Azure"

Write-Output "Removing Android SDK (~10GB)..."
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue "C:\Android"

Write-Output "Removing Haskell (~2GB)..."
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue "C:\Program Files\Haskell"

Write-Output "Removing Ruby (~500MB)..."
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue "C:\hostedtoolcache\Ruby"

Write-Output "Removing PowerShell modules (~500MB)..."
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue "C:\Modules\az_*"
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue "C:\Modules\AWS*"

Write-Output "Removing large packages (~3GB)..."
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue "C:\Program Files (x86)\Google"
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue "C:\Program Files\Google"
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue "C:\Program Files (x86)\Microsoft Visual Studio\Installer"

Write-Output "Disk space after cleanup:"
Get-PSDrive C | Select-Object Used, Free, @{Name="UsedGB";Expression={[math]::Round($_.Used/1GB,2)}}, @{Name="FreeGB";Expression={[math]::Round($_.Free/1GB,2)}}

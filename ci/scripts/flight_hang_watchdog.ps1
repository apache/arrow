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

# Diagnostic for GH-49465. arrow-flight-test passes all 94 tests in ~4s, then
# intermittently hangs on exit and ctest kills it at the 300s timeout. The hang
# only reproduces under the parallel ctest run, not when the binary is run
# standalone, so this watchdog runs in the background during the ctest step and
# minidumps any arrow-flight-test process that outlives the normal run (then
# kills it so ctest moves on), letting us inspect the stuck threads.

param(
  [Parameter(Mandatory = $true)][string]$DumpDir,
  [Parameter(Mandatory = $true)][string]$ProcDump,
  [int]$AgeSeconds = 90,
  [int]$DeadlineMinutes = 30
)

$ErrorActionPreference = 'Continue'
$log = Join-Path $DumpDir 'watchdog.log'
$seen = @{}
$dumped = @{}
$deadline = (Get-Date).AddMinutes($DeadlineMinutes)
"$(Get-Date -Format o) watchdog started (dump arrow-flight-test alive > ${AgeSeconds}s)" | Add-Content $log

while ((Get-Date) -lt $deadline) {
  foreach ($p in @(Get-Process arrow-flight-test -ErrorAction SilentlyContinue)) {
    if (-not $seen.ContainsKey($p.Id)) { $seen[$p.Id] = Get-Date }
    $age = ((Get-Date) - $seen[$p.Id]).TotalSeconds
    if ($age -gt $AgeSeconds -and -not $dumped.ContainsKey($p.Id)) {
      "$(Get-Date -Format o) dumping PID $($p.Id) (age $([int]$age)s)" | Add-Content $log
      & $ProcDump -accepteula -ma $p.Id (Join-Path $DumpDir "flight-hang-$($p.Id).dmp") *>> $log
      $dumped[$p.Id] = $true
      try { Stop-Process -Id $p.Id -Force } catch {}
    }
  }
  Start-Sleep -Seconds 5
}

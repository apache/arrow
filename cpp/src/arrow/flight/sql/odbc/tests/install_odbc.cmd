@REM Licensed to the Apache Software Foundation (ASF) under one
@REM or more contributor license agreements.  See the NOTICE file
@REM distributed with this work for additional information
@REM regarding copyright ownership.  The ASF licenses this file
@REM to you under the Apache License, Version 2.0 (the
@REM "License"); you may not use this file except in compliance
@REM with the License.  You may obtain a copy of the License at
@REM
@REM   http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM Unless required by applicable law or agreed to in writing,
@REM software distributed under the License is distributed on an
@REM "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
@REM KIND, either express or implied.  See the License for the
@REM specific language governing permissions and limitations
@REM under the License.

@echo off

set ODBC_64BIT=%1

@REM enable delayed variable expansion to make environment variables enclosed with "!" to be evaluated 
@REM when the command is executed instead of when the command is parsed
setlocal enableextensions enabledelayedexpansion

if [%ODBC_64BIT%] == [] (
	echo error: 64-bit driver is not specified. Call format: install_odbc abs_path_to_64_bit_driver
	pause
	exit /b 1
)

if exist %ODBC_64BIT% (
	for %%i IN (%ODBC_64BIT%) DO IF EXIST %%~si\NUL (
		echo warning: The path you have specified seems to be a directory. Note that you have to specify path to driver file itself instead.
	)
	echo Installing 64-bit driver: %ODBC_64BIT%
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI\Apache Arrow Flight SQL ODBC Driver" /v DriverODBCVer /t REG_SZ /d "03.80" /f
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI\Apache Arrow Flight SQL ODBC Driver" /v UsageCount /t REG_DWORD /d 00000001 /f
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI\Apache Arrow Flight SQL ODBC Driver" /v Driver /t REG_SZ /d %ODBC_64BIT% /f
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI\Apache Arrow Flight SQL ODBC Driver" /v Setup /t REG_SZ /d %ODBC_64BIT% /f
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers" /v "Apache Arrow Flight SQL ODBC Driver" /t REG_SZ /d "Installed" /f
	
	if !ERRORLEVEL! NEQ 0 (
		echo Error occurred while registering 64-bit driver. Exiting.
		echo ERRORLEVEL: !ERRORLEVEL!
		exit !ERRORLEVEL!
	)
) else (
	echo 64-bit driver can not be found: %ODBC_64BIT%
	echo Call format: install_odbc abs_path_to_64_bit_driver
	pause
	exit /b 1
)

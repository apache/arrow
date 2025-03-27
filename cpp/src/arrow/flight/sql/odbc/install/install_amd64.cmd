@echo off

set ODBC_AMD64=%1

@REM enable delayed variable expansion to make environment variables enclosed with "!" to be evaluated 
@REM when the command is executed instead of when the command is parsed
setlocal enableextensions enabledelayedexpansion

if [%ODBC_AMD64%] == [] (
	echo error: 64-bit driver is not specified. Call format: install_amd64 abs_path_to_64_bit_driver
	pause
	exit /b 1
)

if exist %ODBC_AMD64% (
	for %%i IN (%ODBC_AMD64%) DO IF EXIST %%~si\NUL (
		echo warning: The path you have specified seems to be a directory. Note that you have to specify path to driver file itself instead.
	)
	echo Installing 64-bit driver: %ODBC_AMD64%
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI\Apache Arrow Flight SQL ODBC Driver" /v DriverODBCVer /t REG_SZ /d "03.80" /f
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI\Apache Arrow Flight SQL ODBC Driver" /v UsageCount /t REG_DWORD /d 00000001 /f
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI\Apache Arrow Flight SQL ODBC Driver" /v Driver /t REG_SZ /d %ODBC_AMD64% /f
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI\Apache Arrow Flight SQL ODBC Driver" /v Setup /t REG_SZ /d %ODBC_AMD64% /f
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers" /v "Apache Arrow Flight SQL ODBC Driver" /t REG_SZ /d "Installed" /f
	
	IF !ERRORLEVEL! NEQ 0 (
		echo Error occurred while registering 64-bit driver. Exiting.
		echo ERRORLEVEL: !ERRORLEVEL!
		exit !ERRORLEVEL!
	)
) else (
	echo 64-bit driver can not be found: %ODBC_AMD64%
	echo Call format: install_amd64 abs_path_to_64_bit_driver
	pause
	exit /b 1
)

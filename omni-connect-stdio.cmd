@echo off
rem Configure the JDK, JAR and JVM/Spring options at the top of omni-connect-stdio.ps1.
powershell.exe -NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "%~dp0omni-connect-stdio.ps1"
exit /b %ERRORLEVEL%

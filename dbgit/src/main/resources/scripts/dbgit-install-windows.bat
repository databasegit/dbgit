@echo off

 call :isAdmin

 if %errorlevel% == 0 (
    goto :run
 ) else (
    echo Requesting administrative privileges...
    goto :UACPrompt
 )

 exit /b

 :isAdmin
    fsutil dirty query %systemdrive% >nul
 exit /b

 :run

set INSTALL_PATH=%1

set JAVA_KEY_NAME="HKLM\SOFTWARE\JavaSoft\Java Runtime Environment"
set GIT_KEY_NAME="HKLM\SOFTWARE\GitForWindows"
set VALUE_NAME=CurrentVersion

if defined INSTALL_PATH (
    if "%INSTALL_PATH%"=="help" (
        echo It's installer of Dbgit program
        echo You can choose destination folder as parameter, or current dir will be used
        echo You need to have JRE and Git on your computer. Installer will check if they exist
        echo If you run the installer with -d swith installer will download and install JRE and Git if they doesn't installed

        pause
    )

    echo destination folder is %INSTALL_PATH%
) else (
    set INSTALL_PATH=%~dp0
)

FOR /F "usebackq skip=2 tokens=3" %%A IN (`REG QUERY %JAVA_KEY_NAME% /v %VALUE_NAME% 2^>nul`) DO (
    set ValueValue=%%A
)

if defined ValueValue (
    @echo Found jre %ValueValue%
) else (
    echo JRE not found
    echo Downloading java...

    if EXIST "%ProgramFiles(x86)%" (
        powershell -Command "(New-Object Net.WebClient).DownloadFile('https://javadl.oracle.com/webapps/download/AutoDL?BundleId=236888_42970487e3af4f5aa5bca3f542482c60', '%~dp0\java-install.exe')"
    ) else (
        powershell -Command "(New-Object Net.WebClient).DownloadFile('https://javadl.oracle.com/webapps/download/AutoDL?BundleId=238727_478a62b7d4e34b78b671c754eaaf38ab', '%~dp0\java-install.exe')"
    )

    echo Installing java...
    START /WAIT %~dp0\java-install.exe /s
)
          
FOR /F "usebackq skip=2 tokens=3" %%A IN (`REG QUERY %GIT_KEY_NAME% /v %VALUE_NAME% 2^>nul`) DO (
    set ValueValue=%%A
)

if defined ValueValue (
    @echo Found git %ValueValue%
) else (
    echo Git not found
    echo Downloading git...

    if EXIST "%ProgramFiles(x86)%" (
        Powershell.exe -executionpolicy Bypass -File  %~dp0bin\git-download-x64.ps1 %~dp0git-install.exe
    ) else (
        Powershell.exe -executionpolicy Bypass -File  %~dp0bin\git-download-x86.ps1 %~dp0git-install.exe
    )

    echo Installing git...
    START /WAIT %~dp0git-install.exe /SILENT /COMPONENTS="icons,ext\reg\shellhere,assoc,assoc_sh"
)

if NOT %INSTALL_PATH% == %~dp0 (
    echo Copying files...
    xcopy %~dp0bin %INSTALL_PATH%\dbgit\bin /i /Y
    xcopy %~dp0repo %INSTALL_PATH%\dbgit\repo /i /Y /s

    echo Adding to PATH variable...
    Powershell.exe -executionpolicy Bypass -File  %~dp0bin\path-update.ps1 %INSTALL_PATH%\dbgit
) else (
    echo Adding to PATH variable...
    Powershell.exe -executionpolicy Bypass -File  %~dp0bin\path-update.ps1 %INSTALL_PATH%bin
)


echo Done! Please restart your console

pause 
 exit /b

 :UACPrompt
   echo Set UAC = CreateObject^("Shell.Application"^) > "%temp%\getadmin.vbs"
   echo UAC.ShellExecute "cmd.exe", "/c %~s0 %~1", "", "runas", 1 >> "%temp%\getadmin.vbs"

   "%temp%\getadmin.vbs"
   del "%temp%\getadmin.vbs"
  exit /B`
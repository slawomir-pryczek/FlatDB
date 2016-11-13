@ECHO OFF

ECHO # This file will build golang server and move configuration files
ECHO # to bin directory, under windows.
ECHO #
ECHO #


cd ..\go
SET GOPATH=%CD%

echo Gopath is: %GOPATH%
cd .\src\flatdb

@del flatdb-windows.exe > nul 2> nul
@del ..\..\..\bin\flatdb-windows.exe > nul 2> nul
@del ..\..\..\bin\server-status.html > nul 2> nul
@del ..\..\..\bin\conf.json > nul 2> nul

go build flatdb.go handler_storage.go
rename flatdb.exe flatdb-windows.exe
move flatdb-windows.exe ..\..\..\bin\
echo > ..\..\..\bin\server-status.html
echo > ..\..\..\bin\conf.json
xcopy .\server-status.html ..\..\..\bin\server-status.html /Y
xcopy .\conf.json ..\..\..\bin\conf.json /Y

ECHO # Finished!
PAUSE
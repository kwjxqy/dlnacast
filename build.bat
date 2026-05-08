@echo off
::# 64 Î»£¨Ä¬ÈÏ£©
set GOOS=windows
set GOARCH=amd64
go mod tidy

go build -ldflags="-s -w" -o dlna.exe
dlna
pause
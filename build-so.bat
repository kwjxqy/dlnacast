@echo off
::# 64 位（默认）
set GOOS=windows
set GOARCH=amd64
go mod tidy

go build -ldflags="-s -w" -buildmode=c-shared -o ./windows/dlna.dll ./windows


setlocal
::# 配置 NDK 路径（请根据实际路径修改）
set NDK_PATH=F:/qtandroidsdk/ndk/28.2.13676358
set API_LEVEL=21

::# 目标架构（这里以 arm64 为例）
set ARCH=arm64
set TARGET=aarch64-linux-android

::# 工具链前缀
set TOOLCHAIN=%NDK_PATH%/toolchains/llvm/prebuilt/windows-x86_64
set CC=%TOOLCHAIN%/bin/%TARGET%%API_LEVEL%-clang.cmd

::# 设置环境变量并编译
set CGO_ENABLED=1
set GOOS=android
set GOARCH=arm64
go build -ldflags="-w" -buildmode=c-shared -o ./android/arm64-v8a/libdlna.so ./android

::# 目标架构（这里以 arm64 为例）
set ARCH=arm7
set TARGET=armv7a-linux-androideabi

::# 工具链前缀
set TOOLCHAIN=%NDK_PATH%/toolchains/llvm/prebuilt/windows-x86_64
set CC=%TOOLCHAIN%/bin/%TARGET%%API_LEVEL%-clang.cmd

::# 设置环境变量并编译
set CGO_ENABLED=1
set GOOS=android
set GOARCH=arm
set GOARM=7
go build -ldflags="-w" -buildmode=c-shared -o ./android/armeabi-v7a/libdlna.so ./android
pause
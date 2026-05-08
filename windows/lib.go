// lib.go
package main

/*
#include <stdlib.h>
#include <stdint.h>

// 声明回调类型（若需要异步流式推送，但此处采用同步拉取模型更简单）
*/
import "C"
import (
	"dongdong/dlna/dlnassdp"
)

//export StartServer
func StartServer(name *C.char) {
	server := C.GoString(name)
	dlnassdp.StartServer(server)
}

//export StopServer
func StopServer() {
	dlnassdp.StopServer()
}

func main() {
}

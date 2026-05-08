package main

/*
#include <jni.h>
#include <stdlib.h>

// 辅助函数：从 jstring 获取 UTF-8 字符串（返回 const 指针）
static const char* JstringToCString(JNIEnv* env, jstring jstr) {
    if (jstr == NULL) {
        return NULL;
    }
    return (*env)->GetStringUTFChars(env, jstr, NULL);
}

// 辅助函数：释放 UTF-8 字符串（参数改为 const char*）
static void ReleaseCString(JNIEnv* env, jstring jstr, const char* cstr) {
    if (cstr != NULL) {
        (*env)->ReleaseStringUTFChars(env, jstr, cstr);
    }
}
*/
import "C"
import (
	"dongdong/dlna/dlnassdp"
)

// 用于防止多个请求同时生成同一个文件的竞态条件
//
//export Java_com_dongdong_dlna_Server_StartServer
func Java_com_dongdong_dlna_Server_StartServer(env *C.JNIEnv, obj C.jobject, jname C.jstring) {
	name := C.JstringToCString(env, jname)
	if name == nil {
		return
	}
	defer C.ReleaseCString(env, jname, name)

	server := C.GoString(name)
	dlnassdp.StartServer(server)
}

//export Java_com_dongdong_dlna_Server_StopServer
func Java_com_dongdong_dlna_Server_StopServer(env *C.JNIEnv, obj C.jobject) {
	dlnassdp.StopServer()
}

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

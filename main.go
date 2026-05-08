package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"dongdong/dlna/dlnassdp"
	// 请替换为实际的包路径
)

// ---------- 3. 主函数 ----------
func main() {
	// 创建自定义渲染器

	// 创建 DLNA 协议实例（会自动加载 xml/ 下的服务描述文件）
	dlnassdp.StartServer("Phantom")
	defer dlnassdp.StopServer()

	// 等待退出信号
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	<-c
	log.Println("正在关闭服务...")
}

package dlnassdp

import (
	"dongdong/dlna/utils"
	"fmt"
	"log"
	"net"
	"strings"
	"time"
)

const (
	serviceAddr        = "http://%s:%d/description.xml"
	multicastAddr      = "239.255.255.250:1900" // SSDP 多播地址
	ssdpReadBufferSize = 2048                   // SSDP 读取缓冲区大小
	cacheControlMaxAge = "max-age=1800"         // 缓存控制：最大生存时间
	notifyInterval     = 10 * time.Minute       // 设备通知间隔
)

// ---------- SSDP ----------
func (s *Service) startSSDP() {
	s.devices = []string{
		"upnp:rootdevice",
		s.uuid,
		"urn:schemas-upnp-org:device:MediaRenderer:1",
		"urn:schemas-upnp-org:service:AVTransport:1",
		"urn:schemas-upnp-org:service:ConnectionManager:1",
		"urn:schemas-upnp-org:service:RenderingControl:1",
	}
	// 立即发送 NOTIFY alive
	go s.sendNotifyAlives()
	// 定时 alive
	go func() {
		ticker := time.NewTicker(notifyInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.sendNotifyAlives()
			case <-s.stopChan:
				return
			}
		}
	}()

	// 监听 SSDP M-SEARCH (UDP 1900 多播)
	go s.listenSSDP()
}

// CloseSSDP 安全关闭全局 UDP 连接
func (s *Service) closeSSDP() {
	close(s.stopChan)
	//发送 SSDP byebye 通知（可选但推荐）
	s.sendNotifyByeBye()
	if s.mulConn != nil {
		s.mulConn.Close()
		s.mulConn = nil
	}
}

// sendNotify 发送 SSDP NOTIFY alive 消息
func (s *Service) sendNotifyAlives() {
	conn, err := net.Dial("udp", multicastAddr)
	if err != nil {
		vlog("创建 UDP 连接失败: %v", err)
		return
	}
	defer conn.Close()
	if conn != nil {
		vlog("已发送 SSDP NOTIFY alive 通知")
		localAddr := strings.Split(conn.LocalAddr().String(), ":")[0]
		s.location = fmt.Sprintf(serviceAddr, localAddr, s.Port)
		log.Printf("DLNA 服务启动在 %s \n", s.location)
		for _, nt := range s.devices {
			var usn string
			if nt == s.uuid {
				usn = s.uuid
			} else {
				usn = s.uuid + "::" + nt
			}

			msg := fmt.Sprintf(
				"NOTIFY * HTTP/1.1\r\n"+
					"HOST: 239.255.255.250:1900\r\n"+
					"CACHE-CONTROL: %s\r\n"+
					"LOCATION: %s\r\n"+
					"NT: %s\r\n"+
					"NTS: ssdp:alive\r\n"+
					"SERVER: %s\r\n"+
					"USN: %s\r\n"+
					"\r\n",
				cacheControlMaxAge, s.location, nt, utils.ServerInfo, usn,
			)

			_, err := conn.Write([]byte(msg))
			if err != nil {
				vlog("发送 SSDP NOTIFY alive 失败")
				return
			}
		}
	}
}

// sendNotifyByeBye 发送 SSDP byebye 通知，告知控制点设备已离线
func (s *Service) sendNotifyByeBye() {
	conn, err := net.Dial("udp", multicastAddr)
	if err != nil {
		vlog("创建 UDP 连接失败: %v", err)
		return
	}
	defer conn.Close()
	if conn != nil {
		for _, nt := range s.devices {
			var usn string
			if nt == s.uuid {
				usn = s.uuid
			} else {
				usn = s.uuid + "::" + nt
			}
			msg := fmt.Sprintf(
				"NOTIFY * HTTP/1.1\r\n"+
					"HOST: 239.255.255.250:1900\r\n"+
					"NT: %s\r\n"+
					"NTS: ssdp:byebye\r\n"+
					"USN: %s\r\n"+
					"\r\n",
				nt, usn,
			)
			_, _ = conn.Write([]byte(msg))
		}
		vlog("已发送 SSDP byebye 通知")
	}
}

// listenSSDP 监听 SSDP M-SEARCH 多播请求
func (s *Service) listenSSDP() {
	addr, err := net.ResolveUDPAddr("udp", multicastAddr)
	if err != nil {
		log.Fatal("ResolveUDPAddr 失败:", err)
	}

	conn, err := net.ListenMulticastUDP("udp", nil, addr)
	if err != nil {
		return
	}
	s.mulConn = conn

	vlog("已监听 SSDP 多播 (1900)，等待 M-SEARCH...")

	buf := make([]byte, ssdpReadBufferSize)
	for {
		select {
		case <-s.stopChan:
			vlog("已退出监听 SSDP 多播 (1900)")
			return
		default:
		}
		n, src, err := conn.ReadFromUDP(buf)
		if err != nil {
			vlog("SSDP 读取错误: %v", err)
			continue
		}

		msg := string(buf[:n])
		if !strings.HasPrefix(msg, "M-SEARCH * HTTP/1.1") {
			continue
		}

		st := s.getHeader(msg, "ST")
		man := s.getHeader(msg, "MAN")
		if man != `"ssdp:discover"` {
			continue
		}

		//vlog("收到 M-SEARCH from %s | ST: %s", src, st)

		if len(s.location) > 0 {
			for _, nt := range s.devices {
				if nt == st || st == "ssdp:all" {
					s.sendMSearchResponse(conn, src, nt)
				}
			}
		}
	}

}

// sendMSearchResponse 发送 M-SEARCH 响应
func (s *Service) sendMSearchResponse(conn *net.UDPConn, to *net.UDPAddr, st string) {
	var usn string
	if st == s.uuid {
		usn = s.uuid
	} else {
		usn = s.uuid + "::" + st
	}
	response := fmt.Sprintf(
		"HTTP/1.1 200 OK\r\n"+
			"CACHE-CONTROL: %s\r\n"+
			"EXT:\r\n"+
			"LOCATION: %s\r\n"+
			"SERVER: %s\r\n"+
			"ST: %s\r\n"+
			"USN: %s\r\n"+
			"\r\n",
		cacheControlMaxAge, s.location, utils.ServerInfo, st, usn,
	)

	_, err := conn.WriteToUDP([]byte(response), to)
	if err != nil {
		vlog("发送 M-SEARCH 响应失败: %v", err)
	} else {
		//vlog("已回复 M-SEARCH → %s | ST: %s", to, st)
	}
}

// getHeader 从 HTTP 消息中提取指定头部的值
func (s *Service) getHeader(msg, key string) string {
	lines := strings.Split(msg, "\r\n")
	prefix := strings.ToUpper(key) + ":"
	for _, line := range lines {
		if strings.HasPrefix(strings.ToUpper(line), prefix) {
			return strings.TrimSpace(line[len(key)+1:])
		}
	}
	return ""
}

// vlog 只在 verbose 模式下输出日志
func vlog(format string, v ...interface{}) {
	if true {
		log.Printf(format, v...)
	}
}

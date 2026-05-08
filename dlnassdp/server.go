package dlnassdp

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"regexp"
	"strings"
	"time"

	"dongdong/dlna/protocol"
	"dongdong/dlna/utils"
)

type Service struct {
	myserver *http.Server
	mulConn  *net.UDPConn
	stopChan chan struct{}
	uuid     string
	Port     int
	proto    *protocol.DLNAProtocol
	devices  []string
	location string
}

var server *Service

func StopServer() {
	if server != nil {
		server.stop()
		server = nil
	}
}
func StartServer(name string) {
	if server != nil {
		return
	}
	utils.SetName(name)
	server = &Service{
		stopChan: make(chan struct{}),
		proto:    protocol.GetProtocol(),
		uuid:     utils.GetUUID(),
	}
	server.start()
}

func (s *Service) start() {
	if s.myserver != nil {
		return
	}
	s.proto.Start()
	// 1. 端口
	s.Port = 19000
	listener, err := net.Listen("tcp", ":0")
	if err == nil {
		s.Port = listener.Addr().(*net.TCPAddr).Port
		listener.Close()
	}
	// 设置 HTTP 路由
	http.Handle("/", s)
	s.myserver = &http.Server{Addr: fmt.Sprintf(":%d", s.Port)}
	// 优雅关闭
	go func() {
		if err := s.myserver.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP 服务器错误: %v", err)
		}
	}()
	// 3. SSDP
	s.startSSDP()
}

func (s *Service) stop() {
	// 2. 优雅关闭 HTTP 服务器
	if s.myserver != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := s.myserver.Shutdown(ctx); err != nil {
			log.Printf("HTTP 服务器关闭错误: %v", err)
		} else {
			log.Printf("HTTP 服务器已关闭")
		}
		s.myserver = nil
	}
	s.closeSSDP()
	s.proto.Stop()
}

func (h *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		// SOAP Action 请求
		if r.URL.Path == "/AVTransport/control" ||
			r.URL.Path == "/RenderingControl/control" ||
			r.URL.Path == "/ConnectionManager/control" {
			body, err := io.ReadAll(r.Body)
			defer r.Body.Close()
			if err != nil {
				http.Error(w, "Bad request", http.StatusBadRequest)
				return
			}
			resp, err := h.proto.Call(body)
			if err != nil {
				log.Printf("SOAP 处理错误: %v from %v", err, r)
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "text/xml; charset=utf-8")
			w.Write(resp)
			return
		}
	case "SUBSCRIBE":
		// 事件订阅
		//log.Printf("事件订阅 %s from %s", r.Header, r.Host)
		if r.URL.Path == "/AVTransport/event" ||
			r.URL.Path == "/RenderingControl/event" ||
			r.URL.Path == "/ConnectionManager/event" {
			service := ""
			switch r.URL.Path {
			case "/AVTransport/event":
				service = "AVTransport"
			case "/RenderingControl/event":
				service = "RenderingControl"
			case "/ConnectionManager/event":
				service = "ConnectionManager"
			}
			sid := r.Header.Get("SID")
			callback := r.Header.Get("CALLBACK")
			timeoutStr := r.Header.Get("TIMEOUT")
			timeout := 1800
			if timeoutStr != "" {
				fmt.Sscanf(timeoutStr, "Second-%d", &timeout)
			}

			if sid != "" {
				// 续订
				code := h.proto.RenewSubscribe(sid, timeout)
				if code != 200 {
					http.Error(w, "Precondition Failed", code)
					return
				}
				w.Header().Set("SID", sid)
				w.Header().Set("TIMEOUT", fmt.Sprintf("Second-%d", timeout))
			} else if callback != "" {
				// 新订阅
				re := regexp.MustCompile(`<([^>]+)>`)
				matches := re.FindStringSubmatch(callback)
				if len(matches) < 2 {
					http.Error(w, "Bad Request", 400)
					return
				}
				url := matches[1]
				res := h.proto.AddSubscribe(service, url, timeout)
				w.Header().Set("SID", res["SID"])
				w.Header().Set("TIMEOUT", res["TIMEOUT"])
			} else {
				http.Error(w, "Precondition Failed", 412)
				return
			}
			w.WriteHeader(200)
			return
		}
	case "UNSUBSCRIBE":
		if r.URL.Path == "/AVTransport/event" ||
			r.URL.Path == "/RenderingControl/event" ||
			r.URL.Path == "/ConnectionManager/event" {
			sid := r.Header.Get("SID")
			if sid != "" {
				code := h.proto.RemoveSubscribe(sid)
				if code != 200 {
					http.Error(w, "Precondition Failed", code)
					return
				}
				w.WriteHeader(200)
				return
			}
			http.Error(w, "Precondition Failed", 412)
			return
		}
	case "GET":
		{
			xmlFile := strings.TrimPrefix(r.URL.Path, "/SCPDURL/")
			xmlFile = strings.TrimPrefix(xmlFile, "/")
			xmlData := protocol.LoadXML(xmlFile)
			w.Header().Set("Content-Type", "text/xml; charset=utf-8")
			w.Write([]byte(xmlData))
			return
		}
	default:
		log.Println("Method Not Allowed" + r.URL.Path)
		// GET 请求可返回设备描述等（此处省略）
		http.Error(w, "Method Not Allowed", 405)
	}
}

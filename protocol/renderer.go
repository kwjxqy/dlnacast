package protocol

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// ==================== 配置 ====================
const (
	wsPath = "/ws"
	addr   = ":18080"
)

// 将毫秒转成HH:MM:SS
func formatTimeToUPnP(milliseconds int) string {
	seconds := milliseconds / 1000
	if seconds <= 0 {
		return "00:00:00"
	}
	hrs := int(seconds) / 3600
	mins := (int(seconds) % 3600) / 60
	secs := int(seconds) % 60
	return fmt.Sprintf("%02d:%02d:%02d", hrs, mins, secs)
}

// parseUPnPTime 将 UPnP 时间格式 (HH:MM:SS) 转换为毫秒
func parseUPnPTime(timeStr string) int {
	if len(timeStr) <= 0 {
		return 0
	}
	parts := strings.Split(timeStr, ":")
	if len(parts) != 3 {
		return 0
	}
	hours, _ := strconv.Atoi(parts[0])
	minutes, _ := strconv.Atoi(parts[1])
	seconds, _ := strconv.Atoi(parts[2])
	return (hours*3600 + minutes*60 + seconds) * 1000
}

type Renderer struct {
	proto *DLNAProtocol
	state *ProgressState
}

var myRenderer = &Renderer{state: &ProgressState{Volume: 0, Speed: 1.0}}

func GetRenderer() *Renderer {
	return myRenderer
}

func (c *Renderer) SetState(data []byte) {
	err := json.Unmarshal(data, c.state)
	if err != nil {
		log.Println(err)
		return
	}
	c.proto.SetStateDuration(formatTimeToUPnP(c.state.Duration))

	c.proto.SetStatePosition(formatTimeToUPnP(c.state.Position))

	c.proto.SetStateVolume(c.state.Volume)

	switch c.state.State {
	case 0:
		c.proto.SetStatePause()
	case 1:
		c.proto.SetStatePlay()
	case 2:
		c.proto.SetStateStop()
	}

	c.proto.SetStateMute(c.state.Mute)
}

func (c *Renderer) GetMediaVolume() int {
	return c.state.Volume
}

func (c *Renderer) SetMediaVolume(volume string) {
	vol, _ := strconv.Atoi(volume)
	BroadcastControl("set_volume", 0, vol)
}
func (c *Renderer) SetMediaMute(mute bool) {}
func (c *Renderer) SetMediaURL(url, mediaType string) {
	BroadcastPlay(url, mediaType)
}
func (c *Renderer) SetMediaTitle(title string) {

}
func (c *Renderer) SetMediaResume() {
	BroadcastControl("play", 0, 100)
}
func (c *Renderer) SetMediaPause() {
	BroadcastControl("pause", 0, 0)
}
func (c *Renderer) SetMediaStop() {
	BroadcastControl("stop", 0, 0)
}
func (c *Renderer) SetMediaPosition(position string) {
	BroadcastControl("seek", parseUPnPTime(position), 0)
}

func (c *Renderer) Stop() {
	StopControl()
}

func (c *Renderer) Start(proto *DLNAProtocol) {
	c.proto = proto
	StartControl()
}

// ==================== 全局进度状态 ====================

// ProgressState 保存最新的播放进度信息
type ProgressState struct {
	Position int     `json:"position"` // 当前播放位置（毫秒）
	Duration int     `json:"duration"` // 视频总时长（毫秒）
	State    int     `json:"state"`    // 是否正在播放
	Volume   int     `json:"volume"`
	Speed    float64 `json:"speed"`
	Mute     bool    `json:"mute"`
}

var (
	controlServer *http.Server
)

// ==================== 数据结构 ====================

type WSMessage struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type PlayPayload struct {
	URL       string `json:"url"`
	MediaType string `json:"type"`
}

// 控制指令 Payload（支持 play, pause, seek, set_volume, stop）
type ControlPayload struct {
	Command  string `json:"command"`
	Position int    `json:"position"`
	Volume   int    `json:"volume"`
}

type RegisterPayload struct {
	SessionID string `json:"session_id"`
	DeviceID  string `json:"device_id"`
}

// ==================== 客户端连接管理 ====================

type Client struct {
	conn      *websocket.Conn
	sessionID string
	deviceID  string
	mu        sync.Mutex
}

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	clients          = make(map[*Client]bool)
	clientsMu        sync.RWMutex
	sessionClients   = make(map[string]*Client)
	sessionClientsMu sync.RWMutex
)

// ==================== WebSocket 处理 ====================

func wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket 升级失败: %v", err)
		return
	}

	client := &Client{conn: conn}

	clientsMu.Lock()
	clients[client] = true
	clientsMu.Unlock()

	log.Printf("新客户端连接: %s,客户端数量: %d", conn.RemoteAddr(), len(clients))

	defer func() {
		clientsMu.Lock()
		delete(clients, client)
		clientsMu.Unlock()

		if client.sessionID != "" {
			sessionClientsMu.Lock()
			delete(sessionClients, client.sessionID)
			sessionClientsMu.Unlock()
		}
		conn.Close()
		log.Printf("客户端断开: %s", conn.RemoteAddr())
	}()

	handleClientMessages(client)
}

func handleClientMessages(client *Client) {
	for {
		var msg WSMessage
		err := client.conn.ReadJSON(&msg)
		if err != nil {
			log.Printf("读取客户端消息失败: %v", err)
			return
		}

		switch msg.Type {
		case "register":
			var payload RegisterPayload
			if err := json.Unmarshal(msg.Payload, &payload); err != nil {
				log.Printf("解析注册消息失败: %v", err)
				continue
			}
			client.sessionID = payload.SessionID
			client.deviceID = payload.DeviceID

			sessionClientsMu.Lock()
			sessionClients[payload.SessionID] = client
			sessionClientsMu.Unlock()
			log.Printf("客户端注册: sessionID=%s, deviceID=%s", payload.SessionID, payload.DeviceID)
		// 如果需要，可以在这里更新一个全局的音量状态
		case "report_status":
			myRenderer.SetState(msg.Payload)
		default:
			log.Printf("收到未知消息类型: %s", msg.Type)
		}
	}
}

// ==================== 向客户端发送指令 ====================

// 通用发送函数（避免重复代码）
func sendWSMessage(client *Client, msg *WSMessage) error {
	client.mu.Lock()
	defer client.mu.Unlock()
	return client.conn.WriteJSON(msg)
}

// BroadcastPlay 向所有在线客户端广播播放指令
func BroadcastPlay(videoURL string, mediaType string) {
	clientsMu.RLock()
	defer clientsMu.RUnlock()

	payload, err := json.Marshal(PlayPayload{URL: videoURL, MediaType: mediaType})
	if err != nil {
		return
	}

	msg := &WSMessage{
		Type:    "play",
		Payload: payload,
	}
	count := 0
	for client := range clients {
		if err := sendWSMessage(client, msg); err != nil {
			log.Printf("发送播放指令失败: %v", err)
			continue
		}
		count++
	}
	log.Printf("已发送播放指令，数量: %d", count)
}

// BroadcastControl 向所有在线客户端广播控制指令
func BroadcastControl(command string, position int, volume int) {
	clientsMu.RLock()
	defer clientsMu.RUnlock()

	payload, err := json.Marshal(ControlPayload{
		Command:  command,
		Position: position,
		Volume:   volume,
	})
	if err != nil {
		return
	}

	msg := &WSMessage{
		Type:    "control",
		Payload: payload,
	}

	for client := range clients {
		if err := sendWSMessage(client, msg); err != nil {
			log.Printf("发送控制指令失败: %v", err)
		}
	}
	log.Printf("已发送控制指令: command=%s, position=%d, volume=%d", command, position, volume)
}

func listClientsHandler(w http.ResponseWriter, r *http.Request) {
	sessionClientsMu.RLock()
	defer sessionClientsMu.RUnlock()

	type ClientInfo struct {
		SessionID  string `json:"session_id"`
		DeviceID   string `json:"device_id"`
		RemoteAddr string `json:"remote_addr"`
	}

	infos := []ClientInfo{}
	for sid, client := range sessionClients {
		infos = append(infos, ClientInfo{
			SessionID:  sid,
			DeviceID:   client.deviceID,
			RemoteAddr: client.conn.RemoteAddr().String(),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(infos)
}

// ==================== 主函数 ====================

func StartControl() {
	if controlServer != nil {
		return
	}
	http.HandleFunc(wsPath, wsHandler)
	http.HandleFunc("/clients", listClientsHandler)

	controlServer = &http.Server{Addr: addr}
	go func() {
		log.Printf("控制服务器启动在 %s", addr)
		if err := controlServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("控制服务器错误: %v", err)
		}
	}()
}

func StopControl() {
	log.Println("正在停止控制服务器...")

	// 1. 关闭所有 WebSocket 客户端连接
	closeAllWebSocketConnections()

	// 2. 优雅关闭 HTTP 服务器
	if controlServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := controlServer.Shutdown(ctx); err != nil {
			log.Printf("控制服务器关闭错误: %v", err)
		} else {
			log.Println("控制服务器已关闭")
		}
	}
	controlServer = nil
}

// closeAllWebSocketConnections 遍历并关闭所有活跃的 WebSocket 连接
func closeAllWebSocketConnections() {
	clientsMu.Lock()
	defer clientsMu.Unlock()

	for client := range clients {
		// 发送关闭帧（可选，但直接关闭连接即可）
		client.conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseGoingAway, "server shutdown"),
			time.Now().Add(time.Second))
		client.conn.Close()
	}
	// 清空 maps
	clients = make(map[*Client]bool)
	sessionClients = make(map[string]*Client)
	log.Printf("已关闭所有 WebSocket 连接")
}

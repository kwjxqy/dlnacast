package protocol

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"dongdong/dlna/utils"

	"github.com/beevik/etree"
)

// ---------- 日志 ----------
var logger = log.New(os.Stdout, "[DLNA] ", log.LstdFlags)

// ---------- DLNA 基础类型 ----------
type DataType string

const (
	Boolean DataType = "boolean"
	I2      DataType = "i2"
	UI2     DataType = "ui2"
	I4      DataType = "i4"
	UI4     DataType = "ui4"
	String  DataType = "string"
)

type StateVariable struct {
	Name             string
	SendEvents       bool
	DataType         DataType
	Minimum          *int
	Maximum          *int
	AllowedValueList []string
	Value            string // 统一用 string 存储，实际使用根据需要转换
	Service          string
}

func (sv *StateVariable) SetValue(value string) {
	sv.Value = value
}

type Argument struct {
	Name  string
	State string
	Value string
}

type Action struct {
	Name   string
	Input  []*Argument
	Output []*Argument
}

type Service struct {
	Name      string
	Namespace string
	Actions   map[string]*Action
}

var serviceMap = map[string]*Service{}

func GetService(name string) *Service {
	if s, ok := serviceMap[name]; ok {
		return s
	}
	// 避免空指针
	svc := &Service{Name: name}
	serviceMap[name] = svc
	return svc
}

// ---------- ObserveClient ----------
type ObserveClient struct {
	URL       string
	Service   string
	StartTime int64
	SID       string
	Timeout   int
	Seq       int
	Host      string
	Path      string
	Error     int
	mu        sync.Mutex
}

func randomHex(n int) (string, error) {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("failed to generate random bytes: %w", err)
	}
	return hex.EncodeToString(bytes)[:n], nil
}

// generateSID 生成唯一的订阅 ID
func generateSID() string {
	hexStr, _ := randomHex(8)
	return fmt.Sprintf("uuid:%s", strings.ReplaceAll(strings.ToLower(utils.GetUUID()), "uuid:", "")+"-"+hexStr)
}

func NewObserveClient(service, url string, timeout int) *ObserveClient {
	reHost := regexp.MustCompile(`//([0-9:.]*)`)
	rePath := regexp.MustCompile(`//[0-9:.]*(.*)$`)
	hostMatch := reHost.FindStringSubmatch(url)
	pathMatch := rePath.FindStringSubmatch(url)
	host := ""
	path := ""
	if len(hostMatch) > 1 {
		host = hostMatch[1]
	}
	if len(pathMatch) > 1 {
		path = pathMatch[1]
	}
	return &ObserveClient{
		URL:       url,
		Service:   service,
		StartTime: time.Now().Unix(),
		SID:       generateSID(),
		Timeout:   timeout,
		Seq:       0,
		Host:      host,
		Path:      path,
		Error:     0,
	}
}

func (oc *ObserveClient) IsTimeout() bool {
	return time.Now().Unix()-oc.StartTime > int64(oc.Timeout)
}

func (oc *ObserveClient) Update(timeout int) {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	oc.StartTime = time.Now().Unix()
	oc.Timeout = timeout
}

// SendEventCallback 发送 NOTIFY 请求到客户端
func (oc *ObserveClient) SendEventCallback(data map[string]string) error {
	// 构建 XML
	namespace := "urn:schemas-upnp-org:event-1-0"
	doc := etree.NewDocument()
	propertyset := doc.CreateElement("e:propertyset")
	propertyset.CreateAttr("xmlns:e", namespace)

	if oc.Service == "ConnectionManager" {
		for key, val := range data {
			prop := propertyset.CreateElement("e:property")
			item := prop.CreateElement(key)
			item.SetText(val)
		}
	} else {
		prop := propertyset.CreateElement("e:property")
		lastChange := prop.CreateElement("LastChange")
		eventElem := etree.NewElement("Event")
		eventElem.CreateAttr("xmlns", "urn:schemas-upnp-org:metadata-1-0/AVT/")
		instanceID := eventElem.CreateElement("InstanceID")
		instanceID.CreateAttr("val", "0")
		for key, val := range data {
			p := instanceID.CreateElement(key)
			p.CreateAttr("val", val)
		}
		// 将 Event 元素序列化为字符串填入 LastChange
		buf := &bytes.Buffer{}
		eventDoc := etree.NewDocument()
		eventDoc.SetRoot(eventElem)
		eventDoc.WriteTo(buf)
		lastChange.SetText(buf.String())
	}

	// 构建 HTTP NOTIFY 请求
	bodyBytes, err := doc.WriteToBytes()
	if err != nil {
		return err
	}
	req, err := http.NewRequest("NOTIFY", "http://"+oc.Host+oc.Path, bytes.NewReader(bodyBytes))
	if err != nil {
		return err
	}
	//logger.Printf("notify:%s\n", string(bodyBytes))
	req.Header.Set("NT", "upnp:event")
	req.Header.Set("NTS", "upnp:propchange")
	req.Header.Set("Content-Type", `text/xml; charset="utf-8"`)
	req.Header.Set("Server", utils.ServerInfo)
	req.Header.Set("SID", oc.SID)
	req.Header.Set("SEQ", fmt.Sprintf("%d", oc.Seq))
	req.Header.Set("TIMEOUT", fmt.Sprintf("Second-%d", oc.Timeout))

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// 忽略响应体
	oc.mu.Lock()
	oc.Seq++
	oc.mu.Unlock()
	return nil
}

// 状态观察列表
var serviceStateObserved = map[string][]string{
	"AVTransport": {
		"TransportState", "TransportStatus",
		"CurrentMediaDuration", "CurrentTrackDuration",
		"CurrentTrack", "NumberOfTracks",
	},
	"RenderingControl":  {"Volume", "Mute"},
	"ConnectionManager": {"A_ARG_TYPE_Direction", "SinkProtocolInfo", "CurrentConnectionIDs"},
}

// ---------- DLNAProtocol 核心结构体 ----------
type DLNAProtocol struct {
	running       bool
	stateList     map[string]*StateVariable
	stopCh        chan struct{} // 用于停止事件循环
	subscriptions map[string]*ObserveClient
	subMu         sync.RWMutex
	stateQueue    chan [2]string // 待发送的状态变化：name, value
	renderer      *Renderer
}

var myProtocol = &DLNAProtocol{
	stateList:     make(map[string]*StateVariable),
	subscriptions: make(map[string]*ObserveClient),
}

func GetProtocol() *DLNAProtocol {
	return myProtocol
}

func (p *DLNAProtocol) loadServices() {
	// 假设 XML 文件位于 ./xml/AVTransport.xml 等
	serviceFiles := map[string]string{
		"AVTransport":       "AVTransport.xml",
		"RenderingControl":  "RenderingControl.xml",
		"ConnectionManager": "ConnectionManager.xml",
	}
	for name, file := range serviceFiles {
		xmlData := LoadXML(file)
		doc := etree.NewDocument()
		if err := doc.ReadFromString(xmlData); err != nil {
			logger.Fatalf("Failed to parse %s: %v", file, err)
		}
		p.buildServiceFromDoc(name, doc.Root())
	}
}

// 按本地名递归查找所有后代元素（忽略命名空间前缀与 URI）
func findAllElements(root *etree.Element, localName string) []*etree.Element {
	var result []*etree.Element
	for _, child := range root.ChildElements() {
		if getLocalName(child.Tag) == localName {
			result = append(result, child)
		}
		result = append(result, findAllElements(child, localName)...)
	}
	return result
}

// 提取标签的本地名（去掉 {uri} 或 前缀:）
func getLocalName(tag string) string {
	if idx := strings.LastIndex(tag, "}"); idx != -1 {
		return tag[idx+1:]
	}
	if idx := strings.LastIndex(tag, ":"); idx != -1 {
		return tag[idx+1:]
	}
	return tag
}

// 在 elem 的直接子元素中按本地名查找第一个匹配的子元素
func findChildByLocal(elem *etree.Element, localName string) *etree.Element {
	for _, child := range elem.ChildElements() {
		if getLocalName(child.Tag) == localName {
			return child
		}
	}
	return nil
}

func (p *DLNAProtocol) buildServiceFromDoc(serviceName string, root *etree.Element) {
	namespaceURI := fmt.Sprintf("urn:schemas-upnp-org:service:%s:1", serviceName)

	// 1. 解析所有 <stateVariable> 元素
	for _, svElem := range findAllElements(root, "stateVariable") {
		nameElem := findChildByLocal(svElem, "name")
		if nameElem == nil {
			continue
		}
		name := nameElem.Text()
		sendEvents := false
		if attr := svElem.SelectAttr("sendEvents"); attr != nil && attr.Value == "yes" {
			sendEvents = true
		}
		dataTypeElem := findChildByLocal(svElem, "dataType")
		dataType := String
		if dataTypeElem != nil {
			dataType = DataType(dataTypeElem.Text())
		}
		sv := &StateVariable{
			Name:       name,
			SendEvents: sendEvents,
			DataType:   dataType,
			Service:    serviceName,
		}
		switch dataType {
		case String:
			sv.Value = ""
		case Boolean:
			sv.Value = "false"
		default: // i1, i2, i4, ui1, ui2, ui4 等数值类型
			sv.Value = "0"
		}
		// 默认值
		if def := findChildByLocal(svElem, "defaultValue"); def != nil {
			sv.SetValue(def.Text())
		}
		// allowedValueList
		if list := findChildByLocal(svElem, "allowedValueList"); list != nil {
			values := []string{}
			for _, v := range list.ChildElements() {
				if getLocalName(v.Tag) == "allowedValue" {
					values = append(values, v.Text())
				}
			}
			sv.AllowedValueList = values
			for _, v := range values {
				if v == "NOT_IMPLEMENTED" {
					sv.Value = "NOT_IMPLEMENTED"
					break
				}
			}
		}
		// allowedValueRange
		if rng := findChildByLocal(svElem, "allowedValueRange"); rng != nil {
			min, max := 0, 0
			if m := findChildByLocal(rng, "minimum"); m != nil {
				fmt.Sscanf(m.Text(), "%d", &min)
			}
			if m := findChildByLocal(rng, "maximum"); m != nil {
				fmt.Sscanf(m.Text(), "%d", &max)
			}
			sv.Minimum = &min
			sv.Maximum = &max
		}
		p.stateList[sv.Name] = sv
	}

	// 2. 解析所有 <action> 元素
	actions := make(map[string]*Action)
	for _, actElem := range findAllElements(root, "action") {
		nameElem := findChildByLocal(actElem, "name")
		if nameElem == nil {
			continue
		}
		name := nameElem.Text()
		action := &Action{Name: name}
		argList := findChildByLocal(actElem, "argumentList")
		if argList != nil {
			for _, arg := range argList.ChildElements() {
				if getLocalName(arg.Tag) != "argument" {
					continue
				}
				argNameElem := findChildByLocal(arg, "name")
				relStateElem := findChildByLocal(arg, "relatedStateVariable")
				dirElem := findChildByLocal(arg, "direction")
				if argNameElem == nil || relStateElem == nil || dirElem == nil {
					continue
				}
				a := &Argument{
					Name:  argNameElem.Text(),
					State: relStateElem.Text(),
				}
				if dirElem.Text() == "in" {
					action.Input = append(action.Input, a)
				} else {
					action.Output = append(action.Output, a)
				}
			}
		}
		actions[name] = action
	}

	serviceMap[serviceName] = &Service{
		Name:      serviceName,
		Namespace: namespaceURI,
		Actions:   actions,
	}

	// 调试输出
	//logger.Printf("✅ %s: 状态变量 %d, 动作 %d", serviceName, len(p.stateList), len(actions))
}

func (p *DLNAProtocol) initDefaultStates() {
	p.SetState("CurrentPlayMode", "NORMAL")
	p.SetState("TransportPlaySpeed", "1")
	p.SetState("TransportStatus", "OK")
	p.SetState("RelativeCounterPosition", "2147483647")
	p.SetState("AbsoluteCounterPosition", "2147483647")
	p.SetState("CurrentTrackDuration", "00:00:00")
	p.SetState("CurrentMediaDuration", "00:00:00")
	p.SetState("RelativeTimePosition", "00:00:00")
	p.SetState("AbsoluteTimePosition", "00:00:00")
	p.SetState("A_ARG_TYPE_Direction", "Output")
	p.SetState("CurrentConnectionIDs", "0")
	p.SetState("A_ARG_TYPE_ConnectionStatus", "OK")
	p.SetState("PlaybackStorageMedium", "None")
	p.SetState("Volume", fmt.Sprintf("%d", p.renderer.GetMediaVolume()))
	p.SetState("SinkProtocolInfo", LoadXML("SinkProtocolInfo.csv"))
}

// ---------- 事件线程与订阅管理 ----------
func (p *DLNAProtocol) Start() {
	p.subMu.Lock()
	defer p.subMu.Unlock()
	if p.running {
		return
	}
	p.running = true
	p.stateQueue = make(chan [2]string, 256)
	p.stopCh = make(chan struct{})
	p.renderer = GetRenderer()
	p.renderer.Start(p)
	p.loadServices()
	p.initDefaultStates()

	go p.eventLoop()
	p.SetState("TransportState", "STOPPED")
	p.SetState("TransportStatus", "OK")
}

func (p *DLNAProtocol) Stop() {
	p.subMu.Lock()
	defer p.subMu.Unlock()
	if !p.running {
		return
	}
	p.running = false
	close(p.stateQueue)
	close(p.stopCh)
	p.renderer.Stop()
}

func (p *DLNAProtocol) eventLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			// 收集本秒内累积的状态变化
			changes := make(map[string]string)
		drain:
			for {
				select {
				case kv := <-p.stateQueue:
					changes[kv[0]] = kv[1]
				default:
					break drain
				}
			}
			if len(changes) > 0 {
				p.sendStatesToClients(changes)
			}
		}
	}
}

func (p *DLNAProtocol) AddSubscribe(service, url string, timeout int) map[string]string {
	logger.Printf("SUBSCRIBE %s from %s", service, url)
	p.subMu.Lock()
	defer p.subMu.Unlock()

	// 检查是否已存在
	for _, c := range p.subscriptions {
		if c.URL == url && c.Service == service {
			c.Update(timeout)
			logger.Println("SUBSCRIBE updated")
			return map[string]string{
				"SID":     c.SID,
				"TIMEOUT": fmt.Sprintf("Second-%d", c.Timeout),
			}
		}
	}

	client := NewObserveClient(service, url, timeout)
	// 将客户端加入待处理队列
	p.subscriptions[client.SID] = client
	// 异步发送初始事件
	go p.sendInitEvent(service, client)
	return map[string]string{
		"SID":     client.SID,
		"TIMEOUT": fmt.Sprintf("Second-%d", client.Timeout),
	}
}

func (p *DLNAProtocol) sendInitEvent(service string, client *ObserveClient) {
	data := make(map[string]string)
	for _, stateName := range serviceStateObserved[service] {
		if sv, ok := p.stateList[stateName]; ok {
			data[stateName] = sv.Value
		}
	}
	if err := client.SendEventCallback(data); err != nil {
		logger.Printf("Init event error for %s: %v", client.SID, err)
	}
}

func (p *DLNAProtocol) RemoveSubscribe(sid string) int {
	p.subMu.Lock()
	defer p.subMu.Unlock()
	if _, ok := p.subscriptions[sid]; ok {
		// 通过队列异步删除，避免死锁
		delete(p.subscriptions, sid)
		logger.Printf("Removed client %s ", sid)
		return http.StatusOK
	}
	return http.StatusPreconditionFailed
}

func (p *DLNAProtocol) RenewSubscribe(sid string, timeout int) int {
	p.subMu.Lock()
	defer p.subMu.Unlock()
	if c, ok := p.subscriptions[sid]; ok {
		c.Update(timeout)
		return http.StatusOK
	}
	return http.StatusPreconditionFailed
}

// sendStatesToClients 处理所有待处理的增删，并向所有有效客户端发送状态变化
func (p *DLNAProtocol) sendStatesToClients(changes map[string]string) {
	// 1. 先处理新增/移除队列（一次性处理完，不加锁太久）
	p.subMu.Lock()
	// 复制当前客户端列表，避免长时间持有锁
	clients := make([]*ObserveClient, 0, len(p.subscriptions))
	for _, c := range p.subscriptions {
		clients = append(clients, c)
	}
	p.subMu.Unlock()

	// 2. 向每个客户端发送变化
	for _, client := range clients {
		if client.IsTimeout() {
			// 异步移除，避免死锁（RemoveSubscribe 会拿锁，但我们在锁外调用安全）
			go p.RemoveSubscribe(client.SID)
			continue
		}
		// 筛选属于同一服务的状态
		state := make(map[string]string)
		for name, val := range changes {
			if sv, ok := p.stateList[name]; ok && sv.Service == client.Service {
				state[name] = val
			}
		}
		if len(state) == 0 {
			continue
		}
		if err := client.SendEventCallback(state); err != nil {
			logger.Printf("Send event error to %s: %v", client.SID, err)
			client.Error++
			if client.Error > 10 {
				logger.Printf("Removing client %s due to too many errors", client.SID)
				go p.RemoveSubscribe(client.SID)
			}
		}
	}
}

// ---------- 状态存取 ----------
func (p *DLNAProtocol) SetState(name, value string) {
	sv, ok := p.stateList[name]
	if !ok {
		// 某些状态可能未在 XML 中定义，忽略
		return
	}
	// 判断是否需要加入事件队列
	inAVT := false
	inRC := false
	for _, s := range serviceStateObserved["AVTransport"] {
		if s == name {
			inAVT = true
			break
		}
	}
	for _, s := range serviceStateObserved["RenderingControl"] {
		if s == name {
			inRC = true
			break
		}
	}
	if inAVT || inRC {
		if sv.Value != value {
			logger.Printf("setState %s = %s", name, value)
			select {
			case p.stateQueue <- [2]string{name, value}:
			default:
				logger.Println("stateQueue is full, dropping")
			}
		}

	}
	// 只有值变更时才更新
	if sv.Value != value {
		sv.SetValue(value)
	}
}

func (p *DLNAProtocol) GetState(name string) string {
	if sv, ok := p.stateList[name]; ok {
		return sv.Value
	}
	return ""
}

// ---------- DLNA 动作处理方法 ----------
func (p *DLNAProtocol) RenderingControl_SetVolume(data map[string]*Argument) map[string]string {
	vol := data["DesiredVolume"].Value
	if p.renderer != nil {
		p.renderer.SetMediaVolume(vol)
	}
	return map[string]string{}
}

func (p *DLNAProtocol) RenderingControl_SetMute(data map[string]*Argument) map[string]string {
	muteStr := data["DesiredMute"].Value
	mute := muteStr != "0" && muteStr != "false"
	if p.renderer != nil {
		p.renderer.SetMediaMute(mute)
	}
	return map[string]string{}
}

func extractMediaTypes(s string) []string {
	re := regexp.MustCompile(`http-get:\*:([^:/]+)/[^:]+`)
	matches := re.FindAllStringSubmatch(s, -1)
	seen := map[string]bool{}
	var res []string
	for _, m := range matches {
		mt := m[1] // 捕获组内容，如 "video/mpeg"
		if !seen[mt] {
			seen[mt] = true
			res = append(res, mt)
		}
	}
	return res
}

func (p *DLNAProtocol) AVTransport_SetAVTransportURI(data map[string]*Argument) map[string]string {
	uri := data["CurrentURI"].Value

	p.SetState("CurrentTrackURI", uri)
	title := utils.GetName()
	mediaType := "video"
	if meta, ok := data["CurrentURIMetaData"]; ok && meta.Value != "" {
		//logger.Printf("CurrentURIMetaData: %s", meta.Value)
		metaDoc := etree.NewDocument()
		if err := metaDoc.ReadFromString(meta.Value); err == nil {
			// 尝试从 DIDL-Lite 中提取 dc:title
			for _, t := range metaDoc.FindElements("//dc:title") {
				title = t.Text()
				break
			}
		}
		p.SetState("CurrentTrackMetaData", meta.Value)
		media := extractMediaTypes(meta.Value)
		if len(media) > 0 {
			mediaType = media[0]
		}
	} else {
		p.SetState("CurrentTrackMetaData", data["CurrentURIMetaData"].Value)
	}
	logger.Printf("MediaType:%s", mediaType)
	if p.renderer != nil {
		p.renderer.SetMediaURL(uri, mediaType)
		p.renderer.SetMediaTitle(title)
		p.renderer.SetMediaResume()
	}
	p.SetState("CurrentTrackTitle", title)
	p.SetState("RelativeTimePosition", "00:00:00")
	p.SetState("AbsoluteTimePosition", "00:00:00")
	p.SetState("TransportState", "PLAYING")
	p.SetState("TransportStatus", "OK")
	return map[string]string{}
}

func (p *DLNAProtocol) AVTransport_Play(data map[string]*Argument) map[string]string {
	if p.renderer != nil {
		p.renderer.SetMediaResume()
	}
	p.SetState("TransportState", "PLAYING")
	p.SetState("TransportStatus", "OK")
	return map[string]string{}
}

func (p *DLNAProtocol) AVTransport_Pause(data map[string]*Argument) map[string]string {
	if p.renderer != nil {
		p.renderer.SetMediaPause()
	}
	p.SetState("TransportState", "PAUSED_PLAYBACK")
	return map[string]string{}
}

func (p *DLNAProtocol) AVTransport_Seek(data map[string]*Argument) map[string]string {
	target := data["Target"].Value
	if p.renderer != nil {
		p.renderer.SetMediaPosition(target)
	}
	p.SetState("RelativeTimePosition", target)
	p.SetState("AbsoluteTimePosition", target)
	return map[string]string{}
}

func (p *DLNAProtocol) AVTransport_Stop(data map[string]*Argument) map[string]string {
	if p.renderer != nil {
		p.renderer.SetMediaStop()
	}
	p.SetState("TransportState", "STOPPED")
	return map[string]string{}
}

// ---------- 状态更新便捷方法（由播放器回调）----------
func (p *DLNAProtocol) SetStatePosition(pos string) {
	p.SetState("RelativeTimePosition", pos)
	p.SetState("AbsoluteTimePosition", pos)
}
func (p *DLNAProtocol) SetStateDuration(dur string) {
	p.SetState("CurrentTrackDuration", dur)
	p.SetState("CurrentMediaDuration", dur)
}
func (p *DLNAProtocol) SetStatePlay()  { p.SetState("TransportState", "PLAYING") }
func (p *DLNAProtocol) SetStatePause() { p.SetState("TransportState", "PAUSED_PLAYBACK") }
func (p *DLNAProtocol) SetStateStop()  { p.SetState("TransportState", "STOPPED") }
func (p *DLNAProtocol) SetStateEOF()   { p.SetState("TransportState", "NO_MEDIA_PRESENT") }
func (p *DLNAProtocol) SetStateTransportError() {
	p.SetState("TransportState", "STOPPED")
	p.SetState("TransportStatus", "ERROR_OCCURRED")
}
func (p *DLNAProtocol) SetStateMute(mute bool) {
	p.SetState("Mute", fmt.Sprintf("%v", mute))
}
func (p *DLNAProtocol) SetStateVolume(vol int) {
	p.SetState("Volume", fmt.Sprintf("%d", vol))
}
func (p *DLNAProtocol) SetStateSpeed(speed string) {
	p.SetState("TransportPlaySpeed", speed)
}
func (p *DLNAProtocol) SetStateDisplaySubtitle(show bool) {
	p.SetState("DisplayCurrentSubtitle", fmt.Sprintf("%v", show))
}
func (p *DLNAProtocol) SetStateURL(url string) { p.SetState("CurrentTrackURI", url) }

// ---------- 动作分发与 SOAP 处理 ----------
// 方法映射表（可动态注册，此处静态列出）
var implementedMethods = map[string]bool{
	"AVTransport_SetAVTransportURI": true,
	"AVTransport_Play":              true,
	"AVTransport_Pause":             true,
	"AVTransport_Seek":              true,
	"AVTransport_Stop":              true,
	"RenderingControl_SetVolume":    true,
	"RenderingControl_SetMute":      true,
}

func (p *DLNAProtocol) hasMethod(name string) bool {
	return implementedMethods[name]
}

func (p *DLNAProtocol) callMethod(name string, data map[string]*Argument) (map[string]string, error) {
	switch name {
	case "AVTransport_SetAVTransportURI":
		return p.AVTransport_SetAVTransportURI(data), nil
	case "AVTransport_Play":
		return p.AVTransport_Play(data), nil
	case "AVTransport_Pause":
		return p.AVTransport_Pause(data), nil
	case "AVTransport_Seek":
		return p.AVTransport_Seek(data), nil
	case "AVTransport_Stop":
		return p.AVTransport_Stop(data), nil
	case "RenderingControl_SetVolume":
		return p.RenderingControl_SetVolume(data), nil
	case "RenderingControl_SetMute":
		return p.RenderingControl_SetMute(data), nil
	default:
		return nil, fmt.Errorf("no handler for method %s", name)
	}
}

// Call 处理 SOAP Action 请求
func (p *DLNAProtocol) Call(rawBody []byte) ([]byte, error) {
	doc := etree.NewDocument()
	if err := doc.ReadFromBytes(rawBody); err != nil {
		return nil, fmt.Errorf("invalid XML: %w", err)
	}

	// 查找 SOAP Body，兼容任何命名空间前缀
	var body *etree.Element
	for _, elem := range doc.FindElements("//*") {
		if elem.NamespaceURI() == "http://schemas.xmlsoap.org/soap/envelope/" &&
			getLocalName(elem.Tag) == "Body" {
			body = elem
			break
		}
	}
	if body == nil {
		return nil, fmt.Errorf("missing SOAP Body:%s", string(rawBody))
	}
	var actionElem *etree.Element
	if len(body.ChildElements()) > 0 {
		actionElem = body.ChildElements()[0]
	}
	if actionElem == nil {
		return nil, fmt.Errorf("empty SOAP Body")
	}

	// 收集参数
	param := make(map[string]string)
	for _, child := range actionElem.ChildElements() {
		param[child.Tag] = child.Text()
	}

	// 提取 action 名称和服务名
	fullTag := actionElem.Tag // 例如：{urn:schemas-upnp-org:service:AVTransport:1}Play
	action := ""
	if idx := strings.LastIndex(fullTag, "}"); idx != -1 {
		action = fullTag[idx+1:]
	} else {
		action = fullTag
	}

	namespaceURI := actionElem.NamespaceURI() // 对当前元素有效
	parts := strings.Split(namespaceURI, ":")
	serviceName := ""
	if len(parts) > 3 {
		serviceName = parts[3]
	}

	method := fmt.Sprintf("%s_%s", serviceName, action)
	/*
		// 日志（过滤高频查询以减少刷屏）
		if method != "AVTransport_GetPositionInfo" &&
		method != "AVTransport_GetTransportInfo" &&
			method != "RenderingControl_GetVolume" {

			//log.Printf("namespaceURI %s\n", string(namespaceURI))
			//log.Printf("Call %s %v\n", method, param)
		}
	*/

	res := make(map[string]string)
	svc := GetService(serviceName)
	act, ok := svc.Actions[action]
	if !ok {
		return nil, fmt.Errorf("unknown action %s", action)
	}

	if p.hasMethod(method) {
		// 准备输入参数并调用自定义方法
		callData := make(map[string]*Argument)
		for _, inArg := range act.Input {
			val := param[inArg.Name]
			callData[inArg.Name] = &Argument{
				Name:  inArg.Name,
				State: inArg.State,
				Value: val,
			}
			// 同时更新相关状态变量
			if val != "" {
				p.SetState(inArg.State, val)
			}
		}
		if len(callData) > 0 {
			//logger.Printf("Call: %v", callData)
		}
		result, err := p.callMethod(method, callData)
		if err != nil {
			return nil, err
		}
		res = result
	} else {
		// 未自定义的动作，从状态变量取值
		for _, outArg := range act.Output {
			if sv, ok := p.stateList[outArg.State]; ok {
				res[outArg.Name] = sv.Value
				//logger.Printf("outName:%s\toutState: %v\n", outArg.Name, outArg.State)
			} else {
				res[outArg.Name] = "" // 未知状态填空
			}
		}
	}

	// 调试日志
	if method != "AVTransport_GetPositionInfo" &&
		method != "AVTransport_GetTransportInfo" &&
		method != "ConnectionManager_GetProtocolInfo" {
		if len(res) > 0 {
			logger.Printf("Result: %v", res)
		}
	}

	// 构建 SOAP 响应
	responseDoc := etree.NewDocument()
	ns := "http://schemas.xmlsoap.org/soap/envelope/"
	encStyle := "http://schemas.xmlsoap.org/soap/encoding/"
	envelope := responseDoc.CreateElement("s:Envelope")
	envelope.CreateAttr("xmlns:s", ns)
	envelope.CreateAttr("s:encodingStyle", encStyle)
	respBody := envelope.CreateElement("s:Body")
	actionResp := respBody.CreateElement("u:" + action + "Response")
	actionResp.CreateAttr("xmlns:u", svc.Namespace)
	for key, value := range res {
		prop := actionResp.CreateElement(key)
		prop.SetText(value)
	}
	return responseDoc.WriteToBytes()
}

// ---------- 可选读取状态方法 ----------
func (p *DLNAProtocol) GetStateTitle() string    { return p.GetState("CurrentTrackTitle") }
func (p *DLNAProtocol) GetStateURL() string      { return p.GetState("CurrentTrackURI") }
func (p *DLNAProtocol) GetStatePosition() string { return p.GetState("RelativeTimePosition") }
func (p *DLNAProtocol) GetStateDuration() string { return p.GetState("CurrentMediaDuration") }
func (p *DLNAProtocol) GetStateVolume() int {
	v := p.GetState("Volume")
	var vol int
	fmt.Sscanf(v, "%d", &vol)
	return vol
}
func (p *DLNAProtocol) GetStateMute() bool {
	return strings.ToLower(p.GetState("Mute")) == "true"
}
func (p *DLNAProtocol) GetStateTransportState() string  { return p.GetState("TransportState") }
func (p *DLNAProtocol) GetStateTransportStatus() string { return p.GetState("TransportStatus") }
func (p *DLNAProtocol) GetStateSpeed() string           { return p.GetState("TransportPlaySpeed") }
func (p *DLNAProtocol) GetStateDisplaySubtitle() bool {
	return strings.ToLower(p.GetState("DisplayCurrentSubtitle")) == "true"
}

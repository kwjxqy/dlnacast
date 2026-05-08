package utils

import (
	"fmt"
	"net"
	"strings"

	"github.com/google/uuid"
)

const (
	Version    = "1.0.0"
	ServerInfo = "UPnP/1.0 DongDongCast/1.0"
)

var (
	nowId        string
	friendlyName string
)

func SetName(name string) {
	friendlyName = name
}

func GetName() string {
	if len(friendlyName) <= 0 {
		friendlyName = "冬冬投屏"
	}
	return friendlyName
}

func SetUUID(id string) {
	nowId = id
}

func GetUUID() string {
	if len(nowId) <= 0 {
		nowId = fmt.Sprintf("uuid:%s", GenerateDeviceUUID(friendlyName))
	}
	return nowId
}

// GenerateDeviceUUID 根据指定字符串生成确定性的 UUID v5
func GenerateDeviceUUID(seed string) string {
	return uuid.NewSHA1(uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"), []byte(seed)).String()
}

func GetLocalIP() (string, error) {
	conn, err := net.Dial("udp", "8.8.8.8:53")
	if err != nil {
		return "", fmt.Errorf("failed to get local IP: %w", err)
	}
	defer conn.Close()
	return strings.Split(conn.LocalAddr().String(), ":")[0], nil
}

var mask = net.IPMask([]byte{255, 255, 255, 0})

func SameSubnet(ip1, ip2 []byte) bool {
	a := net.IP(ip1)
	b := net.IP(ip2)

	// 防御：补齐或统一长度
	a = a.To4() // 如果是 IPv4，统一转为 4 字节
	b = b.To4()
	if a == nil || b == nil {
		// 如果不是 IPv4，则按 16 字节 IPv6 处理
		a = net.IP(ip1).To16()
		b = net.IP(ip2).To16()
	}
	if a == nil || b == nil {
		return false
	}

	// 计算网络号并比较
	netA := a.Mask(mask)
	netB := b.Mask(mask)
	return netA.Equal(netB)
}

// GetLocalIPv4Addrs 返回本机所有 Up 且非回环接口上的 IPv4 地址列表
func GetLocalIPv4Addrs() ([]net.IP, error) {
	var ips []net.IP

	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}

	for _, iface := range ifaces {
		// 跳过未启用或回环接口
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			ipNet, ok := addr.(*net.IPNet)
			if !ok {
				continue
			}
			ip4 := ipNet.IP.To4()
			if ip4 != nil {
				ips = append(ips, ip4)
			}
		}
	}

	return ips, nil
}

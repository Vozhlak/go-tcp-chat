package domain

import (
	"net"
	"sync"
	"time"
)

// ChatMessage представляет сообщение в чате
type ChatMessage struct {
	Timestamp   time.Time
	ClientID    string
	Content     string
	MessageType string // "user" или "system"
}

// Client представляет подключенного клиента
type Client struct {
	ID       string
	Conn     net.Conn
	JoinTime time.Time
}

// ServerStats содержит статистику сервера
type ServerStats struct {
	ActiveConnections      int64
	TotalMessagesProcessed int64
	UptimeSeconds          int64
	ErrorCount             int64
}

// ServerConfig содержит конфигурацию сервера
type ServerConfig struct {
	Port               string
	MaxConnections     int
	LogLevel           string
	MessageHistorySize int
	HTTPPort           string
}

// MessageHistory хранит историю сообщений с кольцевым буфером
type MessageHistory struct {
	mu       sync.RWMutex
	Messages []ChatMessage
	head     int
	Size     int
}

// Request используется для получения списка клиентов
type Request struct {
	Response chan []string
}

// CountRequest используется для получения количества клиентов
type CountRequest struct {
	Response chan int
}

// Add добавляет сообщение в историю
func (mh *MessageHistory) Add(msg ChatMessage) {
	mh.mu.Lock()
	defer mh.mu.Unlock()

	mh.Messages[mh.head%mh.Size] = msg
	mh.head++
}

// GetRecent возвращает последние сообщения
func (mh *MessageHistory) GetRecent() []ChatMessage {
	mh.mu.RLock()
	defer mh.mu.RUnlock()

	if mh.head == 0 {
		return []ChatMessage{}
	}

	if mh.head < mh.Size {
		return mh.Messages[0:mh.head]
	}

	buff := mh.head % mh.Size
	result := make([]ChatMessage, mh.Size)

	copy(result, mh.Messages[buff:])
	copy(result[mh.Size-buff:], mh.Messages[0:buff])

	return result
}

// HealthResponse представляет ответ endpoint /health
type HealthResponse struct {
	Status            string `json:"status"`
	ActiveConnections int64  `json:"active_connections"`
	UptimeSeconds     int64  `json:"uptime_seconds"`
}

// StatsResponse представляет ответ endpoint /stats
type StatsResponse struct {
	ActiveConnections      int64   `json:"active_connections"`
	TotalMessagesProcessed int64   `json:"total_messages_processed"`
	UptimeSeconds          int64   `json:"uptime_seconds"`
	ErrorCount             int64   `json:"error_count"`
	MessageRate            float64 `json:"message_rate"`
}

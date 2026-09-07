package hub

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Vozhlak/go-tcp-chat/internal/domain"

	"github.com/google/uuid"
)

const (
	readTimeout = 10 * time.Minute
)

// Hub управляет клиентами и сообщениями
type Hub struct {
	mu             sync.RWMutex
	Clients        map[string]*domain.Client
	Broadcast      chan domain.ChatMessage
	Register       chan *domain.Client
	unregister     chan *domain.Client
	clientsReq     chan *domain.Request
	countReq       chan *domain.CountRequest
	MessageHistory domain.MessageHistory
	Stats          domain.ServerStats
	StartTime      time.Time
}

// New создаёт новый Hub
func New(cfg domain.ServerConfig) *Hub {
	return &Hub{
		Clients:    make(map[string]*domain.Client),
		Broadcast:  make(chan domain.ChatMessage),
		Register:   make(chan *domain.Client, 1),
		unregister: make(chan *domain.Client, 1),
		clientsReq: make(chan *domain.Request, 1),
		countReq:   make(chan *domain.CountRequest, 1),
		MessageHistory: domain.MessageHistory{
			Messages: make([]domain.ChatMessage, cfg.MessageHistorySize),
			Size:     cfg.MessageHistorySize,
		},
		Stats:     domain.ServerStats{},
		StartTime: time.Now(),
	}
}

// Run запускает основной цикл Hub
func (h *Hub) Run() {
	for {
		select {
		case cl, ok := <-h.Register:
			if !ok {
				return
			}

			h.mu.Lock()
			if _, existsClient := h.Clients[cl.ID]; existsClient {
				h.mu.Unlock()
				fmt.Println("user has register")

				continue
			}

			h.Clients[cl.ID] = cl
			h.mu.Unlock()
		case cl, ok := <-h.unregister:
			if !ok {
				return
			}

			h.mu.Lock()
			delete(h.Clients, cl.ID)
			h.mu.Unlock()
		case msg, ok := <-h.Broadcast:
			if !ok {
				return
			}

			h.MessageHistory.Add(msg)
			h.BroadcastMessage(msg)

			atomic.AddInt64(&h.Stats.TotalMessagesProcessed, 1)
		case req, ok := <-h.clientsReq:
			if !ok {
				return
			}

			h.mu.RLock()
			clientIds := make([]string, 0, len(h.Clients))

			for _, cl := range h.Clients {
				clientIds = append(clientIds, cl.ID)
			}

			h.mu.RUnlock()

			req.Response <- clientIds
		case req, ok := <-h.countReq:
			if !ok {
				return
			}

			h.mu.RLock()
			count := len(h.Clients)
			h.mu.RUnlock()

			req.Response <- count
		}
	}
}

// BroadcastMessage отправляет сообщение всем клиентам
func (h *Hub) BroadcastMessage(msg domain.ChatMessage) {
	h.mu.RLock()
	h.mu.RUnlock()

	for _, cl := range h.Clients {
		if cl.ID == msg.ClientID {
			continue
		}

		formatted := FormatMessage(msg)
		_, err := cl.Conn.Write([]byte(formatted + "\n"))
		if err != nil {
			fmt.Printf("write error to %s: %v\n", cl.Conn.RemoteAddr(), err)
		}
	}
}

// GetActiveClients возвращает список активных клиентов
func (h *Hub) GetActiveClients() []string {
	resp := make(chan []string, 1)

	req := &domain.Request{
		Response: resp,
	}

	h.clientsReq <- req

	return <-resp
}

// GetClientCount возвращает количество клиентов
func (h *Hub) GetClientCount() int {
	resp := make(chan int, 1)

	req := &domain.CountRequest{
		Response: resp,
	}

	h.countReq <- req

	return <-resp
}

// SetupClientConnection настраивает новое подключение клиента
func (h *Hub) SetupClientConnection(conn net.Conn) *domain.Client {
	client := &domain.Client{
		ID:       "User_" + GenerateClientID(),
		Conn:     conn,
		JoinTime: time.Now(),
	}

	conn.SetReadDeadline(time.Now().Add(readTimeout))

	clientID := client.ID
	welcomeMsg := fmt.Sprintf("Welcome to the chat, %s!\n", clientID)
	_, err := conn.Write([]byte(welcomeMsg))
	if err != nil {
		fmt.Printf("failed to send welcome message to %s: %v\n", clientID, err)
	}

	fmt.Printf("Client %s connected from %s\n", clientID, conn.RemoteAddr())
	fmt.Printf("Welcome message sent to %s\n", clientID)

	h.sendHistoryToClient(client)

	return client
}

// cleanupClient удаляет клиента из Hub
func (h *Hub) cleanupClient(client *domain.Client) {
	h.unregister <- client

	disconnectMsg := domain.ChatMessage{
		Timestamp:   time.Now(),
		ClientID:    "server",
		Content:     fmt.Sprintf("%s has left the chat", client.ID),
		MessageType: "system",
	}
	h.Broadcast <- disconnectMsg

	client.Conn.Close()
}

// SendUserList отправляет список пользователей клиенту
func (h *Hub) SendUserList(client *domain.Client) {
	clientIds := h.GetActiveClients()

	count := len(clientIds)

	content := fmt.Sprintf("Online users (%d): %s", count, strings.Join(clientIds, ", "))

	msg := domain.ChatMessage{
		Timestamp:   time.Now(),
		ClientID:    "server",
		Content:     content,
		MessageType: "system",
	}

	formatted := FormatMessage(msg)
	if _, err := client.Conn.Write([]byte(formatted + "\n")); err != nil {
		fmt.Printf("write system error of sender user list to %s: %v\n", client.Conn.RemoteAddr(), err)
	}
}

// HandleCommand обрабатывает команды клиента
func (h *Hub) HandleCommand(client *domain.Client, command string) {
	switch command {
	case "/users":
		h.SendUserList(client)
	case "/help":
		msg := domain.ChatMessage{
			Timestamp:   time.Now(),
			ClientID:    "Server",
			Content:     "Available commands: /users, /help, /quit, /time",
			MessageType: "system",
		}

		formatted := FormatMessage(msg)
		if _, err := client.Conn.Write([]byte(formatted + "\n")); err != nil {
			fmt.Printf("write system to %s: %v\n", client.Conn.RemoteAddr(), err)
		}
	case "/time":
		msg := domain.ChatMessage{
			Timestamp:   time.Now(),
			ClientID:    "Server",
			Content:     fmt.Sprintf("Current time: %s", time.Now().Format("15:04:05")),
			MessageType: "system",
		}

		formatted := FormatMessage(msg)
		if _, err := client.Conn.Write([]byte(formatted + "\n")); err != nil {
			fmt.Printf("write system to %s: %v\n", client.Conn.RemoteAddr(), err)
		}
	case "/quit":
		msg := domain.ChatMessage{
			Timestamp:   time.Now(),
			ClientID:    "server",
			Content:     "Goodbye! Closing connection.",
			MessageType: "system",
		}
		formatted := FormatMessage(msg)
		client.Conn.Write([]byte(formatted + "\n"))

		client.Conn.Close()
	default:
		msg := domain.ChatMessage{
			Timestamp:   time.Now(),
			ClientID:    "server",
			Content:     fmt.Sprintf("Unknown command: %s", command),
			MessageType: "system",
		}
		formatted := FormatMessage(msg)
		client.Conn.Write([]byte(formatted + "\n"))
	}
}

// HandleClientMessages обрабатывает сообщения от клиента
func (h *Hub) HandleClientMessages(client *domain.Client) error {
	defer h.cleanupClient(client)

	conn := client.Conn
	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		conn.SetReadDeadline(time.Now().Add(readTimeout))

		content := scanner.Text()

		if strings.HasPrefix(content, "/") {
			h.HandleCommand(client, content)
			continue
		}

		msg := ParseIncomingMessage(content, client.ID)

		h.Broadcast <- msg

		fmt.Printf("[%s] Received from %s: %s\n", msg.Timestamp.Format("15:04:05"), msg.ClientID, msg.Content)
	}

	if err := scanner.Err(); err != nil {
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return fmt.Errorf("read timeout for %s: %w", client.ID, err)
		}
		return fmt.Errorf("scanner error: %w", err)
	}

	return nil
}

// Shutdown корректно останавливает Hub
func (h *Hub) Shutdown(ctx context.Context) error {
	h.mu.RLock()

	shutdownMsg := domain.ChatMessage{
		Timestamp:   time.Now(),
		ClientID:    "server",
		Content:     "Server shutting down in 10 seconds",
		MessageType: "system",
	}

	for _, client := range h.Clients {
		client.Conn.Write([]byte(FormatMessage(shutdownMsg) + "\n"))
	}

	h.mu.RUnlock()

	for {
		select {
		case <-ctx.Done():
			h.mu.RLock()
			for _, client := range h.Clients {
				client.Conn.Close()
			}
			h.mu.RUnlock()
			return ctx.Err()

		case <-time.After(10 * time.Second):
			h.mu.RLock()
			for _, client := range h.Clients {
				client.Conn.Close()
			}
			h.mu.RUnlock()
			return nil
		}
	}
}

// sendHistoryToClient отправляет историю сообщений клиенту
func (h *Hub) sendHistoryToClient(client *domain.Client) {
	history := h.MessageHistory.GetRecent()

	if len(history) == 0 {
		return
	}

	headerMsg := domain.ChatMessage{
		Timestamp:   time.Now(),
		ClientID:    "server",
		Content:     "--- Recent messages ---",
		MessageType: "system",
	}
	formatted := FormatMessage(headerMsg)
	client.Conn.Write([]byte(formatted + "\n"))

	for _, msg := range history {
		formatted = FormatMessage(msg)
		client.Conn.Write([]byte(formatted + "\n"))
	}

	footerMsg := domain.ChatMessage{
		Timestamp:   time.Now(),
		ClientID:    "server",
		Content:     "--- End of history ---",
		MessageType: "system",
	}
	formatted = FormatMessage(footerMsg)
	client.Conn.Write([]byte(formatted + "\n"))
}

// FormatMessage форматирует сообщение для вывода
func FormatMessage(msg domain.ChatMessage) string {
	formattedTime := msg.Timestamp.Format("15:04:05")
	if msg.MessageType == "system" {
		return fmt.Sprintf("[%s] *** %s", formattedTime, msg.Content)
	}

	return fmt.Sprintf("[%s] <%s>: %s", formattedTime, msg.ClientID, msg.Content)
}

// ParseIncomingMessage парсит входящее сообщение
func ParseIncomingMessage(raw, senderID string) domain.ChatMessage {
	return domain.ChatMessage{
		Timestamp:   time.Now(),
		ClientID:    senderID,
		Content:     raw,
		MessageType: "user",
	}
}

// GenerateClientID генерирует уникальный ID клиента
func GenerateClientID() string {
	return uuid.New().String()[:8]
}

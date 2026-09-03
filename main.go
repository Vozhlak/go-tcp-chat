package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
)

var (
	ErrorPortEmpty = errors.New("port cannot be empty")
)

type ChatMessage struct {
	Timestamp   time.Time
	ClientID    string
	Content     string
	MessageType string
}

type Client struct {
	ID       string
	Conn     net.Conn
	JoinTime time.Time
}

type Request struct {
	Response chan []string
}

type CountRequest struct {
	Response chan int
}

type MessageHistory struct {
	mu       sync.RWMutex
	messages []ChatMessage
	head     int
	size     int
}

type Hub struct {
	mu             sync.RWMutex
	Clients        map[string]*Client
	Broadcast      chan ChatMessage
	register       chan *Client
	unregister     chan *Client
	clientsReq     chan *Request
	countReq       chan *CountRequest
	MessageHistory MessageHistory
	Stats          ServerStats
	startTime      time.Time
}

type ServerStats struct {
	ActiveConnections      int64
	TotalMessagesProcessed int64
	UptimeSeconds          int64
	ErrorCount             int64
}

const readTimeout = 10 * time.Minute
const historySize = 50

func (mh *MessageHistory) Add(msg ChatMessage) {
	mh.mu.Lock()
	defer mh.mu.Unlock()

	mh.messages[mh.head%mh.size] = msg
	mh.head++
}

func (mh *MessageHistory) GetRecent() []ChatMessage {
	mh.mu.RLock()
	defer mh.mu.RUnlock()

	if mh.head == 0 {
		return []ChatMessage{}
	}

	if mh.head < mh.size {
		return mh.messages[0:mh.head]
	}

	buff := mh.head % mh.size
	result := make([]ChatMessage, mh.size)

	copy(result, mh.messages[buff:])
	copy(result[mh.size-buff:], mh.messages[0:buff])

	return result
}

func (h *Hub) SendUserList(client *Client) {
	clientIds := h.GetActiveClients()

	count := len(clientIds)

	content := fmt.Sprintf("Online users (%d): %s", count, strings.Join(clientIds, ", "))

	msg := ChatMessage{
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

func (h *Hub) HandleCommand(client *Client, command string) {
	switch command {
	case "/users":
		h.SendUserList(client)
	case "/help":
		msg := ChatMessage{
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
		msg := ChatMessage{
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
		msg := ChatMessage{
			Timestamp:   time.Now(),
			ClientID:    "server",
			Content:     "Goodbye! Closing connection.",
			MessageType: "system",
		}
		formatted := FormatMessage(msg)
		client.Conn.Write([]byte(formatted + "\n"))

		client.Conn.Close()
	default:
		msg := ChatMessage{
			Timestamp:   time.Now(),
			ClientID:    "server",
			Content:     fmt.Sprintf("Unknown command: %s", command),
			MessageType: "system",
		}
		formatted := FormatMessage(msg)
		client.Conn.Write([]byte(formatted + "\n"))
	}
}

func (h *Hub) sendHistoryToClient(client *Client) {
	history := h.MessageHistory.GetRecent()

	if len(history) == 0 {
		return
	}

	headerMsg := ChatMessage{
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

	footerMsg := ChatMessage{
		Timestamp:   time.Now(),
		ClientID:    "server",
		Content:     "--- End of history ---",
		MessageType: "system",
	}
	formatted = FormatMessage(footerMsg)
	client.Conn.Write([]byte(formatted + "\n"))
}

func FormatMessage(msg ChatMessage) string {
	formattedTime := msg.Timestamp.Format("15:04:05")
	if msg.MessageType == "system" {
		return fmt.Sprintf("[%s] *** %s", formattedTime, msg.Content)
	}

	return fmt.Sprintf("[%s] <%s>: %s", formattedTime, msg.ClientID, msg.Content)
}

func ParseIncomingMessage(raw, senderID string) ChatMessage {
	return ChatMessage{
		Timestamp:   time.Now(),
		ClientID:    senderID,
		Content:     raw,
		MessageType: "user",
	}
}

func handleClientMessages(client *Client, h *Hub) error {
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

func GenerateClientID() string {
	return uuid.New().String()[:8]
}

func handleClient(conn net.Conn, h *Hub, logger *log.Logger) {
	defer func() {
		if r := recover(); r != nil {
			logger.Printf("[ERROR] recovered from panic: %v", r)
			atomic.AddInt64(&h.Stats.ErrorCount, 1)
		}
	}()

	client := h.setupClientConnection(conn)

	clientID := client.ID

	h.register <- client

	atomic.AddInt64(&h.Stats.ActiveConnections, 1)

	logger.Printf("[INFO] Client %s connected. Total: %d\n", clientID, atomic.LoadInt64(&h.Stats.ActiveConnections))

	if err := handleClientMessages(client, h); err != nil {
		logger.Printf("[ERROR] Client %s error: %v\n", client.ID, err)
		atomic.AddInt64(&h.Stats.ErrorCount, 1)
	}

	atomic.AddInt64(&h.Stats.ActiveConnections, -1)
	atomic.StoreInt64(&h.Stats.UptimeSeconds, int64(time.Since(h.startTime).Seconds()))

	logger.Printf("[WARN] Client %s disconnected. Total: %d\n", clientID, atomic.LoadInt64(&h.Stats.ActiveConnections))
}

func StartEchoServer(port string, h *Hub, logger *log.Logger) error {
	if port == "" {
		return ErrorPortEmpty
	}
	listener, err := net.Listen("tcp", port)
	if err != nil {
		return fmt.Errorf("failed to create TCP listener: %w", err)
	}
	defer listener.Close()

	logger.Printf("[INFO] Server starting on port %s", port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			return fmt.Errorf("failed to accept connection: %w", err)
		}

		go handleClient(conn, h, logger)
	}
}

func (h *Hub) Run() {
	for {
		select {
		case cl, ok := <-h.register:
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

func (h *Hub) BroadcastMessage(msg ChatMessage) {
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

func (h *Hub) GetActiveClients() []string {
	resp := make(chan []string, 1)

	req := &Request{
		Response: resp,
	}

	h.clientsReq <- req

	return <-resp
}

func (h *Hub) GetClientCount() int {
	resp := make(chan int, 1)

	req := &CountRequest{
		Response: resp,
	}

	h.countReq <- req

	return <-resp
}

func (h *Hub) setupClientConnection(conn net.Conn) *Client {
	client := &Client{
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

func (h *Hub) cleanupClient(client *Client) {
	h.unregister <- client

	disconnectMsg := ChatMessage{
		Timestamp:   time.Now(),
		ClientID:    "server",
		Content:     fmt.Sprintf("%s has left the chat", client.ID),
		MessageType: "system",
	}
	h.Broadcast <- disconnectMsg

	client.Conn.Close()
}

func setupLogging(level string) *log.Logger {
	log.SetFlags(log.Ldate | log.Ltime)
	log.SetPrefix("[TCP-CHAT] ")

	return log.Default()
}

func setupSignalHandling() chan os.Signal {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	return ch
}

func (h *Hub) Shutdown(ctx context.Context) error {
	h.mu.RLock()

	shutdownMsg := ChatMessage{
		Timestamp:   time.Now(),
		ClientID:    "server",
		Content:     "Server shutting down in 10 seconds",
		MessageType: "system",
	}

	for _, client := range h.Clients {
		client.Conn.Write([]byte(FormatMessage(shutdownMsg) + "\n"))
	}

	h.mu.RUnlock()

	close(h.Broadcast)

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

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := setupSignalHandling()

	logger := setupLogging("INFO")

	hub := &Hub{
		Clients:    make(map[string]*Client),
		Broadcast:  make(chan ChatMessage),
		register:   make(chan *Client, 1),
		unregister: make(chan *Client, 1),
		clientsReq: make(chan *Request, 1),
		countReq:   make(chan *CountRequest, 1),
		MessageHistory: MessageHistory{
			messages: make([]ChatMessage, historySize),
			size:     historySize,
		},
		Stats:     ServerStats{},
		startTime: time.Now(),
	}

	go hub.Run()

	go func() {
		if err := StartEchoServer(":8080", hub, logger); err != nil {
			logger.Printf("[ERROR] Server error: %v", err)
		}
	}()

	sig := <-sigChan
	logger.Printf("[WARN] Shutdown signal received: %v", sig)

	if err := hub.Shutdown(ctx); err != nil {
		logger.Printf("[ERROR] Shutdown error: %v", err)
	}

	logger.Printf("[INFO] Server stopped gracefully")
}

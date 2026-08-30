package main

import (
	"bufio"
	"errors"
	"fmt"
	"net"
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

type Hub struct {
	Clients    map[string]*Client
	Broadcast  chan ChatMessage
	register   chan *Client
	unregister chan *Client
	clientsReq chan *Request
	countReq   chan *CountRequest
}

const readTimeout = 10 * time.Minute

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

func handleClient(conn net.Conn, h *Hub) {
	client := h.setupClientConnection(conn)

	clientID := client.ID

	h.register <- client

	fmt.Printf("Client %s connected. Total: %d\n", clientID, h.GetClientCount())

	if err := handleClientMessages(client, h); err != nil {
		fmt.Printf("Client %s error: %v\n", client.ID, err)
	}

	fmt.Printf("Client %s disconnected. Total: %d\n", clientID, h.GetClientCount())
}

func StartEchoServer(port string, h *Hub) error {
	if port == "" {
		return ErrorPortEmpty
	}
	listener, err := net.Listen("tcp", port)
	if err != nil {
		return fmt.Errorf("failed to create TCP listener: %w", err)
	}
	defer listener.Close()

	fmt.Printf("TCP Echo Server listening on %s\n", port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			return fmt.Errorf("failed to accept connection: %w", err)
		}

		go handleClient(conn, h)
	}
}

func (h *Hub) Run() {
	for {
		select {
		case cl, ok := <-h.register:
			if !ok {
				return
			}

			if _, existsClient := h.Clients[cl.ID]; existsClient {
				fmt.Println("user has register")

				continue
			}

			h.Clients[cl.ID] = cl
		case cl, ok := <-h.unregister:
			if !ok {
				return
			}

			delete(h.Clients, cl.ID)
		case msg, ok := <-h.Broadcast:
			if !ok {
				return
			}

			h.BroadcastMessage(msg)
		case req, ok := <-h.clientsReq:
			if !ok {
				return
			}

			clientIds := make([]string, 0, len(h.Clients))

			for _, cl := range h.Clients {
				clientIds = append(clientIds, cl.ID)
			}

			req.Response <- clientIds
		case req, ok := <-h.countReq:
			if !ok {
				return
			}

			count := len(h.Clients)

			req.Response <- count
		}
	}
}

func (h *Hub) BroadcastMessage(msg ChatMessage) {
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

	fmt.Printf("Client %s disconnected (timeout)\n", client.ID)
	fmt.Printf("Cleanup completed for %s\n", client.ID)
}

func main() {
	hub := &Hub{
		Clients:    make(map[string]*Client),
		Broadcast:  make(chan ChatMessage),
		register:   make(chan *Client, 1),
		unregister: make(chan *Client, 1),
		clientsReq: make(chan *Request, 1),
		countReq:   make(chan *CountRequest, 1),
	}

	go hub.Run()

	if err := StartEchoServer(":8080", hub); err != nil {
		fmt.Printf("Server error: %v\n", err)
	}
}

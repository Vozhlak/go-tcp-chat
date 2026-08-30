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

type Hub struct {
	Clients    map[string]*Client
	Broadcast  chan ChatMessage
	register   chan *Client
	unregister chan *Client
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

func HandleClient(client *Client, h *Hub) error {
	conn := client.Conn
	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		content := scanner.Text()
		msg := ParseIncomingMessage(content, client.ID)

		h.Broadcast <- msg

		fmt.Printf("[%s] Received from %s: %s\n", msg.Timestamp.Format("15:04:05"), msg.ClientID, msg.Content)
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanner error: %w", err)
	}

	return nil
}

func GenerateClientID() string {
	return uuid.New().String()[:8]
}

func handleClient(conn net.Conn, clientID string, h *Hub) {
	defer conn.Close()

	fmt.Printf("Client %s connected, starting goroutine\n", clientID)

	client := &Client{
		ID:       clientID,
		Conn:     conn,
		JoinTime: time.Now(),
	}

	h.register <- client

	if err := HandleClient(client, h); err != nil {
		fmt.Printf("Client %s error: %v\n", client.ID, err)
	}

	h.unregister <- client
	fmt.Printf("Client %s disconnected\n", client.ID)
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

		clientID := "User_" + GenerateClientID()

		go handleClient(conn, clientID, h)
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

			cl.Conn.Close()
		}
	}

}

func main() {
	hub := &Hub{
		Clients:    make(map[string]*Client),
		Broadcast:  make(chan ChatMessage),
		register:   make(chan *Client, 1),
		unregister: make(chan *Client, 1),
	}

	go hub.Run()

	if err := StartEchoServer(":8080", hub); err != nil {
		fmt.Printf("Server error: %v\n", err)
	}
}

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

func HandleClient(client *Client) error {
	conn := client.Conn
	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		content := scanner.Text()
		msg := ParseIncomingMessage(content, client.ID)
		formattedMsg := FormatMessage(msg)

		fmt.Printf("[%s] Received from %s: %s\n", msg.Timestamp.Format("15:04:05"), msg.ClientID, msg.Content)

		if _, err := conn.Write([]byte(formattedMsg + "\n")); err != nil {
			return fmt.Errorf("write error to %s: %w", conn.RemoteAddr(), err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanner error: %w", err)
	}

	return nil
}

func GenerateClientID() string {
	return uuid.New().String()[:8]
}

func handleClient(conn net.Conn, clientID string) {
	defer conn.Close()

	fmt.Printf("Client %s connected, starting goroutine\n", clientID)

	client := &Client{
		ID:       clientID,
		Conn:     conn,
		JoinTime: time.Now(),
	}

	if err := HandleClient(client); err != nil {
		fmt.Printf("Client %s error: %v\n", client.ID, err)
	}

	fmt.Printf("Client %s disconnected\n", client.ID)
}

func StartEchoServer(port string) error {
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

		go handleClient(conn, clientID)
	}
}

func main() {
	if err := StartEchoServer(":8080"); err != nil {
		fmt.Printf("Server error: %v\n", err)
	}
}

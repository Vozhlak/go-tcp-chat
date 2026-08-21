package main

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"time"
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
	fmt.Println("Waiting for connection...")

	conn, err := listener.Accept()
	if err != nil {
		return fmt.Errorf("failed to accept connection: %w", err)
	}
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		content := scanner.Text()
		msg := ParseIncomingMessage(content, "User_001")
		formatedMsg := FormatMessage(msg)

		if _, err = conn.Write([]byte(formatedMsg + "\n")); err != nil {
			return fmt.Errorf("write error to %s: %w", conn.RemoteAddr(), err)
		}
	}

	return nil
}

func main() {
	if err := StartEchoServer(":8080"); err != nil {
		fmt.Printf("Server error: %v\n", err)
	}
}

package main

import (
	"bufio"
	"errors"
	"fmt"
	"net"
)

var (
	ErrorPortEmpty = errors.New("port cannot be empty")
)

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
		if _, err = conn.Write([]byte(scanner.Text() + "\n")); err != nil {
			return fmt.Errorf("Write error to %s: %w\n", conn.RemoteAddr(), err)
		}
	}

	return nil
}

func main() {
	if err := StartEchoServer(":8080"); err != nil {
		fmt.Printf("Server error: %v\n", err)
	}
}

package server

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync/atomic"
	"time"

	"github.com/Vozhlak/go-tcp-chat/internal/hub"
)

var (
	ErrorPortEmpty = errors.New("port cannot be empty")
)

// TCPServer представляет TCP сервер
type TCPServer struct {
	port   string
	hub    *hub.Hub
	logger *log.Logger
}

// NewTCPServer создаёт новый TCP сервер
func NewTCPServer(port string, h *hub.Hub, logger *log.Logger) *TCPServer {
	return &TCPServer{
		port:   port,
		hub:    h,
		logger: logger,
	}
}

// Start запускает TCP сервер
func (s *TCPServer) Start() error {
	if s.port == "" {
		return ErrorPortEmpty
	}

	listener, err := net.Listen("tcp", s.port)
	if err != nil {
		return fmt.Errorf("failed to create TCP listener: %w", err)
	}
	defer listener.Close()

	s.logger.Printf("[INFO] Server starting on port %s", s.port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			return fmt.Errorf("failed to accept connection: %w", err)
		}

		go s.handleClient(conn)
	}
}

// handleClient обрабатывает подключение клиента
func (s *TCPServer) handleClient(conn net.Conn) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Printf("[ERROR] recovered from panic: %v", r)
			atomic.AddInt64(&s.hub.Stats.ErrorCount, 1)
		}
	}()

	client := s.hub.SetupClientConnection(conn)
	clientID := client.ID

	s.hub.Register <- client

	atomic.AddInt64(&s.hub.Stats.ActiveConnections, 1)

	s.logger.Printf("[INFO] Client %s connected. Total: %d\n", clientID, atomic.LoadInt64(&s.hub.Stats.ActiveConnections))

	if err := s.hub.HandleClientMessages(client); err != nil {
		s.logger.Printf("[ERROR] Client %s error: %v\n", client.ID, err)
		atomic.AddInt64(&s.hub.Stats.ErrorCount, 1)
	}

	atomic.AddInt64(&s.hub.Stats.ActiveConnections, -1)
	atomic.StoreInt64(&s.hub.Stats.UptimeSeconds, int64(time.Since(s.hub.StartTime).Seconds()))

	s.logger.Printf("[WARN] Client %s disconnected. Total: %d\n", clientID, atomic.LoadInt64(&s.hub.Stats.ActiveConnections))
}

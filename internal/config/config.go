package config

import (
	"flag"

	"github.com/Vozhlak/go-tcp-chat/internal/domain"
)

// Parse парсит аргументы командной строки
func Parse() domain.ServerConfig {
	port := flag.String("port", "8080", "Server port")
	maxConnections := flag.Int("max-connections", 500, "Maximum connections")
	logLevel := flag.String("log-level", "INFO", "Log level")
	messageHistorySize := flag.Int("message-history-size", 50, "Message history size")
	httpPort := flag.String("http-port", "8081", "HTTP monitoring port")

	flag.Parse()

	return domain.ServerConfig{
		Port:               *port,
		MaxConnections:     *maxConnections,
		LogLevel:           *logLevel,
		MessageHistorySize: *messageHistorySize,
		HTTPPort:           *httpPort,
	}
}

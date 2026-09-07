package app

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Vozhlak/go-tcp-chat/internal/config"
	"github.com/Vozhlak/go-tcp-chat/internal/domain"
	"github.com/Vozhlak/go-tcp-chat/internal/hub"
	"github.com/Vozhlak/go-tcp-chat/internal/server"
)

// App представляет приложение
type App struct {
	cfg    domain.ServerConfig
	hub    *hub.Hub
	logger *log.Logger
}

// New создаёт новое приложение
func New(cfg domain.ServerConfig) *App {
	logger := setupLogging(cfg.LogLevel)

	h := hub.New(cfg)

	return &App{
		cfg:    cfg,
		hub:    h,
		logger: logger,
	}
}

// Run запускает приложение
func (a *App) Run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	printStartupBanner(a.cfg)

	// Запускаем Hub
	go a.hub.Run()

	// Запускаем HTTP сервер мониторинга
	httpServer := server.NewHTTPServer(a.hub, a.cfg.HTTPPort)
	go func() {
		if err := httpServer.Start(); err != nil {
			a.logger.Printf("[ERROR] HTTP server error: %v", err)
		}
	}()

	// Запускаем TCP сервер
	tcpServer := server.NewTCPServer(":"+a.cfg.Port, a.hub, a.logger)
	go func() {
		if err := tcpServer.Start(); err != nil {
			a.logger.Printf("[ERROR] TCP server error: %v", err)
		}
	}()

	// Ждём сигнал завершения
	sigChan := setupSignalHandling()
	sig := <-sigChan
	a.logger.Printf("[WARN] Shutdown signal received: %v", sig)

	// Корректное завершение работы
	if err := a.hub.Shutdown(ctx); err != nil {
		a.logger.Printf("[ERROR] Shutdown error: %v", err)
	}

	a.logger.Printf("[INFO] Server stopped gracefully")

	return nil
}

// setupLogging настраивает логгер
func setupLogging(level string) *log.Logger {
	log.SetFlags(log.Ldate | log.Ltime)
	log.SetPrefix("[TCP-CHAT] ")
	return log.Default()
}

// setupSignalHandling настраивает обработку сигналов
func setupSignalHandling() chan os.Signal {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	return ch
}

// printStartupBanner печатает баннер при запуске
func printStartupBanner(cfg domain.ServerConfig) {
	fmt.Println("╔══════════════════════════════════════╗")
	fmt.Println("║         TCP Chat Server              ║")
	fmt.Println("╚══════════════════════════════════════╝")

	fmt.Printf("Port:            %s\n", cfg.Port)
	fmt.Printf("Max Connections: %d\n", cfg.MaxConnections)
	fmt.Printf("Log Level:       %s\n", cfg.LogLevel)
	fmt.Printf("Connect using: telnet localhost %s\n", cfg.Port)
	fmt.Println()
}

// Run - convenience функция для запуска приложения
func Run() error {
	cfg := config.Parse()
	app := New(cfg)
	return app.Run()
}

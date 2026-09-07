package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"sync/atomic"
	"time"

	"go-tcp-chat/internal/domain"
	"go-tcp-chat/internal/hub"
)

// HTTPServer представляет HTTP сервер для мониторинга
type HTTPServer struct {
	hub  *hub.Hub
	port string
}

// NewHTTPServer создаёт новый HTTP сервер
func NewHTTPServer(h *hub.Hub, port string) *HTTPServer {
	return &HTTPServer{
		hub:  h,
		port: port,
	}
}

// Start запускает HTTP сервер
func (s *HTTPServer) Start() error {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", s.handleHealthEndpoint())
	mux.HandleFunc("/stats", s.handleStatsEndpoint())

	server := &http.Server{
		Addr:         ":" + s.port,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return nil
}

// handleHealthEndpoint обрабатывает запросы к /health
func (s *HTTPServer) handleHealthEndpoint() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		activeConnections := atomic.LoadInt64(&s.hub.Stats.ActiveConnections)
		uptimeSeconds := atomic.LoadInt64(&s.hub.Stats.UptimeSeconds)

		response := domain.HealthResponse{
			Status:            "healthy",
			ActiveConnections: activeConnections,
			UptimeSeconds:     uptimeSeconds,
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		json.NewEncoder(w).Encode(response)
	}
}

// handleStatsEndpoint обрабатывает запросы к /stats
func (s *HTTPServer) handleStatsEndpoint() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		activeConnections := atomic.LoadInt64(&s.hub.Stats.ActiveConnections)
		totalMessages := atomic.LoadInt64(&s.hub.Stats.TotalMessagesProcessed)
		uptimeSeconds := atomic.LoadInt64(&s.hub.Stats.UptimeSeconds)
		errorCount := atomic.LoadInt64(&s.hub.Stats.ErrorCount)

		messageRate := 0.0
		if uptimeSeconds > 0 {
			messageRate = float64(totalMessages) / float64(uptimeSeconds)
		}

		response := domain.StatsResponse{
			ActiveConnections:      activeConnections,
			TotalMessagesProcessed: totalMessages,
			UptimeSeconds:          uptimeSeconds,
			ErrorCount:             errorCount,
			MessageRate:            messageRate,
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		json.NewEncoder(w).Encode(response)
	}
}

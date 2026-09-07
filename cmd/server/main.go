package main

import (
	"log"

	"github.com/Vozhlak/go-tcp-chat/internal/app"
)

func main() {
	if err := app.Run(); err != nil {
		log.Fatalf("[FATAL] %v", err)
	}
}

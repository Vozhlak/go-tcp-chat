package main

import (
	"log"

	"go-tcp-chat/internal/app"
)

func main() {
	if err := app.Run(); err != nil {
		log.Fatalf("[FATAL] %v", err)
	}
}

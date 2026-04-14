package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	sdk "github.com/bubustack/bubu-sdk-go"
	agent "github.com/bubustack/livekit-bridge-engram/pkg/engram"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := sdk.StartStreaming(ctx, agent.New()); err != nil {
		log.Fatalf("livekit-bridge engram failed: %v", err)
	}
}

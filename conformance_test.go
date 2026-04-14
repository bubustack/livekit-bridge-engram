package main

import (
	"os"
	"testing"

	"github.com/bubustack/bubu-sdk-go/conformance"
	"github.com/bubustack/livekit-bridge-engram/pkg/config"
	"github.com/bubustack/livekit-bridge-engram/pkg/engram"
)

func TestStreamConformance(t *testing.T) {
	url := os.Getenv("LIVEKIT_URL")
	apiKey := os.Getenv("LIVEKIT_API_KEY")
	apiSecret := os.Getenv("LIVEKIT_API_SECRET")
	room := os.Getenv("LIVEKIT_ROOM")
	if url == "" || apiKey == "" || apiSecret == "" || room == "" {
		t.Skip("set LIVEKIT_URL, LIVEKIT_API_KEY, LIVEKIT_API_SECRET, LIVEKIT_ROOM to run")
	}
	cfg := config.Config{
		LiveKitURL: url,
		Room:       config.RoomRef{Name: room},
	}
	suite := conformance.StreamSuite[config.Config]{
		Engram: engram.New(),
		Config: cfg,
		Secrets: map[string]string{
			"API_KEY":    apiKey,
			"API_SECRET": apiSecret,
		},
	}
	suite.Run(t)
}

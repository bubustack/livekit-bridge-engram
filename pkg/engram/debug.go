package engram

import (
	"context"
	"log/slog"
	"strings"

	sdk "github.com/bubustack/bubu-sdk-go"
	sdkengram "github.com/bubustack/bubu-sdk-go/engram"
)

func (e *LiveKitAgent) debugEnabled(ctx context.Context, logger *slog.Logger) bool {
	if sdk.DebugModeEnabled() {
		return true
	}
	if logger == nil {
		return false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return logger.Enabled(ctx, slog.LevelDebug)
}

func (e *LiveKitAgent) logBootstrapMessage(ctx context.Context, logger *slog.Logger, msg *sdkengram.InboundMessage) {
	if !e.debugEnabled(ctx, logger) || msg == nil {
		return
	}
	logger.Debug("livekit-bridge bootstrap message",
		slog.Int("inputsBytes", len(msg.Inputs)),
		slog.Int("payloadBytes", len(messagePayloadBytes(*msg))),
		slog.Any("metadata", msg.Metadata),
	)
}

func (e *LiveKitAgent) logForwardedSample(ctx context.Context, logger *slog.Logger, sample audioSample, bytes int) {
	if !e.debugEnabled(ctx, logger) {
		return
	}
	logger.Debug("livekit-bridge forwarded audio",
		slog.String("participant", sample.Participant.Identity),
		slog.Int("bytes", bytes),
		slog.Time("timestamp", sample.Timestamp),
	)
}

func (e *LiveKitAgent) logDirective(ctx context.Context, logger *slog.Logger, dir *directive, audioBytes int) {
	if !e.debugEnabled(ctx, logger) || dir == nil {
		return
	}
	logger.Debug("livekit-bridge directive",
		slog.String("type", strings.ToLower(dir.Type)),
		slog.Int("audioBytes", audioBytes),
	)
}

func (e *LiveKitAgent) logDebug(ctx context.Context, logger *slog.Logger, msg string, attrs ...any) {
	if !e.debugEnabled(ctx, logger) || logger == nil {
		return
	}
	logger.Debug(msg, attrs...)
}

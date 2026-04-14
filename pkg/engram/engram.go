package engram

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/bubustack/bobrapet/pkg/storage"
	sdk "github.com/bubustack/bubu-sdk-go"
	sdkengram "github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/bubu-sdk-go/media"
	"github.com/bubustack/bubu-sdk-go/runtime"
	"github.com/bubustack/livekit-bridge-engram/pkg/config"
	"github.com/bubustack/tractatus/transport"
	mediasdk "github.com/livekit/media-sdk"
	lkproto "github.com/livekit/protocol/livekit"
	lklogger "github.com/livekit/protocol/logger"
	lksdk "github.com/livekit/server-sdk-go/v2"
	lkmedia "github.com/livekit/server-sdk-go/v2/pkg/media"
	"github.com/pion/webrtc/v4"
)

const transportAudioType = transport.StreamTypeSpeechAudio
const transportChatMessageType = transport.StreamTypeChatMessage
const transportChatResponseType = transport.StreamTypeChatResponse
const standardLiveKitChatTopic = "lk.chat"
const audioEncodingPCM = "pcm"

var emitSignalFunc = sdk.EmitSignal

// LiveKitAgent is a streaming Engram that joins a LiveKit room, forwards remote
// audio into the story pipeline, and plays downstream responses back into the room.
type LiveKitAgent struct {
	cfg         config.Config
	apiKey      string
	apiSecret   string
	storageOnce sync.Once
	storage     *storage.StorageManager
	storageErr  error
}

func New() *LiveKitAgent { return &LiveKitAgent{} }

// Init validates static configuration and loads secrets.
func (e *LiveKitAgent) Init(_ context.Context, cfg config.Config, secrets *sdkengram.Secrets) error {
	cfg = normalizeConfig(cfg)
	if strings.TrimSpace(cfg.LiveKitURL) == "" {
		return fmt.Errorf("livekitURL is required")
	}

	key, ok := secrets.Get("API_KEY")
	if !ok || strings.TrimSpace(key) == "" {
		return fmt.Errorf("API_KEY secret is required")
	}
	secret, ok := secrets.Get("API_SECRET")
	if !ok || strings.TrimSpace(secret) == "" {
		return fmt.Errorf("API_SECRET secret is required")
	}

	e.cfg = cfg
	e.apiKey = strings.TrimSpace(key)
	e.apiSecret = strings.TrimSpace(secret)
	return nil
}

// Stream handles a single StoryRun connection.
func (e *LiveKitAgent) Stream(
	ctx context.Context,
	in <-chan sdkengram.InboundMessage,
	out chan<- sdkengram.StreamMessage,
) error {
	logger := sdk.LoggerFromContext(ctx).With(
		slog.String("component", "livekit-bridge"),
	)
	execData, err := runtime.LoadExecutionContextData()
	var storyInfo sdkengram.StoryInfo
	if err != nil {
		logger.Warn("failed to load execution context metadata", slog.Any("error", err))
	} else if execData != nil {
		storyInfo = execData.StoryInfo
		logger = logger.With(
			slog.String("story", storyInfo.StoryName),
			slog.String("storyRun", storyInfo.StoryRunID),
			slog.String("step", storyInfo.StepName),
		)
	}
	logger.Info("Stream invoked by hub; awaiting bootstrap message")

	bootstrapMsg, inputs, err := e.bootstrap(ctx, in, logger)
	if err != nil {
		logger.Error("bootstrap failed", slog.Any("error", err))
		return err
	}
	if inputs != nil {
		logger.Info("Bootstrap resolved",
			slog.String("room", inputs.Room.Name),
			slog.String("roomSID", inputs.Room.SID),
			slog.String("participant", inputs.Participant.Identity),
			slog.String("sessionID", inputs.SessionID),
		)
	}

	session, err := e.startSession(ctx, inputs, storyInfo, logger)
	if err != nil {
		logger.Error("failed to start LiveKit session", slog.Any("error", err))
		return err
	}
	defer session.Close()

	_ = e.emitSessionEvent(ctx, out, inputs, session.agentIdentity, "livekit.session.started")

	g, gctx := errgroup.WithContext(ctx)
	if session.captureEnabled {
		g.Go(func() error { return e.forwardAudio(gctx, session, inputs, out, logger) })
	}
	if session.chatEnabled {
		g.Go(func() error { return e.forwardChat(gctx, session, inputs, out, logger) })
	}
	g.Go(func() error { return e.consumeDirectives(gctx, bootstrapMsg, in, session, logger) })

	err = g.Wait()
	session.Close()
	_ = e.emitSessionEvent(ctx, out, inputs, session.agentIdentity, "livekit.session.ended")
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (e *LiveKitAgent) bootstrap(
	ctx context.Context,
	in <-chan sdkengram.InboundMessage,
	logger *slog.Logger,
) (*sdkengram.InboundMessage, *stepInputs, error) {
	var mergedInputs *stepInputs

	if cfgInputs, err := inputsFromConfig(e.cfg); err == nil {
		logger.Info("Loaded bootstrap inputs from static config",
			slog.String("room", cfgInputs.Room.Name),
			slog.String("sessionID", cfgInputs.SessionID),
		)
		mergedInputs = mergeStepInputs(mergedInputs, cfgInputs)
	} else if err != nil {
		e.logDebug(ctx, logger, "config bootstrap unavailable", slog.Any("error", err))
	}
	if runtimeInputs, err := inputsFromRuntime(); err == nil {
		logger.Info("Loaded bootstrap inputs from runtime context",
			slog.String("room", runtimeInputs.Room.Name),
			slog.String("sessionID", runtimeInputs.SessionID),
		)
		mergedInputs = mergeStepInputs(mergedInputs, runtimeInputs)
	} else if err != nil {
		e.logDebug(ctx, logger, "runtime bootstrap unavailable", slog.Any("error", err))
	}
	if hasBootstrapRoom(mergedInputs) {
		return nil, mergedInputs, nil
	}

	for {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case msg, ok := <-in:
			payloadLen := len(messagePayloadBytes(msg))
			logger.Info("Bootstrap channel message",
				slog.Bool("ok", ok),
				slog.Int("inputsBytes", len(msg.Inputs)),
				slog.Int("payloadBytes", payloadLen),
				slog.Any("metadata", msg.Metadata),
			)
			e.logBootstrapMessage(ctx, logger, &msg)
			if !ok {
				return nil, nil, fmt.Errorf("stream closed before bootstrap inputs arrived")
			}
			raw, source := structuredStreamBytes(msg)
			if len(raw) == 0 {
				if isHeartbeat(msg.Metadata) {
					e.logDebug(ctx, logger, "Ignoring heartbeat prior to inputs")
					msg.Done()
					continue
				}
				logger.Warn("Received stream message without inputs; waiting for next bootstrap packet",
					slog.Any("metadata", msg.Metadata),
				)
				msg.Done()
				continue
			}
			// Recalculate payloadLen after checking inputs
			payloadLen = len(messagePayloadBytes(msg))
			logger.Info("Received bootstrap message from hub",
				slog.Int("inputsBytes", len(raw)),
				slog.Int("payloadBytes", payloadLen),
			)

			var parsed stepInputs
			if err := json.Unmarshal(raw, &parsed); err != nil {
				logger.Error("failed to unmarshal step inputs", slog.Any("error", err))
				msg.Done()
				continue
			}
			merged := mergeStepInputs(mergedInputs, &parsed)
			if !hasBootstrapRoom(merged) {
				logger.Warn("step inputs missing room.name; waiting for next message")
				msg.Done()
				continue
			}
			consumed := stripConsumedBootstrapBytes(msg, source)
			return &consumed, merged, nil
		}
	}
}

func (e *LiveKitAgent) startSession(
	ctx context.Context,
	inputs *stepInputs,
	storyInfo sdkengram.StoryInfo,
	logger *slog.Logger,
) (*livekitSession, error) {
	if inputs.Room.Name == "" {
		return nil, fmt.Errorf("room.name is required in step inputs")
	}

	agentID := ensureUniqueAgentIdentity(e.resolveAgentIdentity(inputs, storyInfo), storyInfo)
	sourceFilters := resolveSourceFilters(e.cfg.SourceAllowlist, inputs.SourceAllowlist)
	logger.Info("Starting LiveKit session",
		slog.String("room", inputs.Room.Name),
		slog.String("roomSID", inputs.Room.SID),
		slog.String("agentIdentity", agentID),
		slog.String("participant", inputs.Participant.Identity),
	)

	sess := e.newLivekitSession(ctx, agentID)
	callbacks := e.newRoomCallback(ctx, sess, agentID, sourceFilters, logger)
	room, err := e.connectRoom(inputs.Room.Name, agentID, logger, callbacks)
	if err != nil {
		return nil, err
	}
	sess.room = room
	logger.Info("Connected to LiveKit room",
		slog.String("room", inputs.Room.Name),
		slog.String("roomSID", inputs.Room.SID),
		slog.Int("sampleRate", e.cfg.Capture.SampleRate),
		slog.Int("channels", e.cfg.Capture.Channels),
	)
	if err := e.registerChatStreamHandler(room, sess, sourceFilters, agentID, logger); err != nil {
		room.Disconnect()
		return nil, err
	}

	e.startParticipantSnapshotLoop(ctx, room, inputs, logger)
	if err := e.publishPlaybackTrack(room, sess); err != nil {
		room.Disconnect()
		return nil, err
	}
	if sess.captureEnabled {
		subscribeExistingAudioParticipants(room, logger)
	}
	return sess, nil
}

func resolveSourceFilters(defaultFilters, override []string) []string {
	if len(override) > 0 {
		return override
	}
	return defaultFilters
}

func (e *LiveKitAgent) newLivekitSession(
	ctx context.Context,
	agentID string,
) *livekitSession {
	return &livekitSession{
		agentIdentity:   agentID,
		capture:         e.cfg.Capture,
		audio:           make(chan audioSample, 64),
		chat:            make(chan chatMessage, 32),
		chatEnabled:     e.cfg.Chat.Enabled,
		chatTopic:       e.cfg.Chat.Topic,
		captureEnabled:  enabledFlag(e.cfg.Capture.Enabled, true),
		playbackEnabled: enabledFlag(e.cfg.Playback.Enabled, true),
		done:            make(chan struct{}),
		storage:         e.storageManager(ctx),
	}
}

func (e *LiveKitAgent) newRoomCallback(
	ctx context.Context,
	sess *livekitSession,
	agentID string,
	sourceFilters []string,
	logger *slog.Logger,
) *lksdk.RoomCallback {
	return &lksdk.RoomCallback{
		OnParticipantConnected: func(rp *lksdk.RemoteParticipant) {
			e.handleParticipantConnected(sess, logger, rp)
		},
		ParticipantCallback: lksdk.ParticipantCallback{
			OnDataPacket: func(data lksdk.DataPacket, params lksdk.DataReceiveParams) {
				e.handleCustomChatPacket(sess, agentID, sourceFilters, logger, data, params)
			},
			OnTrackPublished: func(
				publication *lksdk.RemoteTrackPublication,
				rp *lksdk.RemoteParticipant,
			) {
				e.handleTrackPublished(sess, logger, publication, rp)
			},
			OnTrackSubscribed: func(
				track *webrtc.TrackRemote,
				publication *lksdk.RemoteTrackPublication,
				rp *lksdk.RemoteParticipant,
			) {
				e.handleTrackSubscribed(ctx, sess, agentID, sourceFilters, logger, track, publication, rp)
			},
		},
	}
}

func (e *LiveKitAgent) handleParticipantConnected(
	sess *livekitSession,
	logger *slog.Logger,
	rp *lksdk.RemoteParticipant,
) {
	if rp == nil {
		return
	}
	identity := strings.TrimSpace(rp.Identity())
	logger.Info("Participant connected",
		slog.String("participant", identity),
		slog.String("participantSID", rp.SID()),
	)
	if sess.captureEnabled {
		subscribeAudioPublications(logger, rp)
	}
}

func (e *LiveKitAgent) handleCustomChatPacket(
	sess *livekitSession,
	agentID string,
	sourceFilters []string,
	logger *slog.Logger,
	data lksdk.DataPacket,
	params lksdk.DataReceiveParams,
) {
	if !sess.chatEnabled || !usesCustomChatPackets(sess.chatTopic) {
		return
	}
	senderIdentity := strings.TrimSpace(params.SenderIdentity)
	if !e.shouldCaptureParticipant(senderIdentity, agentID) {
		return
	}
	if !matchFilter(sourceFilters, senderIdentity) {
		return
	}
	msg, ok := decodeChatPacket(data, params, sess.chatTopic)
	if !ok {
		return
	}
	logger.Info("Received chat packet",
		slog.String("sender", msg.Participant.Identity),
		slog.String("topic", msg.Topic),
		slog.Int("textLen", len(msg.Text)),
	)
	enqueueChatMessage(sess, logger, msg)
}

func (e *LiveKitAgent) handleTrackPublished(
	sess *livekitSession,
	logger *slog.Logger,
	publication *lksdk.RemoteTrackPublication,
	rp *lksdk.RemoteParticipant,
) {
	if publication == nil || rp == nil {
		return
	}
	logger.Info("Track published",
		slog.String("participant", strings.TrimSpace(rp.Identity())),
		slog.String("participantSID", rp.SID()),
		slog.String("trackSID", publication.SID()),
		slog.String("trackName", publication.Name()),
		slog.String("trackKind", string(publication.Kind())),
		slog.String("trackSource", publication.Source().String()),
		slog.Bool("trackMuted", publication.IsMuted()),
		slog.Bool("trackEnabled", publication.IsEnabled()),
		slog.String("trackMime", publication.MimeType()),
	)
	if publication.Kind() != lksdk.TrackKindAudio || !sess.captureEnabled {
		return
	}
	if err := publication.SetSubscribed(true); err != nil {
		logger.Warn("Failed to subscribe to newly published audio track",
			slog.String("participant", strings.TrimSpace(rp.Identity())),
			slog.String("trackSID", publication.SID()),
			slog.String("error", err.Error()),
		)
	}
}

func (e *LiveKitAgent) handleTrackSubscribed(
	ctx context.Context,
	sess *livekitSession,
	agentID string,
	sourceFilters []string,
	logger *slog.Logger,
	track *webrtc.TrackRemote,
	publication *lksdk.RemoteTrackPublication,
	rp *lksdk.RemoteParticipant,
) {
	if track == nil || rp == nil || track.Kind() != webrtc.RTPCodecTypeAudio {
		return
	}
	if !sess.captureEnabled {
		return
	}
	codec := track.Codec()
	logger.Info("Audio track subscribed",
		slog.String("participant", strings.TrimSpace(rp.Identity())),
		slog.String("participantSID", rp.SID()),
		slog.String("trackSID", publication.SID()),
		slog.String("trackName", publication.Name()),
		slog.String("trackKind", string(publication.Kind())),
		slog.String("trackSource", publication.Source().String()),
		slog.Bool("trackMuted", publication.IsMuted()),
		slog.Bool("trackEnabled", publication.IsEnabled()),
		slog.String("pubMime", publication.MimeType()),
		slog.String("codecMime", codec.MimeType),
		slog.Int("codecClockRate", int(codec.ClockRate)),
		slog.Int("codecChannels", int(codec.Channels)),
		slog.String("streamID", track.StreamID()),
		slog.String("trackID", track.ID()),
	)
	identity := strings.TrimSpace(rp.Identity())
	if !e.shouldCaptureParticipant(identity, agentID) {
		logger.Info("Skipping participant due to agent identity rules",
			slog.String("participant", identity),
			slog.String("agentIdentity", agentID),
			slog.String("agentIdentityPrefix", e.cfg.AgentIdentityPrefix),
		)
		return
	}
	if !matchFilter(sourceFilters, identity) {
		e.logDebug(ctx, logger, "skipping participant due to source allowlist",
			slog.String("participant", identity),
			slog.Any("filters", sourceFilters),
		)
		return
	}
	logger.Info("Subscribing to participant track",
		slog.String("participant", identity),
		slog.String("track", publication.SID()),
	)
	sess.captureTrack(ctx, track, participantInfo{Identity: identity, SID: rp.SID()}, logger)
}

func enqueueChatMessage(sess *livekitSession, logger *slog.Logger, msg chatMessage) {
	select {
	case sess.chat <- msg:
	default:
		logger.Warn("Chat channel full, dropping message",
			slog.String("sender", msg.Participant.Identity),
		)
	}
}

func (e *LiveKitAgent) connectRoom(
	roomName string,
	agentID string,
	logger *slog.Logger,
	callbacks *lksdk.RoomCallback,
) (*lksdk.Room, error) {
	logger.Info("Connecting to LiveKit",
		slog.String("room", roomName),
		slog.String("livekitURL", e.cfg.LiveKitURL),
	)
	room, err := lksdk.ConnectToRoom(e.cfg.LiveKitURL, lksdk.ConnectInfo{
		APIKey:              e.apiKey,
		APISecret:           e.apiSecret,
		RoomName:            roomName,
		ParticipantIdentity: agentID,
	}, callbacks)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to LiveKit: %w", err)
	}
	return room, nil
}

func (e *LiveKitAgent) registerChatStreamHandler(
	room *lksdk.Room,
	sess *livekitSession,
	sourceFilters []string,
	agentID string,
	logger *slog.Logger,
) error {
	if !sess.chatEnabled || usesCustomChatPackets(sess.chatTopic) {
		return nil
	}
	chatTopic := normalizedChatTopic(sess.chatTopic)
	if err := room.RegisterTextStreamHandler(
		chatTopic,
		func(reader *lksdk.TextStreamReader, participantIdentity string) {
			senderIdentity := strings.TrimSpace(participantIdentity)
			if !e.shouldCaptureParticipant(senderIdentity, agentID) {
				return
			}
			if !matchFilter(sourceFilters, senderIdentity) {
				return
			}
			text := strings.TrimSpace(reader.ReadAll())
			if text == "" {
				return
			}
			msg := chatMessage{
				Participant: participantInfo{Identity: senderIdentity},
				Text:        text,
				Topic:       chatTopic,
				Timestamp:   time.Now().UTC(),
			}
			logger.Info("Received chat text stream",
				slog.String("sender", msg.Participant.Identity),
				slog.String("topic", msg.Topic),
				slog.Int("textLen", len(msg.Text)),
			)
			enqueueChatMessage(sess, logger, msg)
		},
	); err != nil {
		return fmt.Errorf("failed to register chat text stream handler: %w", err)
	}
	return nil
}

type roomSnapshot struct {
	ids                  []string
	trackSummaries       []map[string]any
	participantSummaries []map[string]any
	localPubSummaries    []map[string]any
}

func (e *LiveKitAgent) startParticipantSnapshotLoop(
	ctx context.Context,
	room *lksdk.Room,
	inputs *stepInputs,
	logger *slog.Logger,
) {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				e.logRoomSnapshot(logger, inputs.Room.Name, collectRoomSnapshot(room))
			}
		}
	}()
}

func collectRoomSnapshot(room *lksdk.Room) roomSnapshot {
	participants := room.GetRemoteParticipants()
	snapshot := roomSnapshot{
		ids:                  make([]string, 0, len(participants)),
		trackSummaries:       make([]map[string]any, 0, 8),
		participantSummaries: make([]map[string]any, 0, len(participants)),
		localPubSummaries:    make([]map[string]any, 0, 4),
	}

	for _, rp := range participants {
		if rp == nil {
			continue
		}
		identity := strings.TrimSpace(rp.Identity())
		if identity == "" {
			identity = "<empty>"
		}
		snapshot.ids = append(snapshot.ids, identity)
		audioTrackCount := 0
		totalTracks := 0
		for _, pub := range rp.TrackPublications() {
			remotePub, ok := pub.(*lksdk.RemoteTrackPublication)
			if !ok || remotePub == nil {
				continue
			}
			totalTracks++
			if remotePub.Kind() == lksdk.TrackKindAudio {
				audioTrackCount++
			}
			snapshot.trackSummaries = append(snapshot.trackSummaries, map[string]any{
				"participant":    identity,
				"participantSID": rp.SID(),
				"trackSID":       remotePub.SID(),
				"trackName":      remotePub.Name(),
				"trackKind":      remotePub.Kind().String(),
				"trackSource":    remotePub.Source().String(),
				"trackMuted":     remotePub.IsMuted(),
				"trackEnabled":   remotePub.IsEnabled(),
				"trackMime":      remotePub.MimeType(),
				"subscribed":     remotePub.IsSubscribed(),
			})
		}
		snapshot.participantSummaries = append(snapshot.participantSummaries, map[string]any{
			"participant":     identity,
			"participantSID":  rp.SID(),
			"trackTotal":      totalTracks,
			"audioTrackTotal": audioTrackCount,
			"hasAudioTracks":  audioTrackCount > 0,
			"isSpeaking":      rp.IsSpeaking(),
		})
	}

	if lp := room.LocalParticipant; lp != nil {
		for _, pub := range lp.TrackPublications() {
			snapshot.localPubSummaries = append(snapshot.localPubSummaries, map[string]any{
				"participant": strings.TrimSpace(lp.Identity()),
				"trackSID":    pub.SID(),
				"trackName":   pub.Name(),
				"trackKind":   pub.Kind().String(),
				"trackSource": pub.Source().String(),
				"trackMuted":  pub.IsMuted(),
				"trackMime":   pub.MimeType(),
			})
		}
	}
	return snapshot
}

func (e *LiveKitAgent) logRoomSnapshot(
	logger *slog.Logger,
	roomName string,
	snapshot roomSnapshot,
) {
	logger.Info("LiveKit participants snapshot",
		slog.String("room", roomName),
		slog.Int("count", len(snapshot.ids)),
		slog.Any("participants", snapshot.ids),
	)
	if len(snapshot.participantSummaries) > 0 {
		logger.Info("LiveKit participant track summary",
			slog.String("room", roomName),
			slog.Any("participants", snapshot.participantSummaries),
		)
	}
	if len(snapshot.trackSummaries) > 0 {
		logger.Info("LiveKit tracks snapshot",
			slog.String("room", roomName),
			slog.Int("trackCount", len(snapshot.trackSummaries)),
			slog.Any("tracks", snapshot.trackSummaries),
		)
	}
	if len(snapshot.localPubSummaries) > 0 {
		logger.Info("LiveKit local tracks snapshot",
			slog.String("room", roomName),
			slog.Any("tracks", snapshot.localPubSummaries),
		)
	}
}

func (e *LiveKitAgent) publishPlaybackTrack(
	room *lksdk.Room,
	sess *livekitSession,
) error {
	if !sess.playbackEnabled {
		return nil
	}
	sampleRate := e.cfg.Capture.SampleRate
	if sampleRate <= 0 {
		sampleRate = lkmedia.DefaultOpusSampleRate
	}
	channels := e.cfg.Capture.Channels
	if channels <= 0 {
		channels = 1
	} else if channels > 2 {
		channels = 2
	}

	playback, err := lkmedia.NewPCMLocalTrack(sampleRate, channels, lklogger.GetLogger())
	if err != nil {
		return fmt.Errorf("failed to create playback track: %w", err)
	}
	if _, err := room.LocalParticipant.PublishTrack(
		playback,
		&lksdk.TrackPublicationOptions{Name: e.cfg.PlaybackTrackName},
	); err != nil {
		return fmt.Errorf("failed to publish playback track: %w", err)
	}
	sess.playback = playback
	return nil
}

func subscribeExistingAudioParticipants(room *lksdk.Room, logger *slog.Logger) {
	for _, rp := range room.GetRemoteParticipants() {
		subscribeAudioPublications(logger, rp)
	}
}

func (e *LiveKitAgent) forwardAudio(
	ctx context.Context,
	session *livekitSession,
	inputs *stepInputs,
	out chan<- sdkengram.StreamMessage,
	logger *slog.Logger,
) error {
	logger.Info("Starting audio forward loop",
		slog.String("room", inputs.Room.Name),
		slog.String("agentIdentity", session.agentIdentity),
	)
	firstSampleLogged := make(map[string]bool)
	forwardedStatsLogged := make(map[string]int)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case sample, ok := <-session.audio:
			if !ok {
				logger.Info("Audio channel closed; exiting forward loop")
				return nil
			}
			e.logForwardedSample(ctx, logger, sample, len(sample.Data))
			audio := audioBuffer{
				Encoding:   session.capture.Encoding,
				SampleRate: session.capture.SampleRate,
				Channels:   session.capture.Channels,
				Data:       sample.Data,
			}
			if !firstSampleLogged[sample.Participant.Identity] {
				firstSampleLogged[sample.Participant.Identity] = true
				logger.Info("First sample forwarded",
					slog.String("participant", sample.Participant.Identity),
					slog.String("encoding", audio.Encoding),
					slog.Int("sampleRate", audio.SampleRate),
					slog.Int("channels", audio.Channels),
					slog.Int("bytes", len(audio.Data)),
				)
			}

			// Forward raw PCM in native AudioFrame format.
			pcmData := append([]byte(nil), audio.Data...)

			msg := sdkengram.StreamMessage{
				Audio: &sdkengram.AudioFrame{
					PCM:          pcmData,
					SampleRateHz: int32(audio.SampleRate),
					Channels:     int32(audio.Channels),
					Codec:        audio.Encoding,
					Timestamp:    sample.Timestamp.Sub(time.Time{}),
				},
				Metadata: map[string]string{
					"source":          "livekit",
					"provider":        "livekit",
					"type":            transportAudioType,
					"room.name":       inputs.Room.Name,
					"room.sid":        inputs.Room.SID,
					"participant.id":  sample.Participant.Identity,
					"participant.sid": sample.Participant.SID,
					"timestamp":       sample.Timestamp.Format(time.RFC3339Nano),
				},
			}
			if forwardedStatsLogged[sample.Participant.Identity] < 200 {
				rms, peak := pcmStatsInt16Bytes(pcmData)
				logger.Info("INGRESS: Forward PCM stats",
					"participant", sample.Participant.Identity,
					"rms", fmt.Sprintf("%.2f", rms),
					"peak", peak,
					"bytes", len(pcmData))
				forwardedStatsLogged[sample.Participant.Identity]++
			}
			logger.Info("INGRESS: Created StreamMessage with Audio",
				"hasAudio", msg.Audio != nil,
				"pcmLen", len(pcmData),
				"channels", msg.Audio.Channels,
				"sampleRate", msg.Audio.SampleRateHz)
			select {
			case out <- msg:
				e.logDebug(ctx, logger, "Forwarded audio packet",
					slog.String("participant", sample.Participant.Identity),
					slog.Int("bytes", len(pcmData)),
				)
			case <-ctx.Done():
				return ctx.Err()
			case <-session.done:
				return nil
			}
		}
	}
}

func (e *LiveKitAgent) forwardChat(
	ctx context.Context,
	session *livekitSession,
	inputs *stepInputs,
	out chan<- sdkengram.StreamMessage,
	logger *slog.Logger,
) error {
	logger.Info("Starting chat forward loop",
		slog.String("room", inputs.Room.Name),
		slog.String("topic", session.chatTopic),
	)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-session.chat:
			if !ok {
				logger.Info("Chat channel closed; exiting forward loop")
				return nil
			}
			logger.Info("Forwarding chat message",
				slog.String("sender", msg.Participant.Identity),
				slog.Int("textLen", len(msg.Text)),
			)
			payload, err := json.Marshal(map[string]string{
				"text":   msg.Text,
				"sender": msg.Participant.Identity,
				"topic":  msg.Topic,
			})
			if err != nil {
				logger.Warn("Failed to marshal chat message", slog.Any("error", err))
				continue
			}
			streamMsg := sdkengram.StreamMessage{
				Payload: payload,
				Metadata: map[string]string{
					"source":          "livekit",
					"provider":        "livekit",
					"type":            transportChatMessageType,
					"room.name":       inputs.Room.Name,
					"room.sid":        inputs.Room.SID,
					"participant.id":  msg.Participant.Identity,
					"participant.sid": msg.Participant.SID,
					"timestamp":       msg.Timestamp.Format(time.RFC3339Nano),
				},
			}
			select {
			case out <- streamMsg:
			case <-ctx.Done():
				return ctx.Err()
			case <-session.done:
				return nil
			}
		}
	}
}

func subscribeAudioPublications(logger *slog.Logger, rp *lksdk.RemoteParticipant) {
	if rp == nil || logger == nil {
		return
	}
	identity := strings.TrimSpace(rp.Identity())
	for _, pub := range rp.TrackPublications() {
		remotePub, ok := pub.(*lksdk.RemoteTrackPublication)
		if !ok || remotePub == nil {
			continue
		}
		if remotePub.Kind() != lksdk.TrackKindAudio {
			continue
		}
		if err := remotePub.SetSubscribed(true); err != nil {
			logger.Warn("Failed to subscribe to participant audio track",
				slog.String("participant", identity),
				slog.String("trackSID", remotePub.SID()),
				slog.String("error", err.Error()))
		} else {
			logger.Info("Subscribed to participant audio track",
				slog.String("participant", identity),
				slog.String("trackSID", remotePub.SID()))
		}
	}
}

func (e *LiveKitAgent) consumeDirectives(
	ctx context.Context,
	first *sdkengram.InboundMessage,
	in <-chan sdkengram.InboundMessage,
	session *livekitSession,
	logger *slog.Logger,
) error {
	if err := e.applyBootstrapDirective(ctx, first, session, logger); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-in:
			if !ok {
				return nil
			}
			if err := e.handleDirectiveMessage(ctx, msg, session, logger); err != nil {
				return err
			}
		}
	}
}

func (e *LiveKitAgent) applyBootstrapDirective(
	ctx context.Context,
	first *sdkengram.InboundMessage,
	session *livekitSession,
	logger *slog.Logger,
) error {
	if first == nil {
		return nil
	}

	payload := messagePayloadBytes(*first)
	logger.Info("Applying bootstrap directive",
		slog.Int("payloadBytes", len(payload)),
		slog.Any("metadata", first.Metadata),
	)
	if err := e.applyDirective(ctx, payload, session, logger); err != nil {
		if errors.Is(err, context.Canceled) {
			first.Done()
		}
		return err
	}
	first.Done()
	return nil
}

func (e *LiveKitAgent) handleDirectiveMessage(
	ctx context.Context,
	msg sdkengram.InboundMessage,
	session *livekitSession,
	logger *slog.Logger,
) error {
	if isHeartbeat(msg.Metadata) {
		msg.Done()
		return nil
	}

	if handled, err := e.handleDirectiveAudioFrame(ctx, msg, session, logger); handled {
		return err
	}
	if e.handleDirectiveChatResponse(msg, session, logger) {
		return nil
	}

	payload := messagePayloadBytes(msg)
	logger.Info("Directive channel message",
		slog.Int("payloadBytes", len(payload)),
		slog.Any("metadata", msg.Metadata),
	)
	if len(payload) == 0 {
		e.logDebug(ctx, logger, "Ignoring empty payload", slog.Any("metadata", msg.Metadata))
		msg.Done()
		return nil
	}
	if err := e.applyDirective(ctx, payload, session, logger); err != nil {
		if errors.Is(err, context.Canceled) {
			msg.Done()
		}
		return err
	}
	msg.Done()
	return nil
}

func (e *LiveKitAgent) handleDirectiveAudioFrame(
	ctx context.Context,
	msg sdkengram.InboundMessage,
	session *livekitSession,
	logger *slog.Logger,
) (bool, error) {
	if msg.Audio == nil || len(msg.Audio.PCM) == 0 {
		return false, nil
	}

	logger.Info("Received AudioFrame for playback",
		slog.Int("bytes", len(msg.Audio.PCM)),
		slog.Int("sampleRate", int(msg.Audio.SampleRateHz)),
		slog.Int("channels", int(msg.Audio.Channels)),
		slog.String("codec", msg.Audio.Codec),
	)
	audio := &audioBuffer{
		Encoding:   strings.ToLower(msg.Audio.Codec),
		SampleRate: int(msg.Audio.SampleRateHz),
		Channels:   int(msg.Audio.Channels),
		Data:       msg.Audio.PCM,
	}
	normalizePlaybackBuffer(audio, session.capture)
	logger.Info("Queueing AudioFrame for playback",
		slog.String("encoding", audio.Encoding),
		slog.Int("sampleRate", audio.SampleRate),
		slog.Int("channels", audio.Channels),
		slog.Int("bytes", len(audio.Data)),
	)
	if err := session.sendAudio(ctx, audio); err != nil {
		return true, err
	}
	msg.Done()
	return true, nil
}

func (e *LiveKitAgent) handleDirectiveChatResponse(
	msg sdkengram.InboundMessage,
	session *livekitSession,
	logger *slog.Logger,
) bool {
	if !session.chatEnabled {
		return false
	}
	text, ok := extractChatResponseText(msg.Metadata, msg.Payload, msg.Binary)
	if !ok || text == "" {
		return false
	}

	logger.Info("Publishing chat response to LiveKit",
		slog.Int("textLen", len(text)),
		slog.String("topic", session.chatTopic),
	)
	if err := session.sendChatResponse(text); err != nil {
		logger.Warn("Failed to publish chat response", slog.Any("error", err))
	}
	msg.Done()
	return true
}

func (e *LiveKitAgent) applyDirective(
	ctx context.Context,
	payload []byte,
	session *livekitSession,
	logger *slog.Logger,
) error {
	if len(payload) == 0 {
		return nil
	}
	var dir directive
	if err := json.Unmarshal(payload, &dir); err != nil {
		return fmt.Errorf("invalid downstream directive: %w", err)
	}
	dirType := strings.ToLower(dir.Type)
	audioBytes := 0
	if dir.Audio != nil {
		audioBytes = len(dir.Audio.Data)
	}
	logger.Info("Applying downstream directive",
		slog.String("type", dirType),
		slog.Int("audioBytes", audioBytes),
	)
	e.logDirective(ctx, logger, &dir, audioBytes)
	switch {
	case isAudioDirectiveType(dirType):
		if dir.Audio == nil || len(dir.Audio.Data) == 0 {
			return nil
		}
		audio := &audioBuffer{
			Encoding:   strings.TrimSpace(dir.Audio.Encoding),
			SampleRate: dir.Audio.SampleRate.Int(session.capture.SampleRate),
			Channels:   dir.Audio.Channels.Int(session.capture.Channels),
			Data:       dir.Audio.Data,
		}
		normalizePlaybackBuffer(audio, session.capture)
		logger.Info("Prepared playback buffer",
			slog.String("encoding", strings.ToLower(audio.Encoding)),
			slog.Int("sampleRate", audio.SampleRate),
			slog.Int("channels", audio.Channels),
			slog.Int("bytes", len(audio.Data)),
		)
		if err := session.sendAudio(ctx, audio); err != nil {
			return err
		}
		logger.Info("Queued audio for playback",
			slog.String("encoding", strings.ToLower(audio.Encoding)),
			slog.Int("sampleRate", audio.SampleRate),
			slog.Int("channels", audio.Channels),
			slog.Int("bytes", len(audio.Data)),
		)
		return nil
	case dirType == "stop", dirType == "livekit.session.end":
		session.Close()
		return context.Canceled
	default:
		logger.Warn("ignoring unsupported directive", slog.String("type", dir.Type))
		return nil
	}
}

func isChatResponseType(t string) bool {
	t = strings.ToLower(strings.TrimSpace(t))
	return t == transportChatResponseType || t == transport.StreamTypeOpenAIChat || t == "chat.response" || t == "chat"
}

func isAudioDirectiveType(dirType string) bool {
	if dirType == "" {
		return true
	}
	switch dirType {
	case "audio", "speak", transportAudioType:
		return true
	}
	if strings.HasSuffix(dirType, ".audio.v1") {
		return true
	}
	if strings.HasSuffix(dirType, ".audio") {
		return true
	}
	return false
}

func extractChatResponseText(
	metadata map[string]string,
	payload []byte,
	binaryFrame *sdkengram.BinaryFrame,
) (string, bool) {
	if len(payload) == 0 && binaryFrame != nil {
		payload = binaryFrame.Payload
	}
	if len(payload) == 0 {
		return "", false
	}
	type chatPayload struct {
		Type  string          `json:"type,omitempty"`
		Text  string          `json:"text,omitempty"`
		Audio json.RawMessage `json:"audio,omitempty"`
	}
	var decoded chatPayload
	if err := json.Unmarshal(payload, &decoded); err == nil {
		if len(decoded.Audio) == 0 {
			text := strings.TrimSpace(decoded.Text)
			if isChatResponseType(metadata["type"]) && text != "" {
				return text, true
			}
			if isChatResponseType(decoded.Type) && text != "" {
				return text, true
			}
			if text != "" && strings.TrimSpace(decoded.Type) == "" && strings.TrimSpace(metadata["type"]) == "" {
				return text, true
			}
		}
	}
	if isChatResponseType(metadata["type"]) {
		return string(payload), true
	}
	return "", false
}

func (e *LiveKitAgent) emitSessionEvent(
	ctx context.Context,
	out chan<- sdkengram.StreamMessage,
	inputs *stepInputs,
	agentID, eventType string,
) error {
	sessionID := ""
	if inputs != nil {
		sessionID = strings.TrimSpace(inputs.SessionID)
	}
	evt := sessionEvent{
		Kind:        "hook",
		Type:        eventType,
		Room:        inputs.Room,
		Participant: inputs.Participant,
		Agent:       map[string]string{"identity": agentID},
		Event:       inputs.Event,
		Policy:      inputs.Policy,
		Hook: sessionHook{
			Version:   "v1",
			Event:     eventType,
			Source:    "livekit-bridge",
			SessionID: sessionID,
		},
		Timestamp: time.Now().UTC(),
	}
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}
	msg := sdkengram.StreamMessage{
		Kind:    "hook",
		Payload: payload,
		Binary: &sdkengram.BinaryFrame{
			Payload:  payload,
			MimeType: "application/json",
		},
		Metadata: map[string]string{
			"source":      "livekit",
			"provider":    "livekit",
			"type":        eventType,
			"kind":        "hook",
			"hook.event":  eventType,
			"hook.source": "livekit-bridge",
		},
	}
	select {
	case out <- msg:
		e.emitSessionSignal(ctx, &evt, agentID, sessionID)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *LiveKitAgent) emitSessionSignal(ctx context.Context, evt *sessionEvent, agentID, sessionID string) {
	if evt == nil {
		return
	}
	payload := map[string]any{
		"type": evt.Type,
	}
	if evt.Room.Name != "" || evt.Room.SID != "" {
		payload["room"] = map[string]string{
			"name": evt.Room.Name,
			"sid":  evt.Room.SID,
		}
	}
	if evt.Participant.Identity != "" || evt.Participant.SID != "" {
		payload["participant"] = map[string]string{
			"identity": evt.Participant.Identity,
			"sid":      evt.Participant.SID,
		}
	}
	if agentID != "" {
		payload["agentIdentity"] = agentID
	}
	if sessionID != "" {
		payload["sessionId"] = sessionID
	}
	logger := sdk.LoggerFromContext(ctx)
	if err := emitSignalFunc(ctx, evt.Type, payload); err != nil && !errors.Is(err, sdk.ErrSignalsUnavailable) {
		logger.Warn("Failed to emit LiveKit session signal", "type", evt.Type, "error", err)
	}
}

// --- Helpers ---

func normalizeConfig(cfg config.Config) config.Config {
	if cfg.AgentIdentityPrefix == "" {
		cfg.AgentIdentityPrefix = "bubu-agent"
	}
	if cfg.PlaybackTrackName == "" {
		cfg.PlaybackTrackName = "agent-playback"
	}
	if cfg.Capture.Encoding == "" {
		cfg.Capture.Encoding = "pcm"
	}
	if cfg.Capture.SampleRate == 0 {
		cfg.Capture.SampleRate = 48000
	}
	if cfg.Capture.Channels == 0 {
		cfg.Capture.Channels = 1
	}
	if cfg.Capture.Enabled == nil {
		cfg.Capture.Enabled = boolPtr(true)
	}
	if cfg.Playback.Enabled == nil {
		cfg.Playback.Enabled = boolPtr(true)
	}
	if strings.TrimSpace(cfg.Chat.Topic) == "" {
		cfg.Chat.Topic = standardLiveKitChatTopic
	}
	return cfg
}

type streamBytesSource int

const (
	streamBytesSourceNone streamBytesSource = iota
	streamBytesSourceInputs
	streamBytesSourcePayload
	streamBytesSourceBinary
)

func structuredStreamBytes(msg sdkengram.InboundMessage) ([]byte, streamBytesSource) {
	if len(msg.Inputs) > 0 {
		return msg.Inputs, streamBytesSourceInputs
	}
	if len(msg.Payload) > 0 {
		return msg.Payload, streamBytesSourcePayload
	}
	if msg.Binary != nil && len(msg.Binary.Payload) > 0 {
		return msg.Binary.Payload, streamBytesSourceBinary
	}
	return nil, streamBytesSourceNone
}

func messagePayloadBytes(msg sdkengram.InboundMessage) []byte {
	if len(msg.Payload) > 0 {
		return msg.Payload
	}
	if msg.Binary != nil && len(msg.Binary.Payload) > 0 {
		return msg.Binary.Payload
	}
	return nil
}

func hasBootstrapRoom(inputs *stepInputs) bool {
	return inputs != nil && strings.TrimSpace(inputs.Room.Name) != ""
}

func mergeStepInputs(base, overlay *stepInputs) *stepInputs {
	switch {
	case base == nil && overlay == nil:
		return nil
	case base == nil:
		cloned := cloneStepInputs(overlay)
		return &cloned
	case overlay == nil:
		cloned := cloneStepInputs(base)
		return &cloned
	}

	merged := cloneStepInputs(base)
	if v := strings.TrimSpace(overlay.Room.Name); v != "" {
		merged.Room.Name = v
	}
	if v := strings.TrimSpace(overlay.Room.SID); v != "" {
		merged.Room.SID = v
	}
	if v := strings.TrimSpace(overlay.Participant.Identity); v != "" {
		merged.Participant.Identity = v
	}
	if v := strings.TrimSpace(overlay.Participant.SID); v != "" {
		merged.Participant.SID = v
	}
	if v := strings.TrimSpace(overlay.AgentIdentity); v != "" {
		merged.AgentIdentity = v
	}
	if v := strings.TrimSpace(overlay.SessionID); v != "" {
		merged.SessionID = v
	}
	if len(overlay.SourceAllowlist) > 0 {
		merged.SourceAllowlist = append([]string(nil), overlay.SourceAllowlist...)
	}
	merged.Event = mergeStringAnyMaps(merged.Event, overlay.Event)
	merged.Policy = mergeStringAnyMaps(merged.Policy, overlay.Policy)
	return &merged
}

func cloneStepInputs(src *stepInputs) stepInputs {
	if src == nil {
		return stepInputs{}
	}
	cloned := *src
	if len(src.SourceAllowlist) > 0 {
		cloned.SourceAllowlist = append([]string(nil), src.SourceAllowlist...)
	}
	cloned.Event = mergeStringAnyMaps(nil, src.Event)
	cloned.Policy = mergeStringAnyMaps(nil, src.Policy)
	return cloned
}

func mergeStringAnyMaps(base, overlay map[string]any) map[string]any {
	switch {
	case len(base) == 0 && len(overlay) == 0:
		return nil
	case len(base) == 0:
		merged := make(map[string]any, len(overlay))
		for k, v := range overlay {
			merged[k] = v
		}
		return merged
	case len(overlay) == 0:
		merged := make(map[string]any, len(base))
		for k, v := range base {
			merged[k] = v
		}
		return merged
	}
	merged := make(map[string]any, len(base)+len(overlay))
	for k, v := range base {
		merged[k] = v
	}
	for k, v := range overlay {
		merged[k] = v
	}
	return merged
}

func stripConsumedBootstrapBytes(msg sdkengram.InboundMessage, source streamBytesSource) sdkengram.InboundMessage {
	switch source {
	case streamBytesSourcePayload:
		msg.Payload = nil
	case streamBytesSourceBinary:
		msg.Binary = nil
	}
	return msg
}

func boolPtr(v bool) *bool {
	return &v
}

func enabledFlag(v *bool, defaultValue bool) bool {
	if v == nil {
		return defaultValue
	}
	return *v
}

func normalizedChatTopic(topic string) string {
	topic = strings.TrimSpace(topic)
	if topic == "" {
		return standardLiveKitChatTopic
	}
	return topic
}

func usesCustomChatPackets(topic string) bool {
	return normalizedChatTopic(topic) != standardLiveKitChatTopic
}

func decodeChatPacket(data lksdk.DataPacket, params lksdk.DataReceiveParams, expectedTopic string) (chatMessage, bool) {
	msg := chatMessage{
		Participant: participantInfo{Identity: strings.TrimSpace(params.SenderIdentity)},
		Timestamp:   time.Now().UTC(),
	}
	if params.Sender != nil {
		msg.Participant.SID = params.Sender.SID()
	}

	switch packet := data.(type) {
	case *lksdk.UserDataPacket:
		if packet == nil {
			return chatMessage{}, false
		}
		if !usesCustomChatPackets(expectedTopic) {
			return chatMessage{}, false
		}
		if strings.TrimSpace(packet.Topic) != normalizedChatTopic(expectedTopic) {
			return chatMessage{}, false
		}
		text := strings.TrimSpace(string(packet.Payload))
		if text == "" {
			return chatMessage{}, false
		}
		msg.Text = text
		msg.Topic = strings.TrimSpace(packet.Topic)
		return msg, true
	case *lkproto.ChatMessage:
		if packet == nil {
			return chatMessage{}, false
		}
		if usesCustomChatPackets(expectedTopic) {
			return chatMessage{}, false
		}
		text := strings.TrimSpace(packet.GetMessage())
		if text == "" {
			return chatMessage{}, false
		}
		msg.Text = text
		msg.Topic = standardLiveKitChatTopic
		if ts := packet.GetTimestamp(); ts > 0 {
			msg.Timestamp = time.UnixMilli(ts).UTC()
		}
		return msg, true
	default:
		return chatMessage{}, false
	}
}

func buildOutgoingChatPacket(text, topic string, ts time.Time) lksdk.DataPacket {
	if usesCustomChatPackets(topic) {
		return &lksdk.UserDataPacket{
			Payload: []byte(text),
			Topic:   normalizedChatTopic(topic),
		}
	}
	return lksdk.ChatMessage(ts, text)
}

func inputsFromRuntime() (*stepInputs, error) {
	execData, err := runtime.LoadExecutionContextData()
	if err != nil {
		return nil, err
	}
	if execData == nil || len(execData.Inputs) == 0 {
		return nil, fmt.Errorf("runtime inputs not available")
	}
	payload, err := json.Marshal(execData.Inputs)
	if err != nil {
		return nil, err
	}
	var parsed stepInputs
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return nil, err
	}
	if parsed.Room.Name == "" {
		return nil, fmt.Errorf("room missing")
	}
	return &parsed, nil
}

func (e *LiveKitAgent) resolveAgentIdentity(inputs *stepInputs, info sdkengram.StoryInfo) string {
	if id := strings.TrimSpace(inputs.AgentIdentity); id != "" {
		return id
	}
	if runID := strings.TrimSpace(info.StepRunID); runID != "" {
		return sanitizeIdentity(fmt.Sprintf("%s-%s", e.cfg.AgentIdentityPrefix, runID))
	}
	if host, err := os.Hostname(); err == nil && strings.TrimSpace(host) != "" {
		return sanitizeIdentity(host)
	}
	base := sanitizeIdentity(inputs.Room.Name)
	if base == "" {
		base = "session"
	}
	return fmt.Sprintf("%s-%s", e.cfg.AgentIdentityPrefix, base)
}

func inputsFromConfig(cfg config.Config) (*stepInputs, error) {
	if strings.TrimSpace(cfg.LiveKitURL) == "" {
		return nil, fmt.Errorf("livekitURL missing in config")
	}
	if strings.TrimSpace(cfg.Room.Name) == "" {
		return nil, fmt.Errorf("room metadata missing in config")
	}
	inputs := &stepInputs{
		Room:            roomInfo{Name: cfg.Room.Name, SID: cfg.Room.SID},
		Participant:     participantInfo{Identity: cfg.Participant.Identity, SID: cfg.Participant.SID},
		Event:           map[string]any{},
		Policy:          map[string]any{},
		SessionID:       strings.TrimSpace(cfg.SessionID),
		SourceAllowlist: append([]string(nil), cfg.SourceAllowlist...),
	}
	if cfg.Event != nil {
		inputs.Event = cfg.Event
	}
	if cfg.Policy != nil {
		inputs.Policy = cfg.Policy
	}
	return inputs, nil
}

func sanitizeIdentity(v string) string {
	v = strings.ToLower(strings.TrimSpace(v))
	if v == "" {
		return "session"
	}
	var b strings.Builder
	for _, r := range v {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			continue
		}
		if r == '-' || r == '_' {
			b.WriteRune(r)
			continue
		}
		b.WriteRune('-')
	}
	return strings.Trim(b.String(), "-")
}

func ensureUniqueAgentIdentity(base string, info sdkengram.StoryInfo) string {
	base = strings.TrimSpace(base)
	if base == "" {
		return base
	}
	parts := make([]string, 0, 3)
	if v := strings.TrimSpace(info.StepRunID); v != "" {
		parts = append(parts, v)
	}
	if v := strings.TrimSpace(info.StepRunNamespace); v != "" {
		parts = append(parts, v)
	}
	if len(parts) == 0 {
		return base
	}
	suffix := shortHash(strings.Join(parts, "|"))
	if strings.HasSuffix(base, "-"+suffix) {
		return base
	}
	return fmt.Sprintf("%s-%s", base, suffix)
}

func shortHash(input string) string {
	sum := sha1.Sum([]byte(input))
	return hex.EncodeToString(sum[:])[:6]
}

func (e *LiveKitAgent) shouldCaptureParticipant(identity, agentID string) bool {
	identity = strings.TrimSpace(identity)
	if identity == "" {
		return false
	}
	lowerIdentity := strings.ToLower(identity)
	if agentID != "" && lowerIdentity == strings.ToLower(strings.TrimSpace(agentID)) {
		return false
	}
	prefix := strings.ToLower(strings.TrimSpace(e.cfg.AgentIdentityPrefix))
	if prefix != "" && strings.HasPrefix(lowerIdentity, prefix) {
		return false
	}
	return true
}

func matchFilter(filters []string, candidate string) bool {
	if len(filters) == 0 {
		return true
	}
	candidate = strings.ToLower(candidate)
	for _, raw := range filters {
		filter := strings.ToLower(strings.TrimSpace(raw))
		switch {
		case filter == "":
			continue
		case filter == "*":
			return true
		case strings.HasSuffix(filter, "*"):
			prefix := strings.TrimSuffix(filter, "*")
			if strings.HasPrefix(candidate, prefix) {
				return true
			}
		default:
			if candidate == filter {
				return true
			}
		}
	}
	return false
}

func (e *LiveKitAgent) storageManager(ctx context.Context) *storage.StorageManager {
	e.storageOnce.Do(func() {
		sm, err := storage.SharedManager(ctx)
		if err != nil {
			e.storageErr = err
			return
		}
		e.storage = sm
	})
	if e.storageErr != nil {
		return nil
	}
	return e.storage
}

type flexibleInt struct {
	value int
}

func (f *flexibleInt) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || string(data) == "null" {
		f.value = 0
		return nil
	}
	if data[0] == '"' {
		var raw string
		if err := json.Unmarshal(data, &raw); err != nil {
			return err
		}
		raw = strings.TrimSpace(raw)
		if raw == "" {
			f.value = 0
			return nil
		}
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return fmt.Errorf("invalid integer value %q", raw)
		}
		f.value = parsed
		return nil
	}
	var parsed int
	if err := json.Unmarshal(data, &parsed); err != nil {
		return err
	}
	f.value = parsed
	return nil
}

func (f flexibleInt) Int(defaultValue int) int {
	if f.value != 0 {
		return f.value
	}
	return defaultValue
}

func isHeartbeat(meta map[string]string) bool {
	return meta != nil && meta["bubu-heartbeat"] == "true"
}

// --- Types ---

type roomInfo struct {
	Name string `json:"name"`
	SID  string `json:"sid"`
}

type participantInfo struct {
	Identity string `json:"identity"`
	SID      string `json:"sid"`
}

type stepInputs struct {
	Room            roomInfo        `json:"room"`
	Participant     participantInfo `json:"participant"`
	Event           map[string]any  `json:"event"`
	Policy          map[string]any  `json:"policy"`
	AgentIdentity   string          `json:"agentIdentity"`
	SessionID       string          `json:"sessionId"`
	SourceAllowlist []string        `json:"sourceAllowlist"`
}

type audioBuffer struct {
	Encoding   string                  `json:"encoding"`
	SampleRate int                     `json:"sampleRate"`
	Channels   int                     `json:"channels"`
	Data       []byte                  `json:"data"`
	Storage    *media.StorageReference `json:"storage,omitempty"`
}

type directiveAudio struct {
	Encoding   string                  `json:"encoding"`
	SampleRate flexibleInt             `json:"sampleRate"`
	Channels   flexibleInt             `json:"channels"`
	Data       []byte                  `json:"data"`
	Storage    *media.StorageReference `json:"storage,omitempty"`
}

func normalizePlaybackBuffer(audio *audioBuffer, capture config.CaptureConfig) {
	if audio == nil {
		return
	}
	if audio.SampleRate <= 0 {
		audio.SampleRate = capture.SampleRate
	}
	if audio.SampleRate <= 0 {
		audio.SampleRate = lkmedia.DefaultOpusSampleRate
	}
	if audio.Channels <= 0 {
		audio.Channels = capture.Channels
	}
	if audio.Channels <= 0 {
		audio.Channels = 1
	}
	if audio.Encoding == "" {
		audio.Encoding = capture.Encoding
		if audio.Encoding == "" {
			audio.Encoding = audioEncodingPCM
		}
	}
}

type directive struct {
	Type  string          `json:"type"`
	Text  string          `json:"text,omitempty"`
	Audio *directiveAudio `json:"audio,omitempty"`
}

type sessionEvent struct {
	Kind        string            `json:"kind,omitempty"`
	Type        string            `json:"type"`
	Room        roomInfo          `json:"room"`
	Participant participantInfo   `json:"participant"`
	Agent       map[string]string `json:"agent"`
	Event       map[string]any    `json:"event,omitempty"`
	Policy      map[string]any    `json:"policy,omitempty"`
	Hook        sessionHook       `json:"hook"`
	Timestamp   time.Time         `json:"timestamp"`
}

type sessionHook struct {
	Version   string `json:"version,omitempty"`
	Event     string `json:"event"`
	Source    string `json:"source,omitempty"`
	SessionID string `json:"sessionId,omitempty"`
}

type audioSample struct {
	Participant participantInfo
	Data        []byte
	Timestamp   time.Time
}

type chatMessage struct {
	Participant participantInfo
	Text        string
	Topic       string
	Timestamp   time.Time
}

type livekitSession struct {
	room                *lksdk.Room
	playback            *lkmedia.PCMLocalTrack
	audio               chan audioSample
	chat                chan chatMessage
	chatEnabled         bool
	chatTopic           string
	captureEnabled      bool
	playbackEnabled     bool
	done                chan struct{}
	closeOnce           sync.Once
	wg                  sync.WaitGroup
	agentIdentity       string
	capture             config.CaptureConfig
	storage             *storage.StorageManager
	playbackStatsLogged int
}

func (s *livekitSession) captureTrack(
	ctx context.Context,
	track *webrtc.TrackRemote,
	participant participantInfo,
	logger *slog.Logger,
) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		codec := track.Codec()
		channels := s.capture.Channels
		if channels <= 0 {
			channels = 1
		} else if channels > 2 {
			channels = 2
		}

		writer := newPCMDispatchWriter(ctx, s.done, s.audio, participant, s.capture.SampleRate, channels)
		defer func() {
			_ = writer.Close()
		}()

		var opts []lkmedia.PCMRemoteTrackOption
		if s.capture.SampleRate > 0 {
			opts = append(opts, lkmedia.WithTargetSampleRate(s.capture.SampleRate))
		}
		opts = append(opts, lkmedia.WithTargetChannels(channels))

		logger.Info("Starting PCM capture",
			slog.String("participant", participant.Identity),
			slog.String("codecMime", codec.MimeType),
			slog.Int("codecClockRate", int(codec.ClockRate)),
			slog.Int("codecChannels", int(codec.Channels)),
			slog.Int("sampleRate", s.capture.SampleRate),
			slog.Int("channels", channels),
		)
		pcmTrack, err := lkmedia.NewPCMRemoteTrack(track, writer, opts...)
		if err != nil {
			logger.Error("failed to create PCM remote track",
				slog.Any("error", err),
				slog.String("participant", participant.Identity),
			)
			return
		}
		defer pcmTrack.Close()

		select {
		case <-ctx.Done():
		case <-s.done:
		case <-writer.closed:
		}
		logger.Info("Stopping PCM capture", slog.String("participant", participant.Identity))
	}()
}

func (s *livekitSession) sendAudio(ctx context.Context, buf *audioBuffer) error {
	if s.playback == nil || buf == nil {
		return nil
	}
	data, err := s.resolveAudioData(ctx, buf)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	samples := bytesToPCM16(data)
	if len(samples) == 0 {
		return nil
	}
	if s.playbackStatsLogged < 200 {
		sampleRate := buf.SampleRate
		if sampleRate <= 0 {
			sampleRate = s.capture.SampleRate
		}
		if sampleRate <= 0 {
			sampleRate = lkmedia.DefaultOpusSampleRate
		}
		channels := buf.Channels
		if channels <= 0 {
			channels = s.capture.Channels
		}
		if channels <= 0 {
			channels = 1
		}
		rms, peak := pcmStatsInt16Samples(samples)
		sdk.LoggerFromContext(ctx).Info("PLAYBACK: PCM stats",
			slog.String("agentIdentity", s.agentIdentity),
			slog.Int("sampleRate", sampleRate),
			slog.Int("channels", channels),
			slog.String("rms", fmt.Sprintf("%.2f", rms)),
			slog.Int("peak", peak),
			slog.Int("samples", len(samples)),
			slog.Int("bytes", len(data)),
		)
		s.playbackStatsLogged++
	}
	return s.playback.WriteSample(samples)
}

func (s *livekitSession) resolveAudioData(ctx context.Context, buf *audioBuffer) ([]byte, error) {
	if buf == nil {
		return nil, nil
	}
	if len(buf.Data) > 0 {
		return buf.Data, nil
	}
	if buf.Storage == nil {
		return nil, nil
	}
	sm := s.storage
	if sm == nil {
		var err error
		sm, err = storage.SharedManager(ctx)
		if err != nil {
			return nil, fmt.Errorf("storage manager unavailable: %w", err)
		}
		s.storage = sm
	}
	data, err := media.ReadBlob(ctx, sm, buf.Storage)
	if err != nil {
		return nil, fmt.Errorf("failed to read audio from storage: %w", err)
	}
	return data, nil
}

func (s *livekitSession) sendChatResponse(text string) error {
	if s.room == nil || s.room.LocalParticipant == nil {
		return fmt.Errorf("room not connected")
	}
	if !usesCustomChatPackets(s.chatTopic) {
		s.room.LocalParticipant.SendText(text, lksdk.StreamTextOptions{
			Topic: normalizedChatTopic(s.chatTopic),
		})
		return nil
	}
	opts := make([]lksdk.DataPublishOption, 0, 2)
	opts = append(opts,
		lksdk.WithDataPublishReliable(true),
	)
	opts = append(opts, lksdk.WithDataPublishTopic(normalizedChatTopic(s.chatTopic)))
	return s.room.LocalParticipant.PublishDataPacket(
		buildOutgoingChatPacket(text, s.chatTopic, time.Now().UTC()),
		opts...,
	)
}

func (s *livekitSession) Close() {
	s.closeOnce.Do(func() {
		close(s.done)
		if s.room != nil {
			s.room.Disconnect()
		}
		if s.playback != nil {
			s.playback.ClearQueue()
			_ = s.playback.Close()
		}
		s.wg.Wait()
	})
}

type pcmDispatchWriter struct {
	ctx         context.Context
	done        <-chan struct{}
	output      chan<- audioSample
	participant participantInfo
	closed      chan struct{}
	sampleRate  int
	channels    int
	debugCount  int
}

func newPCMDispatchWriter(
	ctx context.Context,
	done <-chan struct{},
	output chan<- audioSample,
	participant participantInfo,
	sampleRate, channels int,
) *pcmDispatchWriter {
	return &pcmDispatchWriter{
		ctx:         ctx,
		done:        done,
		output:      output,
		participant: participant,
		closed:      make(chan struct{}),
		sampleRate:  sampleRate,
		channels:    channels,
	}
}

func (w *pcmDispatchWriter) WriteSample(sample mediasdk.PCM16Sample) error {
	if len(sample) == 0 {
		return nil
	}
	if w.debugCount < 200 {
		rms, peak := pcmStatsInt16Samples(sample)
		sdk.LoggerFromContext(w.ctx).Info("PCM capture stats",
			slog.String("participant", w.participant.Identity),
			slog.Int("sampleRate", w.sampleRate),
			slog.Int("channels", w.channels),
			slog.String("rms", fmt.Sprintf("%.2f", rms)),
			slog.Int("peak", peak),
			slog.Int("samples", len(sample)),
		)
		w.debugCount++
	}
	buf := make([]byte, sample.Size())
	if _, err := sample.CopyTo(buf); err != nil {
		return err
	}
	frame := audioSample{
		Participant: w.participant,
		Data:        buf,
		Timestamp:   time.Now().UTC(),
	}
	select {
	case w.output <- frame:
		return nil
	case <-w.ctx.Done():
		return w.ctx.Err()
	case <-w.done:
		return context.Canceled
	}
}

func pcmStatsInt16Samples(samples []int16) (rms float64, peak int) {
	if len(samples) == 0 {
		return 0, 0
	}
	var sumSquares float64
	for _, v := range samples {
		abs := int(v)
		if abs < 0 {
			abs = -abs
		}
		if abs > peak {
			peak = abs
		}
		sumSquares += float64(v) * float64(v)
	}
	rms = math.Sqrt(sumSquares / float64(len(samples)))
	return rms, peak
}

func pcmStatsInt16Bytes(data []byte) (rms float64, peak int) {
	if len(data) < 2 {
		return 0, 0
	}
	sampleCount := len(data) / 2
	var sumSquares float64
	for i := 0; i < sampleCount; i++ {
		v := int(int16(binary.LittleEndian.Uint16(data[i*2 : i*2+2])))
		abs := v
		if abs < 0 {
			abs = -abs
		}
		if abs > peak {
			peak = abs
		}
		sumSquares += float64(v) * float64(v)
	}
	rms = math.Sqrt(sumSquares / float64(sampleCount))
	return rms, peak
}

func (w *pcmDispatchWriter) Close() error {
	select {
	case <-w.closed:
	default:
		close(w.closed)
	}
	return nil
}

func bytesToPCM16(data []byte) mediasdk.PCM16Sample {
	length := len(data)
	if length < 2 {
		return nil
	}
	if length%2 != 0 {
		length--
		data = data[:length]
	}
	samples := make(mediasdk.PCM16Sample, length/2)
	for i := 0; i < len(samples); i++ {
		samples[i] = int16(binary.LittleEndian.Uint16(data[2*i:]))
	}
	return samples
}

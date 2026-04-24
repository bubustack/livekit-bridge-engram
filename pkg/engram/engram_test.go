package engram

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	sdkengram "github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/core/contracts"
	"github.com/bubustack/livekit-bridge-engram/pkg/config"
	"github.com/bubustack/tractatus/transport"
	lkproto "github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"
)

const (
	testCustomTopic   = "custom-topic"
	testUserOne       = "user-1"
	testRuntimeValue  = "runtime"
	testRuntimePrefix = "runtime-*"
)

func TestEmitSessionSignal(t *testing.T) {
	ctx := context.Background()
	evt := &sessionEvent{
		Type: "livekit.session.started",
		Room: roomInfo{
			Name: "demo-room",
			SID:  "RM123",
		},
		Participant: participantInfo{
			Identity: testUserOne,
			SID:      "PSID",
		},
	}

	var capturedKey string
	var capturedPayload map[string]any
	orig := emitSignalFunc
	emitSignalFunc = func(_ context.Context, key string, payload any) error {
		capturedKey = key
		if m, ok := payload.(map[string]any); ok {
			capturedPayload = m
		}
		return nil
	}
	t.Cleanup(func() {
		emitSignalFunc = orig
	})

	agent := &LiveKitAgent{}
	agent.emitSessionSignal(ctx, evt, "agent-42", "session-xyz")

	if capturedKey != evt.Type {
		t.Fatalf("expected key %s, got %s", evt.Type, capturedKey)
	}
	if capturedPayload["agentIdentity"] != "agent-42" {
		t.Fatalf("expected agentIdentity to be agent-42, got %v", capturedPayload["agentIdentity"])
	}
	if capturedPayload["sessionId"] != "session-xyz" {
		t.Fatalf("expected sessionId to be session-xyz, got %v", capturedPayload["sessionId"])
	}
	room, _ := capturedPayload["room"].(map[string]string)
	if room["name"] != "demo-room" || room["sid"] != "RM123" {
		t.Fatalf("unexpected room payload: %#v", room)
	}
}

func TestIsChatResponseType(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{transport.StreamTypeChatResponse, true},
		{"chat.response.v1", true},
		{"chat.response", true},
		{"chat", true},
		{"Chat.Response.V1", true},   // case-insensitive
		{" chat.response.v1 ", true}, // whitespace trimmed
		{"speech.audio.v1", false},
		{"", false},
		{"chat.message.v1", false},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := isChatResponseType(tt.input); got != tt.want {
				t.Errorf("isChatResponseType(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestNormalizeConfigChatDefaults(t *testing.T) {
	cfg := normalizeConfig(config.Config{})
	if cfg.Chat.Topic != standardLiveKitChatTopic {
		t.Errorf("expected default chat topic %q, got %q", standardLiveKitChatTopic, cfg.Chat.Topic)
	}
	if cfg.Capture.Enabled == nil || !*cfg.Capture.Enabled {
		t.Fatal("expected capture to default to enabled")
	}
	if cfg.Playback.Enabled == nil || !*cfg.Playback.Enabled {
		t.Fatal("expected playback to default to enabled")
	}

	cfg2 := normalizeConfig(config.Config{
		Chat: config.ChatConfig{Topic: testCustomTopic},
	})
	if cfg2.Chat.Topic != testCustomTopic {
		t.Errorf("expected custom chat topic, got %q", cfg2.Chat.Topic)
	}
}

func TestUsesCustomChatPackets(t *testing.T) {
	if usesCustomChatPackets("") {
		t.Fatal("expected empty topic to use standard chat")
	}
	if usesCustomChatPackets(standardLiveKitChatTopic) {
		t.Fatalf("expected topic %q to use standard chat", standardLiveKitChatTopic)
	}
	if !usesCustomChatPackets(testCustomTopic) {
		t.Fatal("expected nonstandard topic to use custom packets")
	}
}

func TestDecodeChatPacketStandardChatMessage(t *testing.T) {
	packet := lksdk.ChatMessage(time.UnixMilli(1712000000000), "Hello from room chat")
	msg, ok := decodeChatPacket(packet, lksdk.DataReceiveParams{SenderIdentity: testUserOne}, standardLiveKitChatTopic)
	if !ok {
		t.Fatal("expected standard chat packet to decode")
	}
	if msg.Text != "Hello from room chat" {
		t.Fatalf("unexpected text %q", msg.Text)
	}
	if msg.Participant.Identity != testUserOne {
		t.Fatalf("unexpected sender %q", msg.Participant.Identity)
	}
	if msg.Topic != standardLiveKitChatTopic {
		t.Fatalf("expected topic %q, got %q", standardLiveKitChatTopic, msg.Topic)
	}
}

func TestDecodeChatPacketCustomTopicUserData(t *testing.T) {
	msg, ok := decodeChatPacket(&lksdk.UserDataPacket{
		Payload: []byte("hello"),
		Topic:   testCustomTopic,
	}, lksdk.DataReceiveParams{SenderIdentity: "user-2"}, testCustomTopic)
	if !ok {
		t.Fatal("expected custom topic packet to decode")
	}
	if msg.Text != "hello" {
		t.Fatalf("unexpected text %q", msg.Text)
	}
	if msg.Topic != testCustomTopic {
		t.Fatalf("unexpected topic %q", msg.Topic)
	}
}

func TestDecodeChatPacketIgnoresUnexpectedTopic(t *testing.T) {
	if _, ok := decodeChatPacket(&lksdk.UserDataPacket{
		Payload: []byte("hello"),
		Topic:   "wrong-topic",
	}, lksdk.DataReceiveParams{SenderIdentity: "user-2"}, testCustomTopic); ok {
		t.Fatal("expected packet with unexpected topic to be ignored")
	}
}

func TestBuildOutgoingChatPacketUsesStandardChatMessageByDefault(t *testing.T) {
	packet := buildOutgoingChatPacket("bonjour", standardLiveKitChatTopic, time.UnixMilli(1712000000000))
	chat, ok := packet.(*lkproto.ChatMessage)
	if !ok {
		t.Fatalf("expected ChatMessage packet, got %T", packet)
	}
	if chat.GetMessage() != "bonjour" {
		t.Fatalf("unexpected chat message %q", chat.GetMessage())
	}
}

func TestBuildOutgoingChatPacketUsesUserDataForCustomTopic(t *testing.T) {
	packet := buildOutgoingChatPacket("bonjour", testCustomTopic, time.UnixMilli(1712000000000))
	user, ok := packet.(*lksdk.UserDataPacket)
	if !ok {
		t.Fatalf("expected UserDataPacket, got %T", packet)
	}
	if string(user.Payload) != "bonjour" {
		t.Fatalf("unexpected payload %q", string(user.Payload))
	}
	if user.Topic != testCustomTopic {
		t.Fatalf("unexpected topic %q", user.Topic)
	}
}

func TestExtractChatResponseTextFallsBackToPayloadText(t *testing.T) {
	payload, err := json.Marshal(map[string]any{
		"text": "Welcome to the demo",
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	text, ok := extractChatResponseText(map[string]string{}, payload, nil)
	if !ok {
		t.Fatal("expected payload-only chat text to be accepted")
	}
	if text != "Welcome to the demo" {
		t.Fatalf("unexpected text %q", text)
	}
}

func TestExtractChatResponseTextRejectsAudioPayload(t *testing.T) {
	payload, err := json.Marshal(map[string]any{
		"type":  "speech.audio.v1",
		"audio": map[string]any{"data": "abc"},
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	if text, ok := extractChatResponseText(map[string]string{}, payload, nil); ok || text != "" {
		t.Fatalf("expected audio payload to be ignored, got ok=%v text=%q", ok, text)
	}
}

func TestExtractChatResponseTextSupportsBinaryFallback(t *testing.T) {
	payload, err := json.Marshal(map[string]any{
		"type": "chat.response.v1",
		"text": "Binary path still works",
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	text, ok := extractChatResponseText(map[string]string{}, nil, &sdkengram.BinaryFrame{Payload: payload})
	if !ok {
		t.Fatal("expected binary chat text to be accepted")
	}
	if text != "Binary path still works" {
		t.Fatalf("unexpected text %q", text)
	}
}

func TestForwardChatMessage(t *testing.T) {
	agent := &LiveKitAgent{
		cfg: normalizeConfig(config.Config{
			Chat: config.ChatConfig{Enabled: true},
		}),
	}

	sess := &livekitSession{
		chat:        make(chan chatMessage, 8),
		chatEnabled: true,
		chatTopic:   "",
		done:        make(chan struct{}),
	}

	out := make(chan sdkengram.StreamMessage, 8)
	inputs := &stepInputs{
		Room:        roomInfo{Name: "test-room", SID: "RM_TEST"},
		Participant: participantInfo{Identity: testUserOne},
	}

	// Enqueue a chat message
	sess.chat <- chatMessage{
		Participant: participantInfo{Identity: testUserOne, SID: "PA_1"},
		Text:        "Hello, world!",
		Topic:       "",
		Timestamp:   time.Now(),
	}
	close(sess.chat) // close to terminate the loop

	ctx := context.Background()
	err := agent.forwardChat(ctx, sess, inputs, out, testLogger(t))
	if err != nil {
		t.Fatalf("forwardChat returned error: %v", err)
	}
	close(out)

	var msgs []sdkengram.StreamMessage
	for m := range out {
		msgs = append(msgs, m)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}

	msg := msgs[0]
	if msg.Metadata["type"] != transport.StreamTypeChatMessage {
		t.Errorf("expected metadata type %q, got %q", transport.StreamTypeChatMessage, msg.Metadata["type"])
	}
	if msg.Metadata["participant.id"] != testUserOne {
		t.Errorf("expected participant.id %s, got %q", testUserOne, msg.Metadata["participant.id"])
	}
	if msg.Binary == nil {
		t.Fatal("expected chat message to include Binary frame for hub schema validation")
	}
	if msg.Binary.MimeType != "application/json" {
		t.Fatalf("expected binary MimeType application/json, got %q", msg.Binary.MimeType)
	}
	if string(msg.Payload) != string(msg.Binary.Payload) {
		t.Fatalf("expected mirrored payload, payload=%q binary=%q", string(msg.Payload), string(msg.Binary.Payload))
	}

	var payload map[string]string
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		t.Fatalf("failed to unmarshal payload: %v", err)
	}
	if payload["text"] != "Hello, world!" {
		t.Errorf("expected text 'Hello, world!', got %q", payload["text"])
	}
	if payload["sender"] != testUserOne {
		t.Errorf("expected sender %s, got %q", testUserOne, payload["sender"])
	}
}

func TestStructuredStreamBytesPrefersInputsOverPayloadAndBinary(t *testing.T) {
	msg := sdkengram.NewInboundMessage(sdkengram.StreamMessage{
		Inputs:  []byte(`{"room":{"name":"inputs"}}`),
		Payload: []byte(`{"room":{"name":"payload"}}`),
		Binary:  &sdkengram.BinaryFrame{Payload: []byte(`{"room":{"name":"binary"}}`)},
	})

	raw, source := structuredStreamBytes(msg)
	if source != streamBytesSourceInputs {
		t.Fatalf("expected inputs source, got %v", source)
	}
	if string(raw) != `{"room":{"name":"inputs"}}` {
		t.Fatalf("unexpected raw bytes %q", string(raw))
	}
}

func TestBootstrapAcceptsPayloadBackedInputsAndClearsConsumedPayload(t *testing.T) {
	agent := &LiveKitAgent{}
	logger := testLogger(t)
	ctx := context.Background()
	in := make(chan sdkengram.InboundMessage, 1)
	in <- sdkengram.NewInboundMessage(sdkengram.StreamMessage{
		Payload: []byte(`{"room":{"name":"demo-room"}}`),
	})
	close(in)

	first, inputs, err := agent.bootstrap(ctx, in, logger)
	if err != nil {
		t.Fatalf("bootstrap returned error: %v", err)
	}
	if inputs.Room.Name != "demo-room" {
		t.Fatalf("expected room demo-room, got %q", inputs.Room.Name)
	}
	if first == nil {
		t.Fatal("expected first bootstrap message")
	}
	if len(first.Payload) != 0 {
		t.Fatalf("expected consumed payload to be cleared, got %q", string(first.Payload))
	}
}

func TestBootstrapMergesRuntimeInputsOverStaticConfig(t *testing.T) {
	t.Setenv(contracts.TriggerDataEnv, `{
		"room":{"name":"runtime-room","sid":"RM_RUNTIME"},
		"participant":{"identity":"runtime-user"},
		"sessionId":"runtime-session",
		"event":{"kind":"runtime"},
		"policy":{"source":"runtime"},
		"sourceAllowlist":["runtime-*"]
	}`)

	agent := &LiveKitAgent{
		cfg: config.Config{
			LiveKitURL: "wss://sandbox.livekit.io",
			Room:       config.RoomRef{Name: "config-room", SID: "RM_CONFIG"},
			Participant: config.ParticipantRef{
				Identity: "config-user",
				SID:      "PA_CONFIG",
			},
			Event:           map[string]any{"kind": "config", "keep": true},
			Policy:          map[string]any{"from": "config"},
			SessionID:       "config-session",
			SourceAllowlist: []string{"config-*"},
		},
	}

	in := make(chan sdkengram.InboundMessage)
	close(in)

	first, inputs, err := agent.bootstrap(context.Background(), in, testLogger(t))
	if err != nil {
		t.Fatalf("bootstrap returned error: %v", err)
	}
	if first != nil {
		t.Fatalf("expected no consumed bootstrap message, got %#v", first)
	}
	if inputs == nil {
		t.Fatal("expected merged inputs")
	}
	if inputs.Room.Name != "runtime-room" || inputs.Room.SID != "RM_RUNTIME" {
		t.Fatalf("expected runtime room override, got %+v", inputs.Room)
	}
	if inputs.Participant.Identity != "runtime-user" || inputs.Participant.SID != "PA_CONFIG" {
		t.Fatalf("expected runtime participant identity with config SID fallback, got %+v", inputs.Participant)
	}
	if inputs.SessionID != "runtime-session" {
		t.Fatalf("expected runtime session id, got %q", inputs.SessionID)
	}
	if got := inputs.Event["kind"]; got != testRuntimeValue {
		t.Fatalf("expected runtime event override, got %v", got)
	}
	if got := inputs.Event["keep"]; got != true {
		t.Fatalf("expected config event key to remain, got %v", got)
	}
	if got := inputs.Policy["source"]; got != testRuntimeValue {
		t.Fatalf("expected runtime policy key, got %v", got)
	}
	if got := inputs.Policy["from"]; got != "config" {
		t.Fatalf("expected config policy key to remain, got %v", got)
	}
	if len(inputs.SourceAllowlist) != 1 || inputs.SourceAllowlist[0] != testRuntimePrefix {
		t.Fatalf("expected runtime source allowlist override, got %#v", inputs.SourceAllowlist)
	}
}

func TestMergeStepInputsOverlayAndMapMerge(t *testing.T) {
	base := &stepInputs{
		Room:            roomInfo{Name: "config-room", SID: "RM_CONFIG"},
		Participant:     participantInfo{Identity: "config-user", SID: "PA_CONFIG"},
		AgentIdentity:   "config-agent",
		SessionID:       "config-session",
		SourceAllowlist: []string{"config-*"},
		Event:           map[string]any{"shared": "config", "configOnly": true},
		Policy:          map[string]any{"base": "config"},
	}
	overlay := &stepInputs{
		Room:            roomInfo{Name: "runtime-room"},
		Participant:     participantInfo{Identity: "runtime-user"},
		SessionID:       "runtime-session",
		SourceAllowlist: []string{testRuntimePrefix},
		Event:           map[string]any{"shared": testRuntimeValue, "runtimeOnly": true},
		Policy:          map[string]any{"runtime": true},
	}

	merged := mergeStepInputs(base, overlay)
	if merged.Room.Name != "runtime-room" || merged.Room.SID != "RM_CONFIG" {
		t.Fatalf("unexpected merged room %+v", merged.Room)
	}
	if merged.Participant.Identity != "runtime-user" || merged.Participant.SID != "PA_CONFIG" {
		t.Fatalf("unexpected merged participant %+v", merged.Participant)
	}
	if merged.SessionID != "runtime-session" {
		t.Fatalf("expected runtime session id, got %q", merged.SessionID)
	}
	if len(merged.SourceAllowlist) != 1 || merged.SourceAllowlist[0] != testRuntimePrefix {
		t.Fatalf("unexpected merged allowlist %#v", merged.SourceAllowlist)
	}
	if merged.Event["shared"] != testRuntimeValue ||
		merged.Event["configOnly"] != true ||
		merged.Event["runtimeOnly"] != true {
		t.Fatalf("unexpected merged event map %#v", merged.Event)
	}
	if merged.Policy["base"] != "config" || merged.Policy["runtime"] != true {
		t.Fatalf("unexpected merged policy map %#v", merged.Policy)
	}

	base.Event["configOnly"] = false
	base.SourceAllowlist[0] = "mutated"
	if merged.Event["configOnly"] != true {
		t.Fatalf("expected merged event map to be isolated from base mutations, got %v", merged.Event["configOnly"])
	}
	if merged.SourceAllowlist[0] != testRuntimePrefix {
		t.Fatalf("expected merged allowlist isolation, got %#v", merged.SourceAllowlist)
	}
}

func TestEmitSessionEventMirrorsPayloadToBinary(t *testing.T) {
	agent := &LiveKitAgent{}
	out := make(chan sdkengram.StreamMessage, 1)
	inputs := &stepInputs{
		Room:        roomInfo{Name: "demo-room", SID: "RM_TEST"},
		Participant: participantInfo{Identity: testUserOne, SID: "PA_1"},
		SessionID:   "session-123",
	}

	err := agent.emitSessionEvent(context.Background(), out, inputs, "agent-42", "livekit.session.started")
	if err != nil {
		t.Fatalf("emitSessionEvent returned error: %v", err)
	}

	select {
	case msg := <-out:
		if msg.Binary == nil {
			t.Fatal("expected binary payload")
		}
		if string(msg.Payload) != string(msg.Binary.Payload) {
			t.Fatalf("expected mirrored payload, payload=%q binary=%q", string(msg.Payload), string(msg.Binary.Payload))
		}
		var evt sessionEvent
		if err := json.Unmarshal(msg.Payload, &evt); err != nil {
			t.Fatalf("failed to unmarshal session event: %v", err)
		}
		if evt.Type != "livekit.session.started" {
			t.Fatalf("unexpected event type %q", evt.Type)
		}
	default:
		t.Fatal("expected emitted session event")
	}
}

func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.Default()
}

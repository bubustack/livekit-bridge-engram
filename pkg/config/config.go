package config

// CaptureConfig describes the PCM format we read from or write to LiveKit.
type CaptureConfig struct {
	Enabled    *bool  `json:"enabled,omitempty" mapstructure:"enabled"`
	Encoding   string `json:"encoding" mapstructure:"encoding"`
	SampleRate int    `json:"sampleRate" mapstructure:"sampleRate"`
	Channels   int    `json:"channels" mapstructure:"channels"`
}

type PlaybackConfig struct {
	Enabled *bool `json:"enabled,omitempty" mapstructure:"enabled"`
}

type RoomRef struct {
	Name string `json:"name" mapstructure:"name"`
	SID  string `json:"sid" mapstructure:"sid"`
}

type ParticipantRef struct {
	Identity string `json:"identity" mapstructure:"identity"`
	SID      string `json:"sid" mapstructure:"sid"`
}

// ChatConfig controls the LiveKit room chat bridge.
type ChatConfig struct {
	Enabled bool `json:"enabled" mapstructure:"enabled"`
	// Empty uses standard LiveKit room chat text streams on lk.chat;
	// any other topic uses custom UserData packets.
	Topic string `json:"topic" mapstructure:"topic"`
}

// Config holds static, per-Engram configuration injected via spec.with.
type Config struct {
	LiveKitURL          string         `json:"livekitURL" mapstructure:"livekitURL"`
	AgentIdentityPrefix string         `json:"agentIdentityPrefix" mapstructure:"agentIdentityPrefix"`
	PlaybackTrackName   string         `json:"playbackTrackName" mapstructure:"playbackTrackName"`
	SourceAllowlist     []string       `json:"sourceAllowlist" mapstructure:"sourceAllowlist"`
	Capture             CaptureConfig  `json:"capture" mapstructure:"capture"`
	Playback            PlaybackConfig `json:"playback" mapstructure:"playback"`
	Room                RoomRef        `json:"room" mapstructure:"room"`
	Participant         ParticipantRef `json:"participant" mapstructure:"participant"`
	Chat                ChatConfig     `json:"chat" mapstructure:"chat"`
	Event               map[string]any `json:"event" mapstructure:"event"`
	Policy              map[string]any `json:"policy" mapstructure:"policy"`
	SessionID           string         `json:"sessionId" mapstructure:"sessionId"`
}

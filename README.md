# 🎧 LiveKit Bridge Engram

A streaming Engram that joins a LiveKit room, mirrors remote audio into the bobrapet story graph, and plays downstream responses back into the room. Pair it with the [`livekit-webhook-impulse`](../livekit-webhook-impulse) to build real-time voice assistants on top of LiveKit Cloud, LiveKit Cloud Sandbox, or self-hosted clusters.

```
LiveKit Room ⇄ LiveKit Bridge Engram ⇄ Transport (bobravoz-grpc) ⇄ Your realtime story
```

## 🌟 Highlights

- Authenticates with LiveKit using server-side API keys.
- Subscribes to remote participant audio tracks (with allowlist support) and emits native streaming `AudioFrame` messages with LiveKit metadata.
- Publishes a PCM playback track so that LLM / TTS responses can be injected back into the room.
- Bridges standard LiveKit room chat text streams on `lk.chat` by default, so the same Engram works with the normal room chat UI instead of requiring a custom topic.
- Ignores any LiveKit participant whose identity matches the configured agent prefix (e.g., other bubu-agent instances) to prevent audio feedback loops.
- Emits session lifecycle events (`livekit.session.started` / `ended`) so you can orchestrate additional steps.

## 🚀 Quick Start

```bash
make lint
go test ./...
make docker-build
```

Apply `Engram.yaml`, wire the template into a realtime Story, and pass room/session
context from `livekit-webhook-impulse` or your own trigger source.

## ⚙️ Configuration (`Engram.spec.with`)

| Field | Type | Description | Default |
|-------|------|-------------|---------|
| `livekitURL` | string | WSS/HTTPS URL for your LiveKit deployment. | *(required)* |
| `agentIdentityPrefix` | string | Prefix used when auto-generating the bot's participant identity. | `bubu-agent` |
| `playbackTrackName` | string | Name for the published playback track. | `agent-playback` |
| `sourceAllowlist` | []string | Optional allowlist for participant identities (exact, `*`, `prefix*`). | accept all |
| `capture.enabled` | bool | Enable participant audio capture and forwarding into the story pipeline. | `true` |
| `capture.encoding` | string | PCM encoding advertised to LiveKit. | `pcm` |
| `capture.sampleRate` | int | Sample rate for capture + playback. | `48000` |
| `capture.channels` | int | Mono/Stereo selection. | `1` |
| `playback.enabled` | bool | Enable published playback track and downstream audio playback. | `true` |
| `chat.enabled` | bool | Enable forwarding LiveKit room chat messages into the story pipeline. | `false` |
| `chat.topic` | string | Optional custom UserData topic. Leave empty to use standard LiveKit room chat text streams on `lk.chat`. | `lk.chat` |

### 🔐 Secrets

Map the template's secret to a Kubernetes secret that contains:

```yaml
stringData:
  API_KEY: lk_api_key
  API_SECRET: supersecret
```

The template mounts these keys with the `LIVEKIT_` prefix, so the container sees
`LIVEKIT_API_KEY` and `LIVEKIT_API_SECRET` while the Engram reads them as `API_KEY`
and `API_SECRET` from the secret bundle.

## 📥 Inputs

Bootstrap inputs are merged from three sources, in this order:

1. static `spec.with` defaults
2. runtime trigger context (`BUBU_TRIGGER_DATA`)
3. first structured inbound stream message (if needed)

Later sources override earlier values, so trigger/session data is not clobbered by static defaults.

The merged input shape is:

```json
{
  "room": { "name": "support-room", "sid": "RM_xxx" },
  "participant": { "identity": "user-alice", "sid": "PA_xxx" },
  "event": { ... },
  "policy": { ... },
  "agentIdentity": "(optional override)"
}
```

The `livekit-webhook-impulse` already supplies this structure when you forward its payload into the LiveKit step.

## 📤 Outputs

- **Audio packets**: native `StreamMessage.Audio` frames (`PCM`, `SampleRateHz`, `Channels`, `Codec`) plus metadata keys such as `type=speech.audio.v1`, `room.*`, and `participant.*`.
- **Chat packets**: JSON payload with `text`, `sender`, and optional `topic`, with metadata `type=chat.message.v1`. By default the bridge reads and writes standard LiveKit room chat text streams on `lk.chat`; nonstandard `chat.topic` values switch the bridge into custom UserData packet mode.
- **Session events**: JSON payload with `type=livekit.session.started|ended` and matching metadata keys to help you bootstrap additional streaming steps.

## 🔄 Runtime Notes

Send JSON payloads back through the transport (e.g., from a TTS step) to control the agent:

```json
{
  "type": "speech.audio.v1",
  "audio": {
    "encoding": "pcm",
    "sampleRate": 48000,
    "channels": 1,
    "data": "<base64 PCM chunk>"
  }
}
```

`type: "stop"` or `"livekit.session.end"` will gracefully disconnect the bot from the room.

## 📘 Example Story

```yaml
steps:
  - name: ingress
    ref: { name: livekit-bridge }
    with:
      livekitURL: wss://my-sandbox.livekit.cloud
      playbackTrackName: assistant-playback
    secrets:
      livekit: livekit-credentials
  - name: stt
    ref: { name: whisper-stream }
  - name: llm
    ref: { name: openai-realtime }
  - name: tts
    ref: { name: tts-stream }
```

Wire the impulse's policy `storyInputs.entryEngram` to this step so each LiveKit event spins up the correct pipeline.

## 🧪 Local Development

- `make lint` – Run the shared lint and static-analysis checks.
- `go test ./...` – Run bridge tests in an environment with the required media dependencies.
- `make docker-build` – Build the bridge image for local clusters.

## 🤝 Community & Support

- [Contributing](./CONTRIBUTING.md)
- [Support](./SUPPORT.md)
- [Security Policy](./SECURITY.md)
- [Code of Conduct](./CODE_OF_CONDUCT.md)
- [Discord](https://discord.gg/dysrB7D8H6)


## 📄 License

Copyright 2025 BubuStack.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

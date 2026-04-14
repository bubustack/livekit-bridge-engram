# Contributing to livekit-bridge-engram

Thank you for considering a contribution—your help keeps this repository healthy for every bobrapet Story.

This guide explains how we work in the `livekit-bridge-engram` repository, from filing issues to sending patches.

## How Can I Contribute?

### Reporting Bugs

- Search for an existing report under [Issues](https://github.com/bubustack/livekit-bridge-engram/issues).
- If nothing matches, [open a new issue](https://github.com/bubustack/livekit-bridge-engram/issues/new) with a clear description, reproduction steps, and both observed and expected behaviour.

### Suggesting Enhancements

- Start a thread in [Discussions](https://github.com/orgs/bubustack/discussions) or file a feature request issue.
- Document motivating use-cases, proposed config additions, and any compatibility considerations for existing Stories.

### Pull Requests

- Fork the repository and branch from `main`.
- Keep changes focused; additive config should include manifest updates (`Engram.yaml` or `Impulse.yaml`) and README documentation.
- Run the quality gates locally:
  - `make lint`
  - `make test`
  - `make docker-build IMG=ghcr.io/<your-user>/livekit-bridge-engram:dev` when binaries or Dockerfile paths change
- Open the PR with a summary, testing evidence, and any follow-up TODOs.

## Development Workflow

### Prerequisites

- Go 1.26+
- Docker (or another OCI-compatible builder)
- `make`

### Setup

1.  Fork the repository.
2.  Clone your fork: `git clone https://github.com/<your-user>/livekit-bridge-engram.git`
3.  Enter the repo: `cd livekit-bridge-engram`
4.  Install tooling: `make lint-config`
5.  Confirm builds and tests: `make build && make test`

### Running Tests

```bash
make test
```

### Commit Message Conventions

We use [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) so releases and change logs stay automated.

Examples:
- `feat: add new configuration option`
- `fix: handle edge case in processing`
- `docs: clarify configuration fields`
- `chore: bump SDK to latest`

### Code of Conduct

Participation in this project is governed by the [Contributor Covenant Code of Conduct](./CODE_OF_CONDUCT.md). Report unacceptable behaviour to [conduct@bubustack.com](mailto:conduct@bubustack.com).

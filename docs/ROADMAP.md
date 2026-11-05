# ResilientMqtt Roadmap

## Vision
ResilientMqtt aims to become a production-ready, enterprise-grade MQTT client ecosystem for .NET, focusing on reliability, observability, extensibility, and long-term stability.

## Phase 1 – Stable Core (v1.x)

### Goals
- Stable production-ready MQTT client
- Backward compatible public API
- Excellent documentation
- High test coverage

### Current Core
- Automatic reconnect
- Exponential backoff
- Offline buffering
- Circuit breaker
- LWT
- Health checks
- Auto re-subscribe
- DI integration
- Hosted service integration
- Logging
- Configuration options

### v1.1 – Core Polish  🧪 built, in release candidates; final not released (see [STATUS](STATUS.md))
- ✅ ConnectionInfo API (`ConnectionInfo` snapshot incl. counters — the "Statistics" core)
- ✅ WaitForConnectionAsync()
- ✅ Better exceptions → classified failure reasons (`LastFailure`, `ConnectionLost` / `ConnectionRestored`)
- ✅ Publish outcomes (`PublishWithResultAsync`), refused subscribe/publish detection
- ✅ MQTT 5 sessions done right (`ProtocolVersion`, `SessionExpirySeconds`)
- ✅ Mutual TLS + private-CA trust, concurrent dispatch, auth-failure backoff, log EventIds
- ✅ Test suite (unit + in-process broker integration)
- ✅ Documentation corrected
- ⏭ Moved to v1.2: PingAsync() (public), FlushAsync(), full Statistics (message rates), durable offline-buffer
  providers (SQLite/Redis), credential provider / hot reload, multiple broker endpoints, named multiple clients

### v1.2 – API Stabilisation
- API freeze
- Semantic Versioning
- Long-term compatibility

## Phase 2 – Extension Packages

- ResilientMqtt.DynamicSecurity
- ResilientMqtt.Diagnostics
- ResilientMqtt.OpenTelemetry
- ResilientMqtt.RequestResponse
- ResilientMqtt.FileTransfer
- ResilientMqtt.DeviceShadow
- ResilientMqtt.OTA

## Phase 3 – Enterprise Platform

- Monitoring Dashboard
- Broker Administration
- Rules Engine
- Plugin Marketplace
- Cluster Support

## Version Roadmap

| Version | Focus |
|----------|-------|
| 1.0 | Stable Core |
| 1.1 | Core Polish 🧪 (release candidates, final not released) |
| 1.2 | Stable API |
| 1.3 | Dynamic Security |
| 1.4 | OpenTelemetry |
| 1.5 | File Transfer |
| 1.6 | Request/Response |
| 1.7 | Device Shadow |
| 1.8 | OTA |
| 2.0 | Enterprise Platform |

## Philosophy

- Keep the core small.
- Add enterprise features as optional packages.
- Prioritise reliability over feature count.
- Never introduce unnecessary breaking changes.

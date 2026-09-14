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

### v1.1 – Core Polish
- Statistics API
- ConnectionInfo API
- WaitForConnectionAsync()
- PingAsync()
- FlushAsync()
- Better exceptions
- Performance improvements
- Documentation improvements

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
| 1.1 | Core Polish |
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

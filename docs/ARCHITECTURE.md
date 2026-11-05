# ResilientMqtt Architecture

## Design Principles

1. Small and stable core
2. Optional extension packages
3. Production-first design
4. Clean dependency injection
5. Backward compatibility

## Core Responsibilities

- Connection lifecycle
- Publish / Subscribe
- Automatic reconnect
- Offline buffering
- Circuit breaker
- Health checks
- LWT
- Topic prefix
- Configuration
- Logging
- Failure classification (why a connection failed)
- MQTT 5 / 3.1.1 sessions
- Hold + in-order replay of a resumed session's queue
- Handler dispatch (inline or concurrent, with back-pressure)
- TLS (thumbprint, private CA, mutual TLS)
- Bounded logging

Details of the 1.1.0 additions: [RELEASE-1.1.0](RELEASE-1.1.0.md).

## Extension Packages

```text
ResilientMqtt
│
├── ResilientMqtt.DynamicSecurity
├── ResilientMqtt.Diagnostics
├── ResilientMqtt.OpenTelemetry
├── ResilientMqtt.RequestResponse
├── ResilientMqtt.FileTransfer
├── ResilientMqtt.DeviceShadow
└── ResilientMqtt.OTA
```

## Long-term Goals

- Stable API
- Enterprise reliability
- Rich ecosystem
- Excellent developer experience

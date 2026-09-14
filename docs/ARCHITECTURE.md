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

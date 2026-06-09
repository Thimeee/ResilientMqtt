using System;
using System.Collections.Generic;
using System.Text;

namespace ResilientMqtt
{
    public class ResilientMqttOptions
    {
        // ════════════════════════════════════════════════════════════════
        // Connection (required)
        // ════════════════════════════════════════════════════════════════

        /// <summary>Broker hostname or IP. Required.</summary>
        public string Host { get; set; } = "localhost";

        /// <summary>Broker TCP port. 1883 (plain) or 8883 (TLS).</summary>
        public int Port { get; set; } = 1883;

        /// <summary>Broker username. Empty = anonymous.</summary>
        public string Username { get; set; } = string.Empty;

        /// <summary>Broker password. Empty = anonymous.</summary>
        public string Password { get; set; } = string.Empty;

        /// <summary>
        /// Unique client identifier. MUST be unique per running instance —
        /// MQTT spec disconnects existing client when a duplicate connects.
        /// </summary>
        public string ClientId { get; set; } = $"resilient-mqtt-client-{Guid.NewGuid():N}";

        /// <summary>
        /// Optional prefix for all published topics.
        /// Example: "branches/B001" → PublishAsync("status") sends to "branches/B001/status".
        /// Empty = no prefix.
        /// </summary>
        public string TopicPrefix { get; set; } = string.Empty;

        /// <summary>
        /// false → broker retains subscriptions + QoS1/2 messages across reconnects (recommended for banking).
        /// true  → clean slate every connect.
        /// </summary>
        public bool CleanSession { get; set; } = false;

        /// <summary>Keep-alive interval (seconds). Default 30.</summary>
        public int KeepAliveSeconds { get; set; } = 30;

        // ════════════════════════════════════════════════════════════════
        // TLS (optional)
        // ════════════════════════════════════════════════════════════════
        public TlsOptions Tls { get; set; } = new();

        // ════════════════════════════════════════════════════════════════
        // Features (each independently toggleable)
        // ════════════════════════════════════════════════════════════════

        /// <summary>Last Will &amp; Testament — broker publishes OFFLINE if we disconnect ungracefully.</summary>
        public LastWillOptions LastWill { get; set; } = new();

        /// <summary>Offline message buffer — queues publishes when broker is unreachable.</summary>
        public OfflineBufferOptions OfflineBuffer { get; set; } = new();

        /// <summary>Circuit breaker — short-circuits publishes after repeated failures.</summary>
        public CircuitBreakerOptions CircuitBreaker { get; set; } = new();

        /// <summary>Idle-aware health check — pings broker on quiet channels.</summary>
        public HealthCheckOptions HealthCheck { get; set; } = new();

        /// <summary>Reconnect strategy with exponential backoff + jitter.</summary>
        public ReconnectOptions Reconnect { get; set; } = new();
    }

    // ════════════════════════════════════════════════════════════════════
    public class TlsOptions
    {
        /// <summary>Enable TLS.</summary>
        public bool Enabled { get; set; } = false;

        /// <summary>
        /// "None"   → default OS certificate validation
        /// "Normal" → accept any certificate (dev / internal LAN)
        /// "Hard"   → pin certificate thumbprint (production)
        /// </summary>
        public string Mode { get; set; } = "None";

        /// <summary>SHA1/SHA256 thumbprint when Mode = "Hard".</summary>
        public string TrustedThumbprint { get; set; } = string.Empty;
    }

    // ════════════════════════════════════════════════════════════════════
    public class LastWillOptions
    {
        /// <summary>Enable broker-side LWT. Disable for fire-and-forget scenarios.</summary>
        public bool Enabled { get; set; } = true;

        /// <summary>Sub-topic (prefixed automatically). Example: "status".</summary>
        public string Topic { get; set; } = "status";

        /// <summary>Payload published at successful connect.</summary>
        public string OnlinePayload { get; set; } = "ONLINE";

        /// <summary>Payload broker will publish on unexpected disconnect, and we publish on graceful shutdown.</summary>
        public string OfflinePayload { get; set; } = "OFFLINE";

        /// <summary>Retain the status message so new subscribers see current state.</summary>
        public bool Retain { get; set; } = true;
    }

    // ════════════════════════════════════════════════════════════════════
    public class OfflineBufferOptions
    {
        /// <summary>Buffer publishes when disconnected? false = drop on disconnect.</summary>
        public bool Enabled { get; set; } = true;

        /// <summary>Maximum messages held in memory. Oldest dropped on overflow.</summary>
        public int MaxSize { get; set; } = 1000;

        /// <summary>Delay between drained messages (ms) — prevents broker flood after reconnect.</summary>
        public int DrainPauseMs { get; set; } = 50;
    }

    // ════════════════════════════════════════════════════════════════════
    public class CircuitBreakerOptions
    {
        /// <summary>Enable circuit breaker on publish path.</summary>
        public bool Enabled { get; set; } = true;

        /// <summary>Consecutive failures before opening the circuit.</summary>
        public int FailureThreshold { get; set; } = 5;

        /// <summary>Minutes before transitioning Open → HalfOpen.</summary>
        public int ResetMinutes { get; set; } = 2;
    }

    // ════════════════════════════════════════════════════════════════════
    public class HealthCheckOptions
    {
        /// <summary>Enable idle-aware ping check.</summary>
        public bool Enabled { get; set; } = true;

        /// <summary>Tick interval (minutes).</summary>
        public int IntervalMinutes { get; set; } = 1;

        /// <summary>Channel idle threshold (minutes) before triggering a ping.</summary>
        public int IdleThresholdMinutes { get; set; } = 2;
    }

    // ════════════════════════════════════════════════════════════════════
    public class ReconnectOptions
    {
        /// <summary>Enable automatic reconnect on disconnect.</summary>
        public bool Enabled { get; set; } = true;

        /// <summary>Initial delay (seconds) for backoff.</summary>
        public int BaseDelaySeconds { get; set; } = 1;

        /// <summary>Maximum delay (seconds) for backoff.</summary>
        public int MaxDelaySeconds { get; set; } = 60;

        /// <summary>Apply random jitter to prevent thundering-herd reconnects.</summary>
        public bool UseJitter { get; set; } = true;
    }
}
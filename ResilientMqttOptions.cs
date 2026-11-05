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

        /// <summary>Prefix of the auto-generated <see cref="ClientId"/> (used to warn when a persistent session is
        /// combined with an id that changes on every start).</summary>
        public const string AutoClientIdPrefix = "resilient-mqtt-client-";

        /// <summary>Session length used when <see cref="CleanSession"/> is false on MQTT 5 and
        /// <see cref="SessionExpirySeconds"/> is not set: 1 day.</summary>
        public const int DefaultPersistentSessionSeconds = 86_400;

        /// <summary>
        /// Unique client identifier. MUST be unique per running instance —
        /// MQTT spec disconnects existing client when a duplicate connects.
        /// Set a STABLE id (e.g. derived from the machine/terminal) when you use a persistent session; the
        /// auto-generated default changes on every start.
        /// </summary>
        public string ClientId { get; set; } = $"{AutoClientIdPrefix}{Guid.NewGuid():N}";

        /// <summary>
        /// Optional prefix for all published topics.
        /// Example: "branches/B001" → PublishAsync("status") sends to "branches/B001/status".
        /// Empty = no prefix.
        /// </summary>
        public string TopicPrefix { get; set; } = string.Empty;

        /// <summary>
        /// MQTT protocol version: "V500" (MQTT 5.0, default) or "V311" (MQTT 3.1.1).
        /// MQTT 5 adds broker reason codes (why a connection was refused or closed — e.g. SessionTakenOver).
        /// </summary>
        public string ProtocolVersion { get; set; } = "V500";

        /// <summary>
        /// Whether the broker keeps this client's session (subscriptions + queued QoS1/2 messages) while it is
        /// offline.
        /// <list type="bullet">
        /// <item><b>false</b> → persistent session: messages published while the client is offline are queued by the
        /// broker and delivered on reconnect. On MQTT 5 the session lives for <see cref="SessionExpirySeconds"/>
        /// (default <see cref="DefaultPersistentSessionSeconds"/> = 1 day). Use a stable <see cref="ClientId"/>.</item>
        /// <item><b>true</b> → clean start every connect, nothing is queued while offline (choose this when stale
        /// commands must never run late — the application re-sends what still matters after a reconnect).</item>
        /// </list>
        /// </summary>
        public bool CleanSession { get; set; } = false;

        /// <summary>
        /// MQTT 5 only: how long (seconds) the broker keeps the session after the connection closes.
        /// null (default) → <see cref="DefaultPersistentSessionSeconds"/> when <see cref="CleanSession"/> is false,
        /// 0 when it is true. 0 = the session ends with the connection. Ignored on MQTT 3.1.1 (the broker decides).
        /// </summary>
        public int? SessionExpirySeconds { get; set; }

        /// <summary>Keep-alive interval (seconds). Default 30.</summary>
        public int KeepAliveSeconds { get; set; } = 30;

        /// <summary>
        /// Max seconds to wait for a single publish (QoS1/2 awaiting PUBACK/PUBCOMP) before it is
        /// treated as failed and the message is buffered. Bounds a publish that would otherwise hang
        /// on a half-open socket while holding the publish lock. Default 15.
        /// </summary>
        public int PublishTimeoutSeconds { get; set; } = 15;

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

        /// <summary>How inbound messages are handed to your subscription handlers.</summary>
        public DispatchOptions Dispatch { get; set; } = new();
    }

    // ════════════════════════════════════════════════════════════════════
    public class DispatchOptions
    {
        /// <summary>
        /// "Inline" (default) → each handler is awaited on the receive loop, one message at a time, in order.
        /// A slow handler delays all inbound traffic AND the keep-alive — keep handlers fast (offload heavy work).
        /// <para>"Concurrent" → handlers run off the receive loop (up to <see cref="MaxConcurrency"/> at once), so a
        /// slow handler can never stall the connection. Messages may then be processed out of order, and a QoS1
        /// message is acknowledged before its handler finishes.</para>
        /// </summary>
        public string Mode { get; set; } = "Inline";

        /// <summary>Concurrent mode: maximum handlers running at the same time. Default 8.</summary>
        public int MaxConcurrency { get; set; } = 8;

        /// <summary>Concurrent mode: on dispose, how long to wait for running handlers to finish. Default 5.</summary>
        public int ShutdownWaitSeconds { get; set; } = 5;

        /// <summary>
        /// Concurrent mode: how many handler calls may wait for a free slot (beyond <see cref="MaxConcurrency"/>)
        /// before intake is slowed (back-pressure) instead of growing an unbounded backlog. Default 1000.
        /// </summary>
        public int MaxPending { get; set; } = 1000;
    }

    // ════════════════════════════════════════════════════════════════════
    public class TlsOptions
    {
        /// <summary>Enable TLS.</summary>
        public bool Enabled { get; set; } = false;

        /// <summary>
        /// How the broker's certificate is checked:
        /// "None"   → default OS certificate validation (public CA / domain PKI)
        /// "Normal" → accept any certificate (dev / internal LAN only — no MITM protection)
        /// "Hard"   → pin the certificate thumbprint (<see cref="TrustedThumbprint"/>)
        /// "Ca"     → trust a private CA (<see cref="CaCertificatePath"/>): full chain validation against that CA
        /// </summary>
        public string Mode { get; set; } = "None";

        /// <summary>Certificate thumbprint (SHA-1 hex, as shown by Windows) when Mode = "Hard".</summary>
        public string TrustedThumbprint { get; set; } = string.Empty;

        /// <summary>Mode = "Ca": path to the CA certificate (PEM or DER) that issued the broker certificate.</summary>
        public string CaCertificatePath { get; set; } = string.Empty;

        /// <summary>Mode = "Ca": also require the certificate name to match the broker host. Default true.</summary>
        public bool CheckCertificateName { get; set; } = true;

        /// <summary>Mutual TLS: path to the client certificate with its private key (PFX/PKCS#12). Empty = none.</summary>
        public string ClientCertificatePath { get; set; } = string.Empty;

        /// <summary>Mutual TLS: password of <see cref="ClientCertificatePath"/> (use user-secrets / env vars).</summary>
        public string ClientCertificatePassword { get; set; } = string.Empty;

        /// <summary>Mutual TLS: alternatively, the thumbprint of a client certificate in the Windows/OS store.</summary>
        public string ClientCertificateThumbprint { get; set; } = string.Empty;

        /// <summary>Store for <see cref="ClientCertificateThumbprint"/>: "LocalMachine" (default — Windows services)
        /// or "CurrentUser".</summary>
        public string ClientCertificateStore { get; set; } = "LocalMachine";
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

        /// <summary>
        /// Payload the BROKER publishes on UNGRACEFUL disconnect (process crash,
        /// network drop, TCP timeout). Set this with a clear "connection lost"
        /// marker so subscribers can distinguish it from a clean shutdown.
        /// </summary>
        public string OfflinePayload { get; set; } = "OFFLINE";

        /// <summary>
        /// Optional. Payload WE publish ourselves just before a GRACEFUL
        /// disconnect (Ctrl+C, service stop, DisconnectAsync called).
        /// If null, falls back to <see cref="OfflinePayload"/>.
        /// <para>Use this to distinguish "we left politely" from "we died":
        /// e.g. <c>OfflinePayload = "{...,reason:connection_lost}"</c> while
        /// <c>GracefulShutdownPayload = "{...,reason:graceful_shutdown}"</c>.</para>
        /// </summary>
        public string? GracefulShutdownPayload { get; set; }

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

        /// <summary>
        /// Max seconds to wait for a health-check ping before treating it as failed (→ reconnect).
        /// Prevents a hung ping from silently disabling the monitor. Default 10.
        /// </summary>
        public int PingTimeoutSeconds { get; set; } = 10;
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

        /// <summary>
        /// Max seconds to wait for a single connect attempt (TCP + MQTT CONNECT/CONNACK handshake)
        /// before giving up and letting the reconnect loop retry. Bounds a half-open connection that
        /// would otherwise hang indefinitely. Default 15.
        /// </summary>
        public int ConnectTimeoutSeconds { get; set; } = 15;

        /// <summary>
        /// Raise the <c>StillDown</c> event once per outage after the client has been unable to
        /// reconnect for this many seconds, so a host can alert on a stuck client. 0 = disabled
        /// (no behaviour change). Default 120.
        /// </summary>
        public int StillDownAfterSeconds { get; set; } = 120;

        /// <summary>
        /// After the broker REFUSES the sign-in (wrong / rotated / revoked credentials), wait at least this many
        /// seconds before the next attempt instead of the normal backoff — retrying fast can't fix credentials and
        /// only loads the broker's authentication. 0 = disabled (normal backoff). Default 0.
        /// </summary>
        public int AuthRejectedDelaySeconds { get; set; } = 0;
    }
}
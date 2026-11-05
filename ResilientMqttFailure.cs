using System;
using System.Collections.Generic;

namespace ResilientMqtt
{
    /// <summary>
    /// Why a connection attempt failed or an established connection was lost. Use it to decide what to do:
    /// a <see cref="Network"/> / <see cref="Timeout"/> problem can recover on its own (or with a restart), while
    /// <see cref="AuthRejected"/> / <see cref="TlsFailure"/> / <see cref="BrokerUnavailable"/> need a person or a
    /// configuration change — retrying or restarting the client won't fix them.
    /// </summary>
    public enum ResilientMqttFailureKind
    {
        /// <summary>No failure recorded.</summary>
        None = 0,

        /// <summary>The broker refused the sign-in (wrong/rotated/revoked credentials, banned client, not authorised).</summary>
        AuthRejected,

        /// <summary>The TLS handshake failed (certificate rejected, wrong TLS setting, TLS on a non-TLS port).</summary>
        TlsFailure,

        /// <summary>The broker host answered but the broker isn't accepting clients (port closed, service stopped,
        /// shutting down, busy, quota / rate limit, redirected).</summary>
        BrokerUnavailable,

        /// <summary>A network problem (name lookup failed, host/network unreachable, connection reset or closed).</summary>
        Network,

        /// <summary>No answer in time (connect timed out, keep-alive / health ping got no reply).</summary>
        Timeout,

        /// <summary>Another client connected with the same client id and the broker closed this session
        /// (typically a duplicated/cloned machine). MQTT 5 only — MQTT 3.1.1 brokers just close the socket.</summary>
        SessionTakenOver,

        /// <summary>The broker rejected the protocol exchange (invalid client id, unsupported protocol version,
        /// malformed packet, the endpoint isn't an MQTT broker).</summary>
        ProtocolError,

        /// <summary>Anything that couldn't be classified — see <see cref="ResilientMqttFailure.Detail"/>.</summary>
        Unknown,
    }

    /// <summary>A classified connection failure.</summary>
    /// <param name="Kind">What kind of failure it was.</param>
    /// <param name="Detail">Short technical description (safe to log; not meant for end users).</param>
    /// <param name="Code">The broker's CONNACK result code or disconnect reason, or the socket error, if any.</param>
    /// <param name="ExceptionType">The type name of the underlying exception, if any.</param>
    /// <param name="AtUtc">When it happened (UTC).</param>
    /// <param name="DuringConnect">true = a connect attempt failed; false = an established connection was lost.</param>
    public sealed record ResilientMqttFailure(
        ResilientMqttFailureKind Kind,
        string Detail,
        string? Code,
        string? ExceptionType,
        DateTime AtUtc,
        bool DuringConnect);

    /// <summary>A point-in-time snapshot of the client's connection health.</summary>
    /// <param name="State">Current lifecycle state.</param>
    /// <param name="IsConnected">true when the underlying MQTT connection is up.</param>
    /// <param name="ConnectedSinceUtc">When the current connection was established (null while down).</param>
    /// <param name="DownSinceUtc">When the current outage began (null while connected).</param>
    /// <param name="ReconnectAttempt">Reconnect attempts made in the current outage (0 while connected).</param>
    /// <param name="TotalConnects">Successful connects since start. Rising fast = the connection is flapping.</param>
    /// <param name="LastFailure">The most recent failure (null after a successful connect).</param>
    /// <param name="BufferedCount">Messages waiting in the offline buffer.</param>
    /// <param name="RejectedSubscriptions">Topic filters the broker refused (filter → result code).</param>
    /// <param name="DeniedPublishes">QoS1/2 publishes the broker refused since start.</param>
    /// <param name="ProtocolVersion">The MQTT protocol version in use ("V500" or "V311").</param>
    /// <param name="SessionPresent">true when the broker resumed an existing session on the last connect.</param>
    public sealed record ResilientMqttConnectionInfo(
        ResilientMqttConnectionState State,
        bool IsConnected,
        DateTime? ConnectedSinceUtc,
        DateTime? DownSinceUtc,
        int ReconnectAttempt,
        long TotalConnects,
        ResilientMqttFailure? LastFailure,
        int BufferedCount,
        IReadOnlyDictionary<string, string> RejectedSubscriptions,
        long DeniedPublishes,
        string ProtocolVersion,
        bool SessionPresent);

    /// <summary>Arguments of <see cref="IResilientMqttClient.ConnectionLost"/>.</summary>
    /// <param name="Failure">Why the connection was lost.</param>
    /// <param name="WasConnectedFor">How long the connection had been up.</param>
    public sealed record ResilientMqttConnectionLostArgs(ResilientMqttFailure Failure, TimeSpan WasConnectedFor);

    /// <summary>Arguments of <see cref="IResilientMqttClient.ConnectionRestored"/>.</summary>
    /// <param name="Downtime">How long the client was without a connection.</param>
    /// <param name="Attempts">Reconnect attempts it took.</param>
    /// <param name="SessionPresent">true when the broker resumed the previous session (queued messages follow).</param>
    /// <param name="LastFailure">The last failure seen during the outage.</param>
    public sealed record ResilientMqttConnectionRestoredArgs(
        TimeSpan Downtime, int Attempts, bool SessionPresent, ResilientMqttFailure? LastFailure);

    /// <summary>What happened to a message given to <see cref="IResilientMqttClient.PublishWithResultAsync{T}"/>.</summary>
    public enum ResilientMqttPublishOutcome
    {
        /// <summary>The implementation doesn't report outcomes (a custom <see cref="IResilientMqttClient"/>).</summary>
        Unknown = 0,

        /// <summary>Handed to the broker (QoS 0) or acknowledged by it (QoS 1/2).</summary>
        Sent,

        /// <summary>Not sent now — kept in the offline buffer and sent after the next (re)connect.</summary>
        Buffered,

        /// <summary>Not sent and not kept (no connection / circuit open / failure with the offline buffer disabled,
        /// or the caller cancelled). The message is lost.</summary>
        Dropped,

        /// <summary>The broker refused it (QoS 1/2 only — e.g. the topic isn't allowed for this client). Not retried.</summary>
        Denied,
    }
}

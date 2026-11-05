using System;
using System.IO;
using System.Net.Sockets;
using System.Security.Authentication;
using MQTTnet;
using MQTTnet.Adapter;
using MQTTnet.Exceptions;

namespace ResilientMqtt.Internal
{
    /// <summary>
    /// Turns what MQTTnet 5 reports into a <see cref="ResilientMqttFailure"/>. Pure — no I/O, no state.
    /// <para>The mapping is built from measured MQTTnet 5.1 behaviour, not from its docs:
    /// a refused sign-in RETURNS a CONNACK code (no exception); a closed port throws MqttCommunicationException →
    /// SocketException(ConnectionRefused); a rejected TLS certificate throws MqttCommunicationException →
    /// AuthenticationException; TLS against a plain port throws MqttCommunicationException → IOException (EOF);
    /// a non-MQTT endpoint throws MqttConnectingFailedException → MqttProtocolViolationException.</para>
    /// </summary>
    internal static class FailureClassifier
    {
        private const int MaxDetail = 240;

        public static ResilientMqttFailure FromConnectResult(MqttClientConnectResultCode code, string? reasonString, DateTime nowUtc)
        {
            var kind = code switch
            {
                MqttClientConnectResultCode.NotAuthorized or
                MqttClientConnectResultCode.BadUserNameOrPassword or
                MqttClientConnectResultCode.Banned or
                MqttClientConnectResultCode.BadAuthenticationMethod => ResilientMqttFailureKind.AuthRejected,

                MqttClientConnectResultCode.ServerUnavailable or
                MqttClientConnectResultCode.ServerBusy or
                MqttClientConnectResultCode.QuotaExceeded or
                MqttClientConnectResultCode.ConnectionRateExceeded or
                MqttClientConnectResultCode.UseAnotherServer or
                MqttClientConnectResultCode.ServerMoved => ResilientMqttFailureKind.BrokerUnavailable,

                _ => ResilientMqttFailureKind.ProtocolError,
            };

            var detail = kind switch
            {
                ResilientMqttFailureKind.AuthRejected => $"Broker refused the sign-in ({code})",
                ResilientMqttFailureKind.BrokerUnavailable => $"Broker is not accepting clients ({code})",
                _ => $"Broker rejected the connection ({code})",
            };
            if (!string.IsNullOrWhiteSpace(reasonString)) detail += $": {reasonString}";

            return new ResilientMqttFailure(kind, Trim(detail), code.ToString(), null, nowUtc, DuringConnect: true);
        }

        public static ResilientMqttFailure FromDisconnect(
            MqttClientDisconnectReason reason, string? reasonString, Exception? exception, bool tlsEnabled, DateTime nowUtc)
        {
            ResilientMqttFailureKind? kind = reason switch
            {
                MqttClientDisconnectReason.SessionTakenOver => ResilientMqttFailureKind.SessionTakenOver,

                MqttClientDisconnectReason.NotAuthorized or
                MqttClientDisconnectReason.BadAuthenticationMethod => ResilientMqttFailureKind.AuthRejected,

                MqttClientDisconnectReason.ServerShuttingDown or
                MqttClientDisconnectReason.ServerBusy or
                MqttClientDisconnectReason.AdministrativeAction or
                MqttClientDisconnectReason.UseAnotherServer or
                MqttClientDisconnectReason.ServerMoved or
                MqttClientDisconnectReason.ConnectionRateExceeded or
                MqttClientDisconnectReason.QuotaExceeded or
                MqttClientDisconnectReason.MaximumConnectTime => ResilientMqttFailureKind.BrokerUnavailable,

                MqttClientDisconnectReason.KeepAliveTimeout => ResilientMqttFailureKind.Timeout,

                MqttClientDisconnectReason.MalformedPacket or
                MqttClientDisconnectReason.ProtocolError or
                MqttClientDisconnectReason.ImplementationSpecificError or
                MqttClientDisconnectReason.TopicFilterInvalid or
                MqttClientDisconnectReason.TopicNameInvalid or
                MqttClientDisconnectReason.ReceiveMaximumExceeded or
                MqttClientDisconnectReason.TopicAliasInvalid or
                MqttClientDisconnectReason.PacketTooLarge or
                MqttClientDisconnectReason.MessageRateTooHigh or
                MqttClientDisconnectReason.PayloadFormatInvalid or
                MqttClientDisconnectReason.RetainNotSupported or
                MqttClientDisconnectReason.QosNotSupported or
                MqttClientDisconnectReason.SharedSubscriptionsNotSupported or
                MqttClientDisconnectReason.SubscriptionIdentifiersNotSupported or
                MqttClientDisconnectReason.WildcardSubscriptionsNotSupported => ResilientMqttFailureKind.ProtocolError,

                _ => null,   // NormalDisconnection / UnspecifiedError / DisconnectWithWillMessage → look at the exception
            };

            if (kind is null)
            {
                if (exception is not null)
                    return FromException(exception, tlsEnabled, duringConnect: false, nowUtc) with { Code = reason.ToString() };

                return new ResilientMqttFailure(ResilientMqttFailureKind.Network,
                    "Connection closed by the broker or the network", reason.ToString(), null, nowUtc, DuringConnect: false);
            }

            var detail = kind switch
            {
                ResilientMqttFailureKind.SessionTakenOver => "Another client connected with the same client id",
                ResilientMqttFailureKind.AuthRejected => "Broker revoked the session (not authorised)",
                ResilientMqttFailureKind.BrokerUnavailable => $"Broker closed the connection ({reason})",
                ResilientMqttFailureKind.Timeout => "Broker closed the connection: keep-alive timeout",
                _ => $"Broker closed the connection: protocol error ({reason})",
            };
            if (!string.IsNullOrWhiteSpace(reasonString)) detail += $": {reasonString}";

            return new ResilientMqttFailure(kind.Value, Trim(detail), reason.ToString(), exception?.GetType().Name,
                nowUtc, DuringConnect: false);
        }

        public static ResilientMqttFailure FromException(Exception exception, bool tlsEnabled, bool duringConnect, DateTime nowUtc)
        {
            var (kind, detail, code) = Classify(exception, tlsEnabled, duringConnect);
            return new ResilientMqttFailure(kind, Trim(detail), code, Innermost(exception).GetType().Name, nowUtc, duringConnect);
        }

        public static ResilientMqttFailure ConnectTimeout(TimeSpan timeout, DateTime nowUtc)
            => new(ResilientMqttFailureKind.Timeout,
                   $"No answer from the broker within {timeout.TotalSeconds:F0} s", null, nameof(OperationCanceledException),
                   nowUtc, DuringConnect: true);

        public static ResilientMqttFailure PingTimeout(DateTime nowUtc)
            => new(ResilientMqttFailureKind.Timeout,
                   "No reply from the broker to a health ping — the connection was dead and has been reset", null, null,
                   nowUtc, DuringConnect: false);

        // ════════════════════════════════════════════════════════════════
        private static (ResilientMqttFailureKind Kind, string Detail, string? Code) Classify(
            Exception exception, bool tlsEnabled, bool duringConnect)
        {
            // Walk the whole chain once; the most specific signal wins (TLS > socket > protocol > generic).
            AuthenticationException? auth = null;
            SocketException? socket = null;
            MqttProtocolViolationException? protocol = null;
            MqttCommunicationTimedOutException? commTimeout = null;
            TimeoutException? timeout = null;
            IOException? io = null;

            for (var e = exception; e is not null; e = Next(e))
            {
                switch (e)
                {
                    case AuthenticationException a: auth ??= a; break;
                    case SocketException s: socket ??= s; break;
                    case MqttProtocolViolationException p: protocol ??= p; break;
                    case MqttCommunicationTimedOutException t: commTimeout ??= t; break;
                    case TimeoutException t: timeout ??= t; break;
                    case IOException i: io ??= i; break;
                }
            }

            if (auth is not null)
                return (ResilientMqttFailureKind.TlsFailure, $"TLS handshake failed: {auth.Message}", null);

            // #48 — with TLS on, the broker dropping the connection WHILE CONNECTING (after the TCP connect succeeded)
            // is almost always a TLS problem: a TLS ClientHello on a plain port, or a required/rejected client
            // certificate. Brokers differ — Mosquitto closes (EOF), others reset (ConnectionReset) — and MQTTnet strips
            // the stack, so the phase can't be told apart from a genuine network reset at that instant. Classified as
            // TlsFailure (a transient reset clears on the next successful attempt anyway). A refused / unreachable TCP
            // connect keeps its own classification below.
            if (tlsEnabled && duringConnect && auth is null && commTimeout is null && timeout is null &&
                ((socket is not null && socket.SocketErrorCode is SocketError.ConnectionReset or SocketError.ConnectionAborted)
                 || (socket is null && io is not null)))
                return (ResilientMqttFailureKind.TlsFailure,
                    "TLS handshake failed — the broker closed the connection while connecting " +
                    "(TLS against a non-TLS port, or a client certificate is required/rejected? A one-off network reset looks the same)",
                    socket?.SocketErrorCode.ToString());

            // #48 — a bare "Connection closed." while connecting (no socket / IO / protocol detail): the broker hung up
            // without a reason. With TLS 1.3 that is how a missing/rejected client certificate surfaces; it is also how a
            // broker connection limit or an IP block looks. Honest answer: Unknown, with the likely causes.
            if (duringConnect && socket is null && io is null && protocol is null && commTimeout is null && timeout is null
                && Innermost(exception).GetType() == typeof(MqttCommunicationException))   // MQTTnet may wrap it in MqttConnectingFailedException
                return (ResilientMqttFailureKind.Unknown, tlsEnabled
                    ? "The broker closed the connection while connecting without giving a reason — with TLS this is usually a missing or rejected client certificate; otherwise a refused sign-in on an MQTT 3.1.1 broker, a broker connection limit or an IP/firewall block"
                    : "The broker closed the connection while connecting without giving a reason (refused sign-in on an MQTT 3.1.1 broker? broker connection limit? IP/firewall block?)", null);

            if (socket is not null)
            {
                var code = socket.SocketErrorCode.ToString();
                return socket.SocketErrorCode switch
                {
                    SocketError.ConnectionRefused =>
                        (ResilientMqttFailureKind.BrokerUnavailable, "Broker host refused the connection (port closed or broker service stopped)", code),
                    SocketError.HostNotFound or SocketError.TryAgain or SocketError.NoData =>
                        (ResilientMqttFailureKind.Network, "Broker host name could not be resolved (DNS)", code),
                    SocketError.TimedOut =>
                        (ResilientMqttFailureKind.Timeout, "Network timed out reaching the broker", code),
                    SocketError.NetworkUnreachable or SocketError.HostUnreachable or SocketError.NetworkDown =>
                        (ResilientMqttFailureKind.Network, "Broker host or network unreachable", code),
                    SocketError.ConnectionReset or SocketError.ConnectionAborted or SocketError.Shutdown =>
                        (ResilientMqttFailureKind.Network, "Connection reset by the broker or the network", code),
                    _ => (ResilientMqttFailureKind.Network, $"Network error: {socket.Message}", code),
                };
            }

            if (protocol is not null)
                return (ResilientMqttFailureKind.ProtocolError,
                    $"Not a valid MQTT exchange (is this endpoint an MQTT broker?): {protocol.Message}", null);

            if (commTimeout is not null || timeout is not null)
                return (ResilientMqttFailureKind.Timeout, "The broker did not answer in time", null);

            if (exception is OperationCanceledException)
                return (ResilientMqttFailureKind.Timeout, "The operation was cancelled before the broker answered", null);

            if (io is not null || exception is MqttCommunicationException || exception is MqttConnectingFailedException)
                return (ResilientMqttFailureKind.Network, $"Communication error: {Innermost(exception).Message}", null);

            return (ResilientMqttFailureKind.Unknown, $"{exception.GetType().Name}: {exception.Message}", null);
        }

        private static Exception? Next(Exception e)
            => e is AggregateException agg && agg.InnerExceptions.Count > 0 ? agg.InnerExceptions[0] : e.InnerException;

        private static Exception Innermost(Exception e)
        {
            var current = e;
            while (Next(current) is { } inner) current = inner;
            return current;
        }

        private static string Trim(string s) => s.Length <= MaxDetail ? s : s[..MaxDetail] + "…";
    }
}

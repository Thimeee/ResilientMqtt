using System.Net.Sockets;
using System.Security.Authentication;
using MQTTnet;
using MQTTnet.Exceptions;
using ResilientMqtt.Internal;

namespace ResilientMqtt.Tests.Unit
{
    // The shapes used here are the ones MQTTnet 5.1 was MEASURED to produce against a real broker
    // (docs/ResilientMqtt-1.0.4-Plan.md §0 in the consuming project).
    public class FailureClassifierTests
    {
        private static readonly DateTime Now = new(2026, 10, 2, 10, 0, 0, DateTimeKind.Utc);

        [Theory]
        [InlineData(MqttClientConnectResultCode.NotAuthorized)]
        [InlineData(MqttClientConnectResultCode.BadUserNameOrPassword)]
        [InlineData(MqttClientConnectResultCode.Banned)]
        [InlineData(MqttClientConnectResultCode.BadAuthenticationMethod)]
        public void Refused_sign_in_is_AuthRejected(MqttClientConnectResultCode code)
        {
            var f = FailureClassifier.FromConnectResult(code, null, Now);
            Assert.Equal(ResilientMqttFailureKind.AuthRejected, f.Kind);
            Assert.Equal(code.ToString(), f.Code);
            Assert.True(f.DuringConnect);
        }

        [Theory]
        [InlineData(MqttClientConnectResultCode.ServerUnavailable)]
        [InlineData(MqttClientConnectResultCode.ServerBusy)]
        [InlineData(MqttClientConnectResultCode.QuotaExceeded)]
        [InlineData(MqttClientConnectResultCode.ConnectionRateExceeded)]
        [InlineData(MqttClientConnectResultCode.UseAnotherServer)]
        [InlineData(MqttClientConnectResultCode.ServerMoved)]
        public void Broker_not_accepting_is_BrokerUnavailable(MqttClientConnectResultCode code)
            => Assert.Equal(ResilientMqttFailureKind.BrokerUnavailable, FailureClassifier.FromConnectResult(code, null, Now).Kind);

        [Theory]
        [InlineData(MqttClientConnectResultCode.ClientIdentifierNotValid)]
        [InlineData(MqttClientConnectResultCode.UnsupportedProtocolVersion)]
        [InlineData(MqttClientConnectResultCode.MalformedPacket)]
        public void Other_connack_codes_are_ProtocolError(MqttClientConnectResultCode code)
            => Assert.Equal(ResilientMqttFailureKind.ProtocolError, FailureClassifier.FromConnectResult(code, null, Now).Kind);

        [Fact]
        public void Connect_result_reason_string_is_kept_in_the_detail()
            => Assert.Contains("password expired", FailureClassifier.FromConnectResult(
                MqttClientConnectResultCode.NotAuthorized, "password expired", Now).Detail);

        [Fact]
        public void Port_closed_is_BrokerUnavailable()
        {
            var ex = new MqttCommunicationException("Error while connecting host", new SocketException((int)SocketError.ConnectionRefused));
            var f = FailureClassifier.FromException(ex, tlsEnabled: false, duringConnect: true, Now);
            Assert.Equal(ResilientMqttFailureKind.BrokerUnavailable, f.Kind);
            Assert.Equal("ConnectionRefused", f.Code);
        }

        [Theory]
        [InlineData(SocketError.HostNotFound)]
        [InlineData(SocketError.TryAgain)]
        [InlineData(SocketError.NetworkUnreachable)]
        [InlineData(SocketError.HostUnreachable)]
        [InlineData(SocketError.ConnectionReset)]
        public void Socket_network_errors_are_Network(SocketError error)
        {
            var ex = new MqttCommunicationException("x", new SocketException((int)error));
            Assert.Equal(ResilientMqttFailureKind.Network, FailureClassifier.FromException(ex, false, true, Now).Kind);
        }

        [Fact]
        public void Socket_timeout_is_Timeout()
            => Assert.Equal(ResilientMqttFailureKind.Timeout, FailureClassifier.FromException(
                new MqttCommunicationException("x", new SocketException((int)SocketError.TimedOut)), false, true, Now).Kind);

        [Fact]
        public void Rejected_certificate_is_TlsFailure()
        {
            var ex = new MqttCommunicationException("The remote certificate was rejected",
                new AuthenticationException("The remote certificate was rejected by the provided RemoteCertificateValidationCallback."));
            Assert.Equal(ResilientMqttFailureKind.TlsFailure, FailureClassifier.FromException(ex, true, true, Now).Kind);
        }

        [Fact]
        public void Eof_during_a_tls_connect_is_TlsFailure_but_plain_eof_is_Network()
        {
            var ex = new MqttCommunicationException("EOF", new IOException("Received an unexpected EOF or 0 bytes from the transport stream."));
            Assert.Equal(ResilientMqttFailureKind.TlsFailure, FailureClassifier.FromException(ex, tlsEnabled: true, duringConnect: true, Now).Kind);
            Assert.Equal(ResilientMqttFailureKind.Network, FailureClassifier.FromException(ex, tlsEnabled: false, duringConnect: true, Now).Kind);
            Assert.Equal(ResilientMqttFailureKind.Network, FailureClassifier.FromException(ex, tlsEnabled: true, duringConnect: false, Now).Kind);
        }

        [Theory]
        [InlineData(SocketError.ConnectionReset)]
        [InlineData(SocketError.ConnectionAborted)]
        public void A_reset_while_connecting_with_tls_is_TlsFailure_without_tls_it_is_Network(SocketError error)   // #48
        {
            var ex = new MqttCommunicationException("forcibly closed", new SocketException((int)error));
            Assert.Equal(ResilientMqttFailureKind.TlsFailure, FailureClassifier.FromException(ex, tlsEnabled: true, duringConnect: true, Now).Kind);
            Assert.Equal(ResilientMqttFailureKind.Network, FailureClassifier.FromException(ex, tlsEnabled: false, duringConnect: true, Now).Kind);
            Assert.Equal(ResilientMqttFailureKind.Network, FailureClassifier.FromException(ex, tlsEnabled: true, duringConnect: false, Now).Kind);
        }

        [Fact]
        public void A_bare_connection_closed_while_connecting_is_Unknown_with_the_likely_causes()   // #48
        {
            var ex = new MqttCommunicationException("Connection closed.");
            var withTls = FailureClassifier.FromException(ex, true, true, Now);
            Assert.Equal(ResilientMqttFailureKind.Unknown, withTls.Kind);
            Assert.Contains("client certificate", withTls.Detail);
            Assert.Equal(ResilientMqttFailureKind.Unknown, FailureClassifier.FromException(ex, false, true, Now).Kind);
            Assert.Equal(ResilientMqttFailureKind.Network, FailureClassifier.FromException(ex, true, duringConnect: false, Now).Kind);
        }

        [Fact]
        public void A_communication_timeout_with_tls_stays_Timeout()
            => Assert.Equal(ResilientMqttFailureKind.Timeout,
                FailureClassifier.FromException(new MqttCommunicationTimedOutException(), true, true, Now).Kind);

        [Fact]
        public void A_refused_port_stays_BrokerUnavailable_even_with_tls()
            => Assert.Equal(ResilientMqttFailureKind.BrokerUnavailable, FailureClassifier.FromException(
                new MqttCommunicationException("x", new SocketException((int)SocketError.ConnectionRefused)), true, true, Now).Kind);

        [Fact]
        public void Non_mqtt_endpoint_is_ProtocolError()
        {
            var ex = new Exception("Error while authenticating.", new MqttProtocolViolationException("Property ID '46' is not supported"));
            Assert.Equal(ResilientMqttFailureKind.ProtocolError, FailureClassifier.FromException(ex, false, true, Now).Kind);
        }

        [Fact]
        public void Communication_timeout_is_Timeout()
            => Assert.Equal(ResilientMqttFailureKind.Timeout,
                FailureClassifier.FromException(new MqttCommunicationTimedOutException(), false, false, Now).Kind);

        [Fact]
        public void Unknown_exception_keeps_its_type_and_message()
        {
            var f = FailureClassifier.FromException(new InvalidOperationException("boom"), false, true, Now);
            Assert.Equal(ResilientMqttFailureKind.Unknown, f.Kind);
            Assert.Contains("boom", f.Detail);
            Assert.Equal(nameof(InvalidOperationException), f.ExceptionType);
        }

        [Fact]
        public void Aggregate_exceptions_are_unwrapped()
            => Assert.Equal(ResilientMqttFailureKind.BrokerUnavailable, FailureClassifier.FromException(
                new AggregateException(new SocketException((int)SocketError.ConnectionRefused)), false, true, Now).Kind);

        [Theory]
        [InlineData(MqttClientDisconnectReason.SessionTakenOver, ResilientMqttFailureKind.SessionTakenOver)]
        [InlineData(MqttClientDisconnectReason.NotAuthorized, ResilientMqttFailureKind.AuthRejected)]
        [InlineData(MqttClientDisconnectReason.ServerShuttingDown, ResilientMqttFailureKind.BrokerUnavailable)]
        [InlineData(MqttClientDisconnectReason.AdministrativeAction, ResilientMqttFailureKind.BrokerUnavailable)]
        [InlineData(MqttClientDisconnectReason.KeepAliveTimeout, ResilientMqttFailureKind.Timeout)]
        [InlineData(MqttClientDisconnectReason.ProtocolError, ResilientMqttFailureKind.ProtocolError)]
        [InlineData(MqttClientDisconnectReason.PacketTooLarge, ResilientMqttFailureKind.ProtocolError)]
        public void Broker_disconnect_reasons_map(MqttClientDisconnectReason reason, ResilientMqttFailureKind expected)
        {
            var f = FailureClassifier.FromDisconnect(reason, null, null, false, Now);
            Assert.Equal(expected, f.Kind);
            Assert.False(f.DuringConnect);
            Assert.Equal(reason.ToString(), f.Code);
        }

        [Fact]
        public void Plain_close_without_exception_is_Network()
            => Assert.Equal(ResilientMqttFailureKind.Network,
                FailureClassifier.FromDisconnect(MqttClientDisconnectReason.NormalDisconnection, null, null, false, Now).Kind);

        [Fact]
        public void Unspecified_disconnect_uses_the_exception()
        {
            var f = FailureClassifier.FromDisconnect(MqttClientDisconnectReason.UnspecifiedError, null,
                new MqttCommunicationException("x", new SocketException((int)SocketError.ConnectionReset)), false, Now);
            Assert.Equal(ResilientMqttFailureKind.Network, f.Kind);
            Assert.Equal("UnspecifiedError", f.Code);
        }

        [Fact]
        public void Connect_timeout_and_ping_timeout_are_Timeout()
        {
            var connect = FailureClassifier.ConnectTimeout(TimeSpan.FromSeconds(15), Now);
            Assert.Equal(ResilientMqttFailureKind.Timeout, connect.Kind);
            Assert.Contains("15 s", connect.Detail);
            Assert.True(connect.DuringConnect);

            var ping = FailureClassifier.PingTimeout(Now);
            Assert.Equal(ResilientMqttFailureKind.Timeout, ping.Kind);
            Assert.False(ping.DuringConnect);
        }

        [Fact]
        public void Long_details_are_trimmed()
            => Assert.True(FailureClassifier.FromException(new Exception(new string('x', 2000)), false, true, Now).Detail.Length <= 241);
    }
}

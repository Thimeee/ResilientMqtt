using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using MQTTnet;
using MQTTnet.Formatter;
using ResilientMqtt.Internal;

namespace ResilientMqtt.Tests.Unit
{
    public class SessionAndProtocolOptionsTests
    {
        [Fact]
        public void CleanSession_false_without_expiry_gets_the_one_day_default()   // #19
            => Assert.Equal(86_400u, ResilientMqttClient.EffectiveSessionExpirySeconds(new ResilientMqttOptions { CleanSession = false }));

        [Fact]
        public void CleanSession_true_without_expiry_gets_no_session()
            => Assert.Equal(0u, ResilientMqttClient.EffectiveSessionExpirySeconds(new ResilientMqttOptions { CleanSession = true }));

        [Theory]
        [InlineData(false, 0, 0u)]
        [InlineData(false, 300, 300u)]
        [InlineData(true, 600, 600u)]
        public void An_explicit_expiry_always_wins(bool clean, int seconds, uint expected)
            => Assert.Equal(expected, ResilientMqttClient.EffectiveSessionExpirySeconds(
                new ResilientMqttOptions { CleanSession = clean, SessionExpirySeconds = seconds }));

        [Theory]
        [InlineData("V500", MqttProtocolVersion.V500)]
        [InlineData("v5", MqttProtocolVersion.V500)]
        [InlineData("5.0", MqttProtocolVersion.V500)]
        [InlineData(null, MqttProtocolVersion.V500)]
        [InlineData("V311", MqttProtocolVersion.V311)]
        [InlineData("3.1.1", MqttProtocolVersion.V311)]
        public void Protocol_version_parses(string? value, MqttProtocolVersion expected)
            => Assert.Equal(expected, ResilientMqttClient.ParseProtocolVersion(value));

        [Fact]
        public void Unknown_protocol_version_throws_a_clear_error()
            => Assert.Contains("V500", Assert.Throws<InvalidOperationException>(() => ResilientMqttClient.ParseProtocolVersion("V6")).Message);
    }

    public class OptionsValidatorTests
    {
        [Fact]
        public void Defaults_are_valid()
            => Assert.Empty(ResilientMqttOptionsValidator.Check(new ResilientMqttOptions()));

        [Fact]
        public void Each_problem_gets_its_own_message()
        {
            var o = new ResilientMqttOptions { Port = 0, ProtocolVersion = "V9", SessionExpirySeconds = -1 };
            o.Reconnect.BaseDelaySeconds = 10;
            o.Reconnect.MaxDelaySeconds = 5;
            o.Dispatch.Mode = "Parallel";
            o.Dispatch.MaxConcurrency = 0;
            o.OfflineBuffer.Enabled = true;
            o.OfflineBuffer.MaxSize = 0;

            var errors = ResilientMqttOptionsValidator.Check(o);
            Assert.Contains(errors, e => e.Contains("Port"));
            Assert.Contains(errors, e => e.Contains("ProtocolVersion"));
            Assert.Contains(errors, e => e.Contains("SessionExpirySeconds"));
            Assert.Contains(errors, e => e.Contains("MaxDelaySeconds"));
            Assert.Contains(errors, e => e.Contains("Dispatch.Mode"));
            Assert.Contains(errors, e => e.Contains("MaxConcurrency"));
            Assert.Contains(errors, e => e.Contains("MaxSize"));
        }

        [Fact]
        public void Tls_modes_are_checked()
        {
            var hard = new ResilientMqttOptions(); hard.Tls.Enabled = true; hard.Tls.Mode = "Hard";
            Assert.Contains(ResilientMqttOptionsValidator.Check(hard), e => e.Contains("TrustedThumbprint"));

            var ca = new ResilientMqttOptions(); ca.Tls.Enabled = true; ca.Tls.Mode = "Ca"; ca.Tls.CaCertificatePath = @"C:\nope\ca.pem";
            Assert.Contains(ResilientMqttOptionsValidator.Check(ca), e => e.Contains("CaCertificatePath"));

            var both = new ResilientMqttOptions(); both.Tls.Enabled = true;
            both.Tls.ClientCertificatePath = @"C:\nope\c.pfx"; both.Tls.ClientCertificateThumbprint = "AB";
            Assert.Contains(ResilientMqttOptionsValidator.Check(both), e => e.Contains("not both"));

            var unknown = new ResilientMqttOptions(); unknown.Tls.Enabled = true; unknown.Tls.Mode = "Strict";
            Assert.Contains(ResilientMqttOptionsValidator.Check(unknown), e => e.Contains("unknown"));
        }

        [Fact]
        public void ValidateOnStart_surfaces_the_specific_message()
        {
            var services = new ServiceCollection().AddLogging();
            services.AddResilientMqtt(o => { o.Tls.Enabled = true; o.Tls.Mode = "Hard"; }).ValidateOnStart();
            using var sp = services.BuildServiceProvider();
            var ex = Assert.Throws<OptionsValidationException>(() => sp.GetRequiredService<IOptions<ResilientMqttOptions>>().Value);
            Assert.Contains("TrustedThumbprint", ex.Message);
        }

        [Fact]
        public async Task The_mqttnet_client_is_not_leaked_into_the_container()   // #34
        {
            var services = new ServiceCollection().AddLogging();
            services.AddResilientMqtt(o => o.Host = "127.0.0.1");
            await using var sp = services.BuildServiceProvider();
            Assert.Null(sp.GetService<IMqttClient>());
            Assert.NotNull(sp.GetRequiredService<IResilientMqttClient>());
        }
    }

    public class CircuitBreakerTests
    {
        private DateTime _now = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private CircuitBreaker Breaker(int threshold = 1) => new(threshold, TimeSpan.FromMinutes(1), NullLogger.Instance, () => _now);

        private void OpenIt(CircuitBreaker b)
        {
            Assert.True(b.TryAcquire(out _));
            b.RecordFailure();
            Assert.Equal(CircuitState.Open, b.State);
        }

        [Fact]
        public void Half_open_admits_exactly_one_probe()   // #10
        {
            var b = Breaker();
            OpenIt(b);
            _now = _now.AddMinutes(2);
            Assert.True(b.TryAcquire(out var probe));
            Assert.True(probe);
            Assert.False(b.TryAcquire(out _));
        }

        [Fact]
        public void An_abandoned_probe_frees_the_slot()   // #28
        {
            var b = Breaker();
            OpenIt(b);
            _now = _now.AddMinutes(2);
            Assert.True(b.TryAcquire(out var probe));
            b.RecordAbandoned(probe);
            Assert.True(b.TryAcquire(out var next));
            Assert.True(next);
        }

        [Fact]
        public void Abandoning_a_non_probe_does_not_release_someone_elses_probe()
        {
            var b = Breaker();
            OpenIt(b);
            _now = _now.AddMinutes(2);
            Assert.True(b.TryAcquire(out _));          // the probe
            b.RecordAbandoned(isProbe: false);         // a late, normal caller
            Assert.False(b.TryAcquire(out _));
        }

        [Fact]
        public void Success_reports_when_it_closes_the_circuit()   // #29
        {
            var b = Breaker();
            Assert.False(b.RecordSuccess());
            OpenIt(b);
            _now = _now.AddMinutes(2);
            Assert.True(b.TryAcquire(out _));
            Assert.True(b.RecordSuccess());
            Assert.Equal(CircuitState.Closed, b.State);
        }

        [Fact]
        public void A_failed_probe_reopens()
        {
            var b = Breaker();
            OpenIt(b);
            _now = _now.AddMinutes(2);
            Assert.True(b.TryAcquire(out _));
            b.RecordFailure();
            Assert.Equal(CircuitState.Open, b.State);
            Assert.False(b.TryAcquire(out _));
        }
    }

    public class TlsConfiguratorTests
    {
        private static (X509Certificate2 Ca, X509Certificate2 Leaf) IssueChain(string caName = "CN=Test CA")
        {
            using var caKey = RSA.Create(2048);
            var caReq = new CertificateRequest(caName, caKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            caReq.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
            caReq.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
            var ca = caReq.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(1));

            using var leafKey = RSA.Create(2048);
            var leafReq = new CertificateRequest("CN=broker", leafKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            var leaf = leafReq.Create(ca, DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddMonths(6), Guid.NewGuid().ToByteArray()[..8]);
            return (ca, leaf);
        }

        [Fact]
        public void A_certificate_issued_by_the_trusted_ca_is_accepted()   // #37
        {
            var (ca, leaf) = IssueChain();
            Assert.True(TlsConfigurator.ValidateAgainstCa(leaf, null, SslPolicyErrors.RemoteCertificateChainErrors, ca, checkName: true));
        }

        [Fact]
        public void A_certificate_from_another_ca_is_rejected()
        {
            var (_, leaf) = IssueChain("CN=Real CA");
            var (otherCa, _) = IssueChain("CN=Other CA");
            Assert.False(TlsConfigurator.ValidateAgainstCa(leaf, null, SslPolicyErrors.None, otherCa, checkName: true));
        }

        [Fact]
        public void A_name_mismatch_is_rejected_unless_name_checking_is_off()
        {
            var (ca, leaf) = IssueChain();
            Assert.False(TlsConfigurator.ValidateAgainstCa(leaf, null, SslPolicyErrors.RemoteCertificateNameMismatch, ca, checkName: true));
            Assert.True(TlsConfigurator.ValidateAgainstCa(leaf, null, SslPolicyErrors.RemoteCertificateNameMismatch, ca, checkName: false));
        }

        [Fact]
        public void No_certificate_is_rejected()
        {
            var (ca, _) = IssueChain();
            Assert.False(TlsConfigurator.ValidateAgainstCa(null, null, SslPolicyErrors.RemoteCertificateNotAvailable, ca, true));
        }

        [Theory]
        [InlineData("f7 98 46 0b", "F798460B")]
        [InlineData("\u200ef798460b", "F798460B")]   // invisible mark Windows adds when copying a thumbprint
        [InlineData("F7:98:46:0B", "F798460B")]
        public void Thumbprints_are_normalised(string input, string expected)
            => Assert.Equal(expected, TlsConfigurator.NormalizeThumbprint(input));

        [Theory]
        [InlineData("", "None")]
        [InlineData(null, "None")]
        [InlineData(" none ", "None")]
        [InlineData("normal", "Normal")]
        [InlineData("HARD", "Hard")]
        [InlineData("Ca", "Ca")]
        public void Tls_modes_parse_in_one_place(string? value, string expected)   // R10
            => Assert.Equal(expected, TlsConfigurator.ParseMode(value)?.ToString());

        [Fact]
        public void Unknown_tls_mode_does_not_crash_startup()   // R6 — 1.0.3 fell back to OS validation
        {
            var o = new TlsOptions { Enabled = true, Mode = "Strict" };
            Assert.Null(TlsConfigurator.ParseMode(o.Mode));
            TlsConfigurator.Configure(new MqttClientTlsOptionsBuilder(), o, NullLogger.Instance);   // no throw
        }

        [Fact]
        public void Missing_client_certificate_fails_fast()
        {
            var o = new TlsOptions { Enabled = true, Mode = "None", ClientCertificatePath = @"C:\nope\client.pfx" };
            Assert.Contains("ClientCertificatePath", Assert.Throws<InvalidOperationException>(() =>
                TlsConfigurator.Configure(new MqttClientTlsOptionsBuilder(), o, NullLogger.Instance)).Message);
        }
    }
}

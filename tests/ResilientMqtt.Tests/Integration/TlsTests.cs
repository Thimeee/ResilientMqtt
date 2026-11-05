using ResilientMqtt.Tests.Infrastructure;

namespace ResilientMqtt.Tests.Integration
{
    // End-to-end TLS against a real TLS endpoint (in-process broker + throwaway PKI) — #37 / #11.
    public class TlsTests
    {
        private static ResilientMqttOptions Tls(int port, Action<TlsOptions> tls)
            => TestClients.Options(port, o =>
            {
                o.Reconnect.Enabled = false;
                o.Reconnect.ConnectTimeoutSeconds = 6;
                o.Tls.Enabled = true;
                tls(o.Tls);
            });

        [Fact]
        public async Task Hard_mode_connects_with_the_pinned_thumbprint_and_refuses_another()
        {
            var ca = TestPki.CreateCa();
            var server = TestPki.CreateServerCert(ca);
            await using var broker = await TestBroker.StartAsync(tlsCertificate: server);

            await using (var ok = TestClients.Create(Tls(broker.Port, t => { t.Mode = "Hard"; t.TrustedThumbprint = server.Thumbprint.ToLowerInvariant(); })))
                Assert.True(await ok.ConnectAsync());

            await using var bad = TestClients.Create(Tls(broker.Port, t => { t.Mode = "Hard"; t.TrustedThumbprint = new string('A', 40); }));
            Assert.False(await bad.ConnectAsync());
            Assert.Equal(ResilientMqttFailureKind.TlsFailure, bad.LastFailure?.Kind);
        }

        [Fact]
        public async Task Ca_mode_trusts_certificates_issued_by_the_configured_ca_only()
        {
            var ca = TestPki.CreateCa("CN=Bank CA");
            var otherCa = TestPki.CreateCa("CN=Somebody Else CA");
            await using var broker = await TestBroker.StartAsync(tlsCertificate: TestPki.CreateServerCert(ca));

            await using (var ok = TestClients.Create(Tls(broker.Port, t => { t.Mode = "Ca"; t.CaCertificatePath = TestPki.WritePem(ca); })))
                Assert.True(await ok.ConnectAsync());

            await using var wrongCa = TestClients.Create(Tls(broker.Port, t => { t.Mode = "Ca"; t.CaCertificatePath = TestPki.WritePem(otherCa); }));
            Assert.False(await wrongCa.ConnectAsync());
            Assert.Equal(ResilientMqttFailureKind.TlsFailure, wrongCa.LastFailure?.Kind);
        }

        [Fact]
        public async Task Ca_mode_checks_the_certificate_name_unless_told_not_to()
        {
            var ca = TestPki.CreateCa();
            var wrongName = TestPki.CreateServerCert(ca, withLoopbackNames: false, subject: "CN=broker.bank.example");
            await using var broker = await TestBroker.StartAsync(tlsCertificate: wrongName);
            var caPath = TestPki.WritePem(ca);

            await using (var strict = TestClients.Create(Tls(broker.Port, t => { t.Mode = "Ca"; t.CaCertificatePath = caPath; })))
            {
                Assert.False(await strict.ConnectAsync());
                Assert.Equal(ResilientMqttFailureKind.TlsFailure, strict.LastFailure?.Kind);
            }

            await using var lenient = TestClients.Create(Tls(broker.Port, t => { t.Mode = "Ca"; t.CaCertificatePath = caPath; t.CheckCertificateName = false; }));
            Assert.True(await lenient.ConnectAsync());
        }

        [Fact]
        public async Task None_mode_uses_os_validation_and_rejects_a_private_ca()
        {
            var ca = TestPki.CreateCa();
            await using var broker = await TestBroker.StartAsync(tlsCertificate: TestPki.CreateServerCert(ca));

            await using var client = TestClients.Create(Tls(broker.Port, t => t.Mode = "None"));
            Assert.False(await client.ConnectAsync());
            Assert.Equal(ResilientMqttFailureKind.TlsFailure, client.LastFailure?.Kind);
        }

        [Fact]
        public async Task Normal_mode_accepts_any_certificate_and_warns()
        {
            var ca = TestPki.CreateCa();
            await using var broker = await TestBroker.StartAsync(tlsCertificate: TestPki.CreateServerCert(ca));
            var log = new ListLogger<ResilientMqttClient>();

            await using var client = TestClients.Create(Tls(broker.Port, t => t.Mode = "Normal"), log);
            Assert.True(await client.ConnectAsync());
            Assert.True(log.Has(ResilientMqttLogEvents.TlsInsecure));
        }

        [Fact]
        public async Task Mutual_tls_connects_with_a_client_certificate_and_fails_without_one()
        {
            var ca = TestPki.CreateCa();
            await using var broker = await TestBroker.StartAsync(
                tlsCertificate: TestPki.CreateServerCert(ca), requireClientCertificate: true, clientCa: ca);
            var caPath = TestPki.WritePem(ca);
            var (pfx, password, thumbprint) = TestPki.CreateClientPfx(ca);
            try
            {
                await using (var withCert = TestClients.Create(Tls(broker.Port, t =>
                {
                    t.Mode = "Ca"; t.CaCertificatePath = caPath;
                    t.ClientCertificatePath = pfx; t.ClientCertificatePassword = password;
                })))
                {
                    Assert.True(await withCert.ConnectAsync());
                    Assert.Equal(thumbprint, broker.LastClientCertificateThumbprint);
                }

                await using var withoutCert = TestClients.Create(Tls(broker.Port, t => { t.Mode = "Ca"; t.CaCertificatePath = caPath; }));
                Assert.False(await withoutCert.ConnectAsync());
                // TLS 1.3: the broker rejects the missing certificate after the client's handshake → a bare
                // "Connection closed" — reported honestly as Unknown, naming the client certificate as the likely cause (#48)
                Assert.Equal(ResilientMqttFailureKind.Unknown, withoutCert.LastFailure?.Kind);
                Assert.Contains("client certificate", withoutCert.LastFailure!.Detail);
            }
            finally { File.Delete(pfx); }
        }

        [Fact]
        public void Wrong_pfx_password_fails_fast_at_startup()
        {
            var ca = TestPki.CreateCa();
            var (pfx, _, _) = TestPki.CreateClientPfx(ca);
            try
            {
                var ex = Assert.Throws<InvalidOperationException>(() =>
                    TestClients.Create(Tls(1, t => { t.Mode = "Normal"; t.ClientCertificatePath = pfx; t.ClientCertificatePassword = "wrong"; })));
                Assert.Contains("wrong password", ex.Message);
            }
            finally { File.Delete(pfx); }
        }

        [Fact]
        public async Task Tls_against_a_plain_listener_is_TlsFailure()
        {
            await using var plain = await TestBroker.StartAsync();
            await using var client = TestClients.Create(Tls(plain.Port, t => t.Mode = "Normal"));
            Assert.False(await client.ConnectAsync());
            Assert.Equal(ResilientMqttFailureKind.TlsFailure, client.LastFailure?.Kind);   // this broker RESETS (Mosquitto sends EOF) — both are TLS (#48)
        }
    }
}

using System.Collections.Concurrent;
using System.Security.Cryptography.X509Certificates;
using ResilientMqtt.TestKit;

namespace ResilientMqtt.ChaosTests
{
    // Production broker specifics: authentication, ACL, TLS / mutual TLS, restart with persistence, MQTT 3.1.1 Will.
    [Collection("chaos")]
    public class MosquittoTests(ChaosFixture fixture, ITestOutputHelper output) : ChaosTestBase(fixture, output)
    {
        private static string Cert(string file) => Path.Combine(ChaosEnvironment.CertDir, file);

        private static ResilientMqttOptions Tls(int port, Action<TlsOptions> tls) => ChaosEnvironment.Options(ChaosEnvironment.Mosquitto, false, o =>
        {
            o.Host = "localhost";   // the test server certificate is issued for "localhost" / 127.0.0.1
            o.Port = port;
            o.Reconnect.Enabled = false;
            o.Tls.Enabled = true;
            tls(o.Tls);
        });

        [SkippableFact]
        public async Task Wrong_password_is_AuthRejected()
        {
            var b = await RequireAsync();
            await using var client = ChaosEnvironment.CreateClient(ChaosEnvironment.Options(b, false, o => { o.Password = "wrong"; o.Reconnect.Enabled = false; }));
            Assert.False(await client.ConnectAsync());
            LogFailure("wrong password", client.LastFailure);
            Assert.Equal(ResilientMqttFailureKind.AuthRejected, client.LastFailure?.Kind);
        }

        [SkippableFact]
        public async Task Acl_refusals_are_reported()
        {
            var b = await RequireAsync();
            await using var client = ChaosEnvironment.CreateClient(ChaosEnvironment.Options(b, false));
            Assert.True(await client.ConnectAsync());

            // Publish refusals are reported by every Mosquitto ACL backend (PUBACK NotAuthorized on MQTT 5).
            Assert.Equal(ResilientMqttPublishOutcome.Denied, await client.PublishWithResultAsync("denied/x", "no", useTopicPrefix: false));
            Assert.Equal(ResilientMqttPublishOutcome.Sent, await client.PublishWithResultAsync("chaos/allowed", "yes", useTopicPrefix: false));
            Assert.Equal(1, client.ConnectionInfo.DeniedPublishes);

            // Subscribe refusals depend on the ACL backend: the dynamic-security plugin (production) answers the SUBACK
            // with NotAuthorized — proven against the lab broker — but the classic acl_file used here GRANTS the
            // SUBACK and silently filters delivery, so no client can see that refusal. Record what this broker does.
            await client.SubscribeAsync("denied/#", (_, _, _) => Task.CompletedTask);
            var refused = client.ConnectionInfo.RejectedSubscriptions.ContainsKey("denied/#");
            Log($"acl_file SUBACK for denied/# → {(refused ? "refused (reported)" : "granted, delivery filtered by the broker (not detectable)")}");
        }

        [SkippableFact]
        public async Task Tls_modes_against_mosquitto()
        {
            await RequireAsync();
            var serverThumbprint = X509Certificate2.CreateFromPem(File.ReadAllText(Cert("server.pem"))).Thumbprint;

            await using (var ca = ChaosEnvironment.CreateClient(Tls(ChaosEnvironment.MosquittoTlsPort, t => { t.Mode = "Ca"; t.CaCertificatePath = Cert("ca.pem"); })))
                Assert.True(await ca.ConnectAsync());

            await using (var hard = ChaosEnvironment.CreateClient(Tls(ChaosEnvironment.MosquittoTlsPort, t => { t.Mode = "Hard"; t.TrustedThumbprint = serverThumbprint; })))
                Assert.True(await hard.ConnectAsync());

            await using (var wrong = ChaosEnvironment.CreateClient(Tls(ChaosEnvironment.MosquittoTlsPort, t => { t.Mode = "Hard"; t.TrustedThumbprint = new string('A', 40); })))
            {
                Assert.False(await wrong.ConnectAsync());
                LogFailure("wrong thumbprint", wrong.LastFailure);
                Assert.Equal(ResilientMqttFailureKind.TlsFailure, wrong.LastFailure?.Kind);
            }

            await using (var os = ChaosEnvironment.CreateClient(Tls(ChaosEnvironment.MosquittoTlsPort, t => t.Mode = "None")))
            {
                Assert.False(await os.ConnectAsync());   // private CA → OS validation rejects it
                LogFailure("OS validation", os.LastFailure);
                Assert.Equal(ResilientMqttFailureKind.TlsFailure, os.LastFailure?.Kind);
            }

            await using var plainPort = ChaosEnvironment.CreateClient(Tls(ChaosEnvironment.Mosquitto.DirectPort, t => t.Mode = "Normal"));
            Assert.False(await plainPort.ConnectAsync());
            LogFailure("TLS against the plain port", plainPort.LastFailure);
            Assert.Equal(ResilientMqttFailureKind.TlsFailure, plainPort.LastFailure?.Kind);
        }

        [SkippableFact]
        public async Task Mutual_tls_against_mosquitto()
        {
            await RequireAsync();
            await using (var withCert = ChaosEnvironment.CreateClient(Tls(ChaosEnvironment.MosquittoMutualTlsPort, t =>
            {
                t.Mode = "Ca";
                t.CaCertificatePath = Cert("ca.pem");
                t.ClientCertificatePath = Cert("client.pfx");
                t.ClientCertificatePassword = ChaosEnvironment.ClientPfxPassword;
            })))
                Assert.True(await withCert.ConnectAsync());

            await using var withoutCert = ChaosEnvironment.CreateClient(Tls(ChaosEnvironment.MosquittoMutualTlsPort, t => { t.Mode = "Ca"; t.CaCertificatePath = Cert("ca.pem"); }));
            Assert.False(await withoutCert.ConnectAsync());
            LogFailure("mutual TLS without a client certificate", withoutCert.LastFailure);
            Assert.Contains(withoutCert.LastFailure!.Kind, new[] { ResilientMqttFailureKind.TlsFailure, ResilientMqttFailureKind.Unknown });
        }

        [SkippableFact]
        public async Task A_broker_restart_resumes_the_persistent_session()
        {
            var b = await RequireAsync();
            var topic = $"chaos/restart/{Guid.NewGuid():N}";
            var received = new ConcurrentQueue<string>();
            ResilientMqttConnectionRestoredArgs? restored = null;

            await using var client = ChaosEnvironment.CreateClient(
                ChaosEnvironment.Options(b, false, o => { o.ClientId = $"chaos-rst-{Guid.NewGuid():N}"[..30]; o.CleanSession = false; }),
                retryDelay: TimeSpan.FromSeconds(1));
            await client.SubscribeAsync(topic, (_, p, _) => { received.Enqueue(p); return Task.CompletedTask; });
            client.ConnectionRestored += a => { restored = a; return Task.CompletedTask; };
            Assert.True(await client.ConnectAsync());

            await ChaosEnvironment.DockerAsync($"restart {b.Container}");
            Assert.True(await client.WaitForConnectionAsync(TimeSpan.FromSeconds(60)));
            Assert.True(await ChaosEnvironment.Eventually(() => restored is not null, TimeSpan.FromSeconds(10)));
            Log($"restored after {restored!.Downtime.TotalSeconds:F1} s ({restored.LastFailure?.Kind}); sessionPresent={restored.SessionPresent}");
            Assert.True(restored.SessionPresent);

            await using var publisher = ChaosEnvironment.CreateClient(ChaosEnvironment.Options(b, false));
            Assert.True(await publisher.ConnectAsync());
            await publisher.PublishWithResultAsync(topic, "after-restart", useTopicPrefix: false);
            Assert.True(await ChaosEnvironment.Eventually(() => received.Contains("after-restart"), TimeSpan.FromSeconds(10)));
        }

        [SkippableFact]
        public async Task V311_zombie_reset_tells_the_server_through_the_will()
        {
            var b = await RequireAsync();
            var id = Guid.NewGuid().ToString("N")[..10];
            var statusTopic = $"chaos/v311/{id}/status";
            var statuses = new ConcurrentQueue<string>();

            await using var observer = ChaosEnvironment.CreateClient(ChaosEnvironment.Options(b, false));
            await observer.SubscribeAsync(statusTopic, (_, p, _) => { statuses.Enqueue(p); return Task.CompletedTask; });
            Assert.True(await observer.ConnectAsync());

            await using var kiosk = ChaosEnvironment.CreateClient(ChaosEnvironment.Options(b, viaProxy: true, o =>
            {
                o.ProtocolVersion = "V311";
                o.TopicPrefix = $"chaos/v311/{id}";
                o.LastWill.Enabled = true;
                o.LastWill.OnlinePayload = "online";
                o.LastWill.OfflinePayload = "connection_lost";
                o.HealthCheck.PingTimeoutSeconds = 2;
            }), healthInterval: TimeSpan.FromSeconds(1), retryDelay: TimeSpan.FromSeconds(1));
            Assert.True(await kiosk.ConnectAsync());
            Assert.True(await ChaosEnvironment.Eventually(() => statuses.Contains("online"), TimeSpan.FromSeconds(10)));

            await Fixture.Toxi.AddToxicAsync(b, "hole", "timeout", "downstream", new { timeout = 0 });   // broker → kiosk dead
            Assert.True(await ChaosEnvironment.Eventually(() => statuses.Contains("connection_lost"), TimeSpan.FromSeconds(30)),
                "the server was never told: " + string.Join(",", statuses));
        }
    }
}

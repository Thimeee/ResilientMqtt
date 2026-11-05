using System.Collections.Concurrent;
using System.Security.Cryptography.X509Certificates;
using MQTTnet;
using MQTTnet.Exceptions;
using MQTTnet.Protocol;
using ResilientMqtt.Tests.Infrastructure;

namespace ResilientMqtt.Tests.Integration
{
    // T1 — the paths the honest coverage list said were "reviewed in code only", now driven deterministically by a
    // fault-injection decorator around the real MQTTnet client.
    public class FaultInjectionTests
    {
        [Fact]
        public async Task A_connack_that_lands_right_at_the_timeout_boundary_counts_as_connected()   // #47
        {
            await using var broker = await TestBroker.StartAsync();
            var faulty = new FaultyMqttClient(new MqttClientFactory().CreateMqttClient());
            faulty.ConnectOverride = async (o, _) =>
            {
                var result = await faulty.Inner.ConnectAsync(o, CancellationToken.None);   // CONNACK received…
                await Task.Delay(1300);                                                     // …but we resume after the 1 s timer fired
                return result;
            };
            var log = new ListLogger<ResilientMqttClient>();
            await using var client = TestClients.Create(TestClients.Options(broker.Port, o => o.Reconnect.ConnectTimeoutSeconds = 1),
                log, mqttClient: faulty);

            Assert.True(await client.ConnectAsync());
            Assert.Null(client.LastFailure);
            Assert.False(log.Has(ResilientMqttLogEvents.ConnectTimedOut));
        }

        [Fact]
        public async Task A_shutdown_cancel_wrapped_by_mqttnet_is_not_recorded_as_a_failure()   // #50, #67
        {
            var faulty = new FaultyMqttClient(new MqttClientFactory().CreateMqttClient());
            faulty.ConnectOverride = async (_, ct) =>
            {
                try { await Task.Delay(Timeout.Infinite, ct); }
                catch (OperationCanceledException oce) { throw new MqttCommunicationException(oce); }   // how MQTTnet wraps it
                throw new InvalidOperationException("unreachable");
            };
            var log = new ListLogger<ResilientMqttClient>();
            var client = TestClients.Create(TestClients.Options(Net.FreePort(), o =>
            {
                o.Reconnect.ConnectTimeoutSeconds = 30;   // our own timeout must not be what ends it
                o.Tls.Enabled = true;                     // a wrapped cancel must not become "TLS failure" either
                o.Tls.Mode = "Normal";
            }), log, mqttClient: faulty);

            var connecting = client.ConnectAsync();
            await Task.Delay(300);
            var sw = System.Diagnostics.Stopwatch.StartNew();
            await client.DisposeAsync();                                   // shutdown cancels the in-flight connect (#67)
            Assert.False(await connecting.WaitAsync(TimeSpan.FromSeconds(5)));
            Assert.True(sw.Elapsed < TimeSpan.FromSeconds(5), $"shutdown waited {sw.Elapsed.TotalSeconds:F1} s for the connect");

            Assert.Null(client.LastFailure);
            Assert.False(log.Has(ResilientMqttLogEvents.ConnectFailed));
            Assert.False(log.Has(ResilientMqttLogEvents.ConnectTimedOut));
            Assert.NotEqual(ResilientMqttConnectionState.Reconnecting, client.State);
        }

        [Fact]
        public async Task A_caller_token_cancelled_during_set_up_does_not_leave_a_half_initialised_connection()   // #54
        {
            await using var broker = await TestBroker.StartAsync();
            var faulty = new FaultyMqttClient(new MqttClientFactory().CreateMqttClient());
            faulty.SubscribeOverride = async (o, ct) => { await Task.Delay(800, ct); return await faulty.Inner.SubscribeAsync(o, ct); };   // slow SUBACK
            var onConnected = false;
            await using var client = TestClients.Create(TestClients.Options(broker.Port), mqttClient: faulty);
            await client.SubscribeAsync("pre/registered", (_, _, _) => Task.CompletedTask);   // restored during set-up
            client.OnConnected += () => { onConnected = true; return Task.CompletedTask; };

            using var caller = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));   // fires mid-set-up
            Assert.True(await client.ConnectAsync(caller.Token));
            Assert.True(caller.IsCancellationRequested);
            Assert.True(onConnected);
            Assert.True(await client.WaitForConnectionAsync(TimeSpan.FromSeconds(1)));
        }

        [Fact]
        public async Task A_full_hold_queue_applies_back_pressure_and_drops_nothing()   // #65 at the limit
        {
            await using var broker = await TestBroker.StartAsync();
            var clientId = "bp-" + Guid.NewGuid().ToString("N")[..6];
            Action<ResilientMqttOptions> cfg = o => { o.ClientId = clientId; o.CleanSession = false; };
            await using (var run1 = TestClients.Create(TestClients.Options(broker.Port, cfg)))
            {
                run1.OnConnected += () => run1.SubscribeAsync("h/#", (_, _, _) => Task.CompletedTask);
                Assert.True(await run1.ConnectAsync());
                Assert.True(await Net.Eventually(() => broker.Subscriptions.Any(s => s.ClientId == clientId)));
            }
            using (var server = await broker.RawClientAsync())
                for (var i = 1; i <= 8; i++)
                    await server.PublishStringAsync("h/x", $"m{i}", MqttQualityOfServiceLevel.AtLeastOnce);

            var received = new ConcurrentQueue<string>();
            await using var run2 = TestClients.Create(TestClients.Options(broker.Port, cfg), heldMessageLimit: 3);   // 8 queued, room for 3
            run2.OnConnected += async () =>
            {
                await Task.Delay(500);
                await run2.SubscribeAsync("h/#", (_, p, _) => { received.Enqueue(p); return Task.CompletedTask; });
            };
            Assert.True(await run2.ConnectAsync());

            Assert.True(await Net.Eventually(() => received.Count == 8, 8000), $"received {received.Count} of 8");
            Assert.Equal(Enumerable.Range(1, 8).Select(i => $"m{i}"), received.ToArray());
        }

        [Fact]
        public async Task Mutual_tls_with_a_client_certificate_from_the_certificate_store()   // #37, store variant
        {
            var ca = TestPki.CreateCa();
            await using var broker = await TestBroker.StartAsync(
                tlsCertificate: TestPki.CreateServerCert(ca), requireClientCertificate: true, clientCa: ca);
            var (pfx, password, thumbprint) = TestPki.CreateClientPfx(ca);
            using var store = new X509Store(StoreName.My, StoreLocation.CurrentUser);
            store.Open(OpenFlags.ReadWrite);
            var cert = X509CertificateLoader.LoadPkcs12FromFile(pfx, password, X509KeyStorageFlags.PersistKeySet | X509KeyStorageFlags.UserKeySet);
            store.Add(cert);
            try
            {
                await using var client = TestClients.Create(TestClients.Options(broker.Port, o =>
                {
                    o.Reconnect.Enabled = false;
                    o.Tls.Enabled = true;
                    o.Tls.Mode = "Ca";
                    o.Tls.CaCertificatePath = TestPki.WritePem(ca);
                    o.Tls.ClientCertificateThumbprint = thumbprint;
                    o.Tls.ClientCertificateStore = "CurrentUser";
                }));
                Assert.True(await client.ConnectAsync());
                Assert.Equal(thumbprint, broker.LastClientCertificateThumbprint);
            }
            finally
            {
                store.Remove(cert);   // leave the user's certificate store exactly as it was
                File.Delete(pfx);
            }
        }
    }
}

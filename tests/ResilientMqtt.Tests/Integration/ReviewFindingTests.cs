using System.Collections.Concurrent;
using MQTTnet;
using MQTTnet.Protocol;
using ResilientMqtt.Internal;
using ResilientMqtt.Tests.Infrastructure;

namespace ResilientMqtt.Tests.Integration
{
    // Findings R1–R7 from the independent code review of the 1.1.0 diff.
    public class ReviewFindingTests
    {
        [Fact]
        public async Task Held_messages_are_not_lost_when_the_process_stops_before_OnConnected_finishes()   // R1
        {
            await using var broker = await TestBroker.StartAsync();
            var clientId = "r1-" + Guid.NewGuid().ToString("N")[..6];
            Action<ResilientMqttOptions> cfg = o => { o.ClientId = clientId; o.CleanSession = false; o.Reconnect.ConnectTimeoutSeconds = 1; };

            await using (var run1 = TestClients.Create(TestClients.Options(broker.Port, cfg)))
            {
                run1.OnConnected += () => run1.SubscribeAsync("cmd/#", (_, _, _) => Task.CompletedTask);
                Assert.True(await run1.ConnectAsync());
                Assert.True(await Net.Eventually(() => broker.Subscriptions.Any(s => s.ClientId == clientId)));
            }
            using (var server = await broker.RawClientAsync())
                for (var i = 1; i <= 3; i++)
                    await server.PublishStringAsync("cmd/run", $"c{i}", MqttQualityOfServiceLevel.AtLeastOnce);

            // Run 2: the queue arrives and is held… then the process stops before OnConnected ever subscribes.
            var run2 = TestClients.Create(TestClients.Options(broker.Port, cfg));
            var stop = new TaskCompletionSource();
            run2.OnConnected += async () => { await stop.Task; };
            var connecting = run2.ConnectAsync();
            await Task.Delay(500);
            await run2.DisposeAsync();   // the app stops WHILE OnConnected is still running — nothing reached a handler
            stop.SetResult();            // let the abandoned set-up unwind (it must not replay into the stopped app)
            await connecting;

            // Run 3: the commands must still arrive (held = not acknowledged = redelivered by the broker).
            var received = new ConcurrentQueue<string>();
            await using var run3 = TestClients.Create(TestClients.Options(broker.Port, cfg));
            await run3.SubscribeAsync("cmd/#", (_, p, _) => { received.Enqueue(p); return Task.CompletedTask; });
            Assert.True(await run3.ConnectAsync());

            Assert.True(await Net.Eventually(() => received.Count >= 3, 5000), $"received {received.Count} of 3");
            Assert.Equal(new[] { "c1", "c2", "c3" }, received.Distinct().ToArray());
        }

        [Fact]
        public async Task Replayed_held_messages_are_acknowledged_and_not_redelivered_after_a_reconnect()   // R1
        {
            await using var broker = await TestBroker.StartAsync();
            var clientId = "r1b-" + Guid.NewGuid().ToString("N")[..6];
            Action<ResilientMqttOptions> cfg = o => { o.ClientId = clientId; o.CleanSession = false; };

            await using (var run1 = TestClients.Create(TestClients.Options(broker.Port, cfg)))
            {
                run1.OnConnected += () => run1.SubscribeAsync("cmd/#", (_, _, _) => Task.CompletedTask);
                Assert.True(await run1.ConnectAsync());
                Assert.True(await Net.Eventually(() => broker.Subscriptions.Any(s => s.ClientId == clientId)));
            }
            using (var server = await broker.RawClientAsync())
                await server.PublishStringAsync("cmd/run", "once", MqttQualityOfServiceLevel.AtLeastOnce);

            var received = new ConcurrentQueue<string>();
            await using var run2 = TestClients.Create(TestClients.Options(broker.Port, cfg));
            run2.OnConnected += async () =>
            {
                await Task.Delay(300);
                await run2.SubscribeAsync("cmd/#", (_, p, _) => { received.Enqueue(p); return Task.CompletedTask; });
            };
            Assert.True(await run2.ConnectAsync());
            Assert.True(await Net.Eventually(() => received.Count == 1));
            Assert.True(await Net.Eventually(() => broker.AckedByClients >= 1));   // the manual ack after the replay reached the broker

            await broker.KickAsync(clientId);   // reconnect into the same session
            Assert.True(await Net.Eventually(() => run2.ConnectionInfo.TotalConnects == 2 && run2.IsConnected));
            await Task.Delay(500);
            Assert.Single(received);   // acknowledged after replay → the broker did not send it again
        }

        [Fact]
        public async Task Disconnect_during_set_up_ends_with_the_graceful_status_and_no_health_timer()   // R3
        {
            await using var broker = await TestBroker.StartAsync();
            var healthLog = new ListLogger<IdleAwareHealthCheck>();
            var health = new IdleAwareHealthCheck(TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(1), healthLog);
            var o = TestClients.Options(broker.Port, x =>
            {
                x.TopicPrefix = "terminal/R3";
                x.LastWill.Enabled = true;
                x.LastWill.OnlinePayload = "online";
                x.LastWill.GracefulShutdownPayload = "offline-graceful";
                x.Reconnect.ConnectTimeoutSeconds = 5;
            });
            var inOnConnected = new TaskCompletionSource();
            await using var client = TestClients.Create(o, healthCheck: health);
            client.OnConnected += async () => { inOnConnected.TrySetResult(); await Task.Delay(400); };

            var connecting = client.ConnectAsync();
            await inOnConnected.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await client.DisconnectAsync();   // arrives while the set-up is still in OnConnected
            await connecting;

            var statuses = broker.Published.Where(p => p.Topic == "terminal/R3/status").Select(p => p.Payload).ToArray();
            Assert.Equal("offline-graceful", statuses.Last());

            var before = healthLog.Entries.Count;
            await Task.Delay(500);
            Assert.Equal(before, healthLog.Entries.Count);   // the health timer was not restarted after the disconnect
        }

        [Fact]
        public async Task A_drop_during_OnConnected_does_not_leave_a_stale_ready_signal()   // R4
        {
            await using var broker = await TestBroker.StartAsync();
            var o = TestClients.Options(broker.Port);
            var connects = 0;
            await using var client = TestClients.Create(o);
            client.OnConnected += async () =>
            {
                if (Interlocked.Increment(ref connects) == 1)
                {
                    await broker.KickAsync(o.ClientId);   // dropped while still setting up
                    await Task.Delay(200);
                }
            };

            await client.ConnectAsync();
            Assert.True(await client.WaitForConnectionAsync(TimeSpan.FromSeconds(8)));
            Assert.True(client.IsConnected);
            Assert.True(Volatile.Read(ref connects) >= 2);   // ready came from the SECOND (current) connection
        }

        [Fact]
        public async Task V311_zombie_reset_publishes_the_will_itself()   // R5
        {
            await using var broker = await TestBroker.StartAsync();
            using var proxy = new FreezableProxy(broker.Port);
            var health = new IdleAwareHealthCheck(TimeSpan.FromMilliseconds(300), TimeSpan.FromMilliseconds(400),
                TimeSpan.FromSeconds(1), Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance);
            await using var client = TestClients.Create(TestClients.Options(proxy.Port, x =>
            {
                x.ProtocolVersion = "V311";
                x.TopicPrefix = "terminal/R5";
                x.LastWill.Enabled = true;
                x.LastWill.OnlinePayload = "online";
                x.LastWill.OfflinePayload = "connection_lost";
            }), healthCheck: health);
            Assert.True(await client.ConnectAsync());

            proxy.FreezeDownstream();   // broker → client dead, client → broker still alive

            Assert.True(await Net.Eventually(() =>
                broker.Published.Any(p => p.Topic == "terminal/R5/status" && p.Payload == "connection_lost"), 10_000),
                "the clean V311 DISCONNECT discarded the Will and nothing told the server the kiosk was lost");
        }

        [Fact]
        public async Task Unknown_tls_mode_falls_back_to_os_validation_with_a_warning()   // R6 (1.0.3-compatible)
        {
            var ca = TestPki.CreateCa();
            await using var broker = await TestBroker.StartAsync(tlsCertificate: TestPki.CreateServerCert(ca));
            var log = new ListLogger<ResilientMqttClient>();
            await using var client = TestClients.Create(TestClients.Options(broker.Port, o =>
            {
                o.Reconnect.Enabled = false;
                o.Tls.Enabled = true;
                o.Tls.Mode = "Default";   // not a known mode — 1.0.3 meant "OS validation"
            }), log);   // must not throw at construction

            Assert.True(log.Has(ResilientMqttLogEvents.TlsInsecure));
            Assert.False(await client.ConnectAsync());   // OS validation rejects the private CA, exactly like "None"
            Assert.Equal(ResilientMqttFailureKind.TlsFailure, client.LastFailure?.Kind);
        }

        [Fact]
        public async Task CleanSession_true_with_an_expiry_warns_that_nothing_survives_a_reconnect()   // R7
        {
            await using var broker = await TestBroker.StartAsync();
            var log = new ListLogger<ResilientMqttClient>();
            await using var client = TestClients.Create(TestClients.Options(broker.Port, o =>
            {
                o.CleanSession = true;
                o.SessionExpirySeconds = 3600;
            }), log);
            Assert.Contains(log.Entries, e => e.Id.Id == ResilientMqttLogEvents.SessionMode.Id && e.Message.Contains("DISCARDS"));
            Assert.DoesNotContain(log.Entries, e => e.Message.Contains("persistent session ("));
        }
    }
}

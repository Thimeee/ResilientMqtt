using System.Collections.Concurrent;
using Microsoft.Extensions.Logging.Abstractions;
using MQTTnet;
using MQTTnet.Formatter;
using MQTTnet.Protocol;
using ResilientMqtt.Internal;
using ResilientMqtt.Tests.Infrastructure;

namespace ResilientMqtt.Tests.Integration
{
    // Real sockets, real (in-process) broker. Each test owns its broker on a random port.
    public class ConnectionTests
    {
        [Fact]
        public async Task Connects_and_reports_healthy_connection_info()
        {
            await using var broker = await TestBroker.StartAsync();
            await using var client = TestClients.Create(TestClients.Options(broker.Port));

            Assert.True(await client.ConnectAsync());
            var info = client.ConnectionInfo;
            Assert.True(info.IsConnected);
            Assert.Equal(ResilientMqttConnectionState.Connected, info.State);
            Assert.Equal(1, info.TotalConnects);
            Assert.Null(info.LastFailure);
            Assert.Null(info.DownSinceUtc);
            Assert.NotNull(info.ConnectedSinceUtc);
            Assert.Equal("V500", info.ProtocolVersion);
        }

        [Fact]
        public async Task CleanSession_false_really_asks_the_broker_for_a_session()   // #19
        {
            await using var broker = await TestBroker.StartAsync();
            await using var client = TestClients.Create(TestClients.Options(broker.Port, o => o.CleanSession = false));
            Assert.True(await client.ConnectAsync());

            var c = Assert.Single(broker.Connects);
            Assert.False(c.CleanSession);
            Assert.Equal(86_400u, c.SessionExpiry);
            Assert.Equal(MqttProtocolVersion.V500, c.Protocol);
        }

        [Fact]
        public async Task CleanSession_true_asks_for_no_session_and_V311_is_honoured()
        {
            await using var broker = await TestBroker.StartAsync();
            await using (var clean = TestClients.Create(TestClients.Options(broker.Port, o => o.CleanSession = true)))
                Assert.True(await clean.ConnectAsync());
            await using (var v311 = TestClients.Create(TestClients.Options(broker.Port, o => o.ProtocolVersion = "V311")))
                Assert.True(await v311.ConnectAsync());

            var all = broker.Connects.ToArray();
            Assert.Equal(0u, all[0].SessionExpiry);
            Assert.True(all[0].CleanSession);
            Assert.Equal(MqttProtocolVersion.V311, all[1].Protocol);
        }

        [Fact]
        public async Task A_persistent_session_receives_what_was_published_while_offline()   // #19 end-to-end
        {
            await using var broker = await TestBroker.StartAsync();
            var clientId = "sess-" + Guid.NewGuid().ToString("N")[..6];

            await using (var first = TestClients.Create(TestClients.Options(broker.Port, o => { o.ClientId = clientId; o.CleanSession = false; })))
            {
                Assert.True(await first.ConnectAsync());
                await first.SubscribeAsync("cmd/x", (_, _, _) => Task.CompletedTask);
            }   // graceful disconnect — the session stays on the broker (expiry 1 day)

            using (var publisher = await broker.RawClientAsync())
                await publisher.PublishStringAsync("cmd/x", "while-offline", MqttQualityOfServiceLevel.AtLeastOnce);

            var received = new ConcurrentQueue<string>();
            await using var second = TestClients.Create(TestClients.Options(broker.Port, o => { o.ClientId = clientId; o.CleanSession = false; }));
            await second.SubscribeAsync("cmd/x", (_, p, _) => { received.Enqueue(p); return Task.CompletedTask; });
            Assert.True(await second.ConnectAsync());

            Assert.True(await Net.Eventually(() => received.Contains("while-offline")));
            Assert.True(second.ConnectionInfo.SessionPresent);
        }

        [Fact]
        public async Task A_clean_session_does_not_receive_what_was_published_while_offline()
        {
            await using var broker = await TestBroker.StartAsync();
            var clientId = "clean-" + Guid.NewGuid().ToString("N")[..6];

            await using (var first = TestClients.Create(TestClients.Options(broker.Port, o => { o.ClientId = clientId; o.CleanSession = true; })))
            {
                Assert.True(await first.ConnectAsync());
                await first.SubscribeAsync("cmd/y", (_, _, _) => Task.CompletedTask);
            }

            using (var publisher = await broker.RawClientAsync())
                await publisher.PublishStringAsync("cmd/y", "stale-command", MqttQualityOfServiceLevel.AtLeastOnce);

            var received = new ConcurrentQueue<string>();
            await using var second = TestClients.Create(TestClients.Options(broker.Port, o => { o.ClientId = clientId; o.CleanSession = true; }));
            await second.SubscribeAsync("cmd/y", (_, p, _) => { received.Enqueue(p); return Task.CompletedTask; });
            Assert.True(await second.ConnectAsync());

            await Task.Delay(500);
            Assert.Empty(received);
            Assert.False(second.ConnectionInfo.SessionPresent);
        }

        [Fact]
        public async Task Persistent_session_with_auto_client_id_warns()   // #38
        {
            await using var broker = await TestBroker.StartAsync();
            var log = new ListLogger<ResilientMqttClient>();
            var o = TestClients.Options(broker.Port, x => { x.CleanSession = false; x.ClientId = ResilientMqttOptions.AutoClientIdPrefix + "abc"; });
            await using var client = TestClients.Create(o, log);
            Assert.True(log.Has(ResilientMqttLogEvents.SessionWithRandomClientId));
        }

        [Fact]
        public async Task Wrong_password_is_AuthRejected_and_OnDisconnected_is_not_fired_per_attempt()   // #20 #21
        {
            await using var broker = await TestBroker.StartAsync();
            broker.RequiredPassword = "right";
            var disconnectedCalls = 0;
            var lost = 0;
            await using var client = TestClients.Create(TestClients.Options(broker.Port, o => o.Password = "wrong"));
            client.OnDisconnected += () => { Interlocked.Increment(ref disconnectedCalls); return Task.CompletedTask; };
            client.ConnectionLost += _ => { Interlocked.Increment(ref lost); return Task.CompletedTask; };

            Assert.False(await client.ConnectAsync());
            Assert.True(await Net.Eventually(() => broker.Connects.Count >= 4));   // the reconnect loop kept trying

            Assert.Equal(ResilientMqttFailureKind.AuthRejected, client.LastFailure?.Kind);
            Assert.Equal(ResilientMqttConnectionState.Reconnecting, client.State);
            Assert.NotNull(client.ConnectionInfo.DownSinceUtc);
            Assert.True(client.ConnectionInfo.ReconnectAttempt >= 3);
            Assert.Equal(0, Volatile.Read(ref disconnectedCalls));   // it never WAS connected
            Assert.Equal(0, Volatile.Read(ref lost));
        }

        [Fact]
        public async Task Auth_rejection_slows_the_retries_when_configured()   // #36
        {
            await using var broker = await TestBroker.StartAsync();
            broker.RequiredPassword = "right";
            await using var client = TestClients.Create(TestClients.Options(broker.Port, o =>
            {
                o.Password = "wrong";
                o.Reconnect.AuthRejectedDelaySeconds = 3;
            }));

            Assert.False(await client.ConnectAsync());
            await Task.Delay(1500);
            Assert.Single(broker.Connects);   // fast retry (50 ms) would have made ~25 attempts by now
        }

        [Fact]
        public async Task Closed_port_is_BrokerUnavailable()
        {
            // Windows needs ~2 s to report a refused loopback port (it retries the SYN) — give it room.
            await using var client = TestClients.Create(TestClients.Options(Net.FreePort(), o =>
            {
                o.Reconnect.Enabled = false;
                o.Reconnect.ConnectTimeoutSeconds = 6;
            }));
            Assert.False(await client.ConnectAsync());
            Assert.Equal(ResilientMqttFailureKind.BrokerUnavailable, client.LastFailure?.Kind);
            Assert.Equal(ResilientMqttConnectionState.Disconnected, client.State);
        }

        [Fact]
        public async Task A_broker_that_never_answers_times_out_and_says_so()   // #1 #22
        {
            using var silent = new SilentListener();
            var log = new ListLogger<ResilientMqttClient>();
            await using var client = TestClients.Create(TestClients.Options(silent.Port, o =>
            {
                o.Reconnect.ConnectTimeoutSeconds = 1;
                o.Reconnect.Enabled = false;
            }), log);

            var sw = System.Diagnostics.Stopwatch.StartNew();
            Assert.False(await client.ConnectAsync());
            Assert.InRange(sw.Elapsed.TotalSeconds, 0.5, 5);

            Assert.Equal(ResilientMqttFailureKind.Timeout, client.LastFailure?.Kind);
            Assert.Equal(ResilientMqttConnectionState.Disconnected, client.State);   // not stuck at "Connecting"
            Assert.True(log.Has(ResilientMqttLogEvents.ConnectTimedOut));            // used to be silent
        }

        [Fact]
        public async Task Cold_start_recovers_when_the_broker_comes_up_and_WaitForConnection_returns()   // #13 #40
        {
            var port = Net.FreePort();
            ResilientMqttConnectionRestoredArgs? restored = null;
            await using var client = TestClients.Create(TestClients.Options(port, o => o.Reconnect.ConnectTimeoutSeconds = 6));
            client.ConnectionRestored += a => { restored = a; return Task.CompletedTask; };

            Assert.False(await client.ConnectAsync());
            Assert.False(await client.WaitForConnectionAsync(TimeSpan.FromMilliseconds(300)));

            await using var broker = await TestBroker.StartAsync(port);
            Assert.True(await client.WaitForConnectionAsync(TimeSpan.FromSeconds(8)));

            Assert.True(await Net.Eventually(() => restored is not null));
            Assert.True(restored!.Attempts >= 1);
            Assert.Equal(ResilientMqttFailureKind.BrokerUnavailable, restored.LastFailure?.Kind);
            Assert.True(restored.Downtime > TimeSpan.Zero);
        }

        [Fact]
        public async Task A_kick_raises_ConnectionLost_once_then_reconnects_with_ConnectionRestored()   // #20 #21
        {
            await using var broker = await TestBroker.StartAsync();
            var o = TestClients.Options(broker.Port);
            var lost = new ConcurrentQueue<ResilientMqttConnectionLostArgs>();
            var restored = new ConcurrentQueue<ResilientMqttConnectionRestoredArgs>();
            var onDisconnected = 0;
            await using var client = TestClients.Create(o);
            client.ConnectionLost += a => { lost.Enqueue(a); return Task.CompletedTask; };
            client.ConnectionRestored += a => { restored.Enqueue(a); return Task.CompletedTask; };
            client.OnDisconnected += () => { Interlocked.Increment(ref onDisconnected); return Task.CompletedTask; };
            Assert.True(await client.ConnectAsync());

            await broker.KickAsync(o.ClientId, MqttDisconnectReasonCode.AdministrativeAction);

            Assert.True(await Net.Eventually(() => restored.Count == 1));
            var l = Assert.Single(lost);
            Assert.Equal(ResilientMqttFailureKind.BrokerUnavailable, l.Failure.Kind);
            Assert.Equal("AdministrativeAction", l.Failure.Code);
            Assert.Equal(1, Volatile.Read(ref onDisconnected));
            Assert.Equal(2, client.ConnectionInfo.TotalConnects);
            Assert.Null(client.LastFailure);
        }

        [Fact]
        public async Task Same_client_id_elsewhere_is_SessionTakenOver()   // MQTT 5 reason
        {
            await using var broker = await TestBroker.StartAsync();
            var o = TestClients.Options(broker.Port, x => x.Reconnect.Enabled = false);
            ResilientMqttConnectionLostArgs? lost = null;
            await using var client = TestClients.Create(o);
            client.ConnectionLost += a => { lost = a; return Task.CompletedTask; };
            Assert.True(await client.ConnectAsync());

            using var intruder = await broker.RawClientAsync(o.ClientId);

            Assert.True(await Net.Eventually(() => lost is not null));
            Assert.Equal(ResilientMqttFailureKind.SessionTakenOver, lost!.Failure.Kind);
        }

        [Fact]
        public async Task A_drop_during_OnConnected_still_reconnects()   // #26 lost wake-up
        {
            await using var broker = await TestBroker.StartAsync();
            var o = TestClients.Options(broker.Port);
            var connects = 0;
            await using var client = TestClients.Create(o);   // NullHealthCheck: only the #26 re-check can rescue it
            client.OnConnected += async () =>
            {
                if (Interlocked.Increment(ref connects) == 2)
                {
                    // Second connect comes from the reconnect loop, which still holds its latch here.
                    await broker.KickAsync(o.ClientId);
                    await Task.Delay(200);   // let the drop arrive while we are still "inside" the connect
                }
            };

            Assert.True(await client.ConnectAsync());
            await broker.KickAsync(o.ClientId);

            Assert.True(await Net.Eventually(() => Volatile.Read(ref connects) >= 3 && client.IsConnected));
        }

        [Fact]
        public async Task A_zombie_connection_is_detected_even_while_publishing_QoS0()   // #25
        {
            await using var broker = await TestBroker.StartAsync();
            using var proxy = new FreezableProxy(broker.Port);
            var o = TestClients.Options(proxy.Port, x => x.HealthCheck.PingTimeoutSeconds = 1);
            var health = new IdleAwareHealthCheck(TimeSpan.FromMilliseconds(300), TimeSpan.FromMilliseconds(400),
                TimeSpan.FromSeconds(1), NullLogger.Instance);
            ResilientMqttConnectionLostArgs? lost = null;
            await using var client = TestClients.Create(o, healthCheck: health);
            client.ConnectionLost += a => { lost = a; return Task.CompletedTask; };
            Assert.True(await client.ConnectAsync());

            proxy.FreezeDownstream();   // broker → client now silently dropped; the socket stays open

            // Like the kiosk agent: keep publishing QoS0 telemetry — that must NOT count as "the broker is alive".
            using var cts = new CancellationTokenSource();
            var publisher = Task.Run(async () =>
            {
                while (!cts.IsCancellationRequested)
                {
                    await client.PublishAsync("telemetry/health", new { cpu = 1 }, qos: ResilientMqttQos.AtMostOnce);
                    await Task.Delay(100);
                }
            });

            Assert.True(await Net.Eventually(() => lost is not null, 10_000));
            Assert.Equal(ResilientMqttFailureKind.Timeout, lost!.Failure.Kind);
            Assert.True(await Net.Eventually(() => client.IsConnected && proxy.ConnectionCount >= 2, 10_000));
            cts.Cancel();
            await publisher;
        }
    }
}

using System.Collections.Concurrent;
using Microsoft.Extensions.Logging.Abstractions;
using MQTTnet;
using MQTTnet.Protocol;
using ResilientMqtt.Internal;
using ResilientMqtt.Tests.Infrastructure;

namespace ResilientMqtt.Tests.Integration
{
    // Paths the first 1.1.0 test pass did not cover (listed honestly in the review, then tested here).
    public class CoverageGapTests
    {
        [Fact]
        public async Task Concurrent_dispatch_applies_back_pressure_and_loses_nothing()   // #35 MaxPending
        {
            await using var broker = await TestBroker.StartAsync();
            var log = new ListLogger<ResilientMqttClient>();
            var release = new TaskCompletionSource();
            var handled = new ConcurrentQueue<string>();
            await using var client = TestClients.Create(TestClients.Options(broker.Port, o =>
            {
                o.Dispatch.Mode = "Concurrent";
                o.Dispatch.MaxConcurrency = 1;
                o.Dispatch.MaxPending = 1;
            }), log);
            await client.SubscribeAsync("bp/#", async (_, p, _) => { await release.Task; handled.Enqueue(p); });
            Assert.True(await client.ConnectAsync());

            using var publisher = await broker.RawClientAsync();
            for (var i = 1; i <= 6; i++)
                _ = publisher.PublishStringAsync("bp/x", $"m{i}", MqttQualityOfServiceLevel.AtLeastOnce);

            Assert.True(await Net.Eventually(() => log.Has(ResilientMqttLogEvents.DispatchBackPressure), 5000));
            Assert.Empty(handled);                                   // all blocked behind the slow handler…
            release.SetResult();
            Assert.True(await Net.Eventually(() => handled.Count == 6, 8000), $"handled {handled.Count} of 6");   // …and none lost
        }

        [Fact]
        public async Task Dispose_waits_for_running_concurrent_handlers()   // #35 ShutdownWaitSeconds
        {
            await using var broker = await TestBroker.StartAsync();
            var started = new TaskCompletionSource();
            var finished = false;
            var client = TestClients.Create(TestClients.Options(broker.Port, o => o.Dispatch.Mode = "Concurrent"));
            await client.SubscribeAsync("work", async (_, _, _) =>
            {
                started.TrySetResult();
                await Task.Delay(600);   // deliberately ignores the cancellation token
                finished = true;
            });
            Assert.True(await client.ConnectAsync());

            using (var publisher = await broker.RawClientAsync())
                await publisher.PublishStringAsync("work", "go", MqttQualityOfServiceLevel.AtLeastOnce);
            await started.Task.WaitAsync(TimeSpan.FromSeconds(5));

            await client.DisposeAsync();
            Assert.True(finished);
        }

        [Fact]
        public async Task V311_zombie_connection_is_reset_and_reconnects()   // #25 on MQTT 3.1.1
        {
            await using var broker = await TestBroker.StartAsync();
            using var proxy = new FreezableProxy(broker.Port);
            var health = new IdleAwareHealthCheck(TimeSpan.FromMilliseconds(300), TimeSpan.FromMilliseconds(400),
                TimeSpan.FromSeconds(1), NullLogger.Instance);
            ResilientMqttConnectionLostArgs? lost = null;
            await using var client = TestClients.Create(
                TestClients.Options(proxy.Port, o => o.ProtocolVersion = "V311"), healthCheck: health);
            client.ConnectionLost += a => { lost = a; return Task.CompletedTask; };
            Assert.True(await client.ConnectAsync());

            proxy.FreezeDownstream();

            Assert.True(await Net.Eventually(() => lost is not null, 10_000));
            Assert.Equal(ResilientMqttFailureKind.Timeout, lost!.Failure.Kind);
            Assert.True(await Net.Eventually(() => client.IsConnected && proxy.ConnectionCount >= 2, 10_000));
        }

        [Fact]
        public async Task V311_persistent_session_queues_while_offline()   // #19 on MQTT 3.1.1
        {
            await using var broker = await TestBroker.StartAsync();
            var clientId = "v311-" + Guid.NewGuid().ToString("N")[..6];
            Action<ResilientMqttOptions> cfg = o => { o.ClientId = clientId; o.CleanSession = false; o.ProtocolVersion = "V311"; };

            await using (var first = TestClients.Create(TestClients.Options(broker.Port, cfg)))
            {
                Assert.True(await first.ConnectAsync());
                await first.SubscribeAsync("cmd/v311", (_, _, _) => Task.CompletedTask);
            }
            using (var publisher = await broker.RawClientAsync())
                await publisher.PublishStringAsync("cmd/v311", "queued", MqttQualityOfServiceLevel.AtLeastOnce);

            var received = new ConcurrentQueue<string>();
            await using var second = TestClients.Create(TestClients.Options(broker.Port, cfg));
            await second.SubscribeAsync("cmd/v311", (_, p, _) => { received.Enqueue(p); return Task.CompletedTask; });
            Assert.True(await second.ConnectAsync());

            Assert.True(await Net.Eventually(() => received.Contains("queued")));
            Assert.Equal("V311", second.ConnectionInfo.ProtocolVersion);
        }

        [Fact]
        public async Task WaitForConnection_returns_only_when_OnConnected_has_finished()   // #46
        {
            var port = Net.FreePort();
            var onConnectedDone = false;
            await using var client = TestClients.Create(TestClients.Options(port));
            client.OnConnected += async () => { await Task.Delay(700); onConnectedDone = true; };

            Assert.False(await client.ConnectAsync());   // broker not up yet → reconnect loop runs
            await using var broker = await TestBroker.StartAsync(port);

            Assert.True(await client.WaitForConnectionAsync(TimeSpan.FromSeconds(10)));
            Assert.True(onConnectedDone);   // used to return as soon as the socket was up
        }
    }
}

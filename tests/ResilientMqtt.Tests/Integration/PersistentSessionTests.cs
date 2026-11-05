using System.Collections.Concurrent;
using MQTTnet;
using MQTTnet.Protocol;
using ResilientMqtt.Tests.Infrastructure;

namespace ResilientMqtt.Tests.Integration
{
    // Edge cases that only exist since 1.1.0 made persistent sessions real (#19).
    public class PersistentSessionTests
    {
        [Fact]
        public async Task Messages_queued_while_the_process_was_down_reach_handlers_registered_in_OnConnected()   // #44
        {
            await using var broker = await TestBroker.StartAsync();
            var clientId = "proc-" + Guid.NewGuid().ToString("N")[..6];
            Action<ResilientMqttOptions> cfg = o => { o.ClientId = clientId; o.CleanSession = false; };

            // Run 1: the app subscribes the common way — inside OnConnected — then the process stops.
            await using (var run1 = TestClients.Create(TestClients.Options(broker.Port, cfg)))
            {
                run1.OnConnected += () => run1.SubscribeAsync("cmd/#", (_, _, _) => Task.CompletedTask);
                Assert.True(await run1.ConnectAsync());
                Assert.True(await Net.Eventually(() => broker.Subscriptions.Any(s => s.ClientId == clientId)));
            }

            // While it is down, the server sends commands — the broker queues them in the session.
            using (var server = await broker.RawClientAsync())
                for (var i = 1; i <= 3; i++)
                    await server.PublishStringAsync("cmd/run", $"c{i}", MqttQualityOfServiceLevel.AtLeastOnce);

            // Run 2 (new process): same pattern. The broker delivers the queue right after CONNACK — before OnConnected.
            var received = new ConcurrentQueue<string>();
            await using var run2 = TestClients.Create(TestClients.Options(broker.Port, cfg));
            run2.OnConnected += async () =>
            {
                await Task.Delay(500);   // a realistic OnConnected (several awaits) — the queue arrives meanwhile
                await run2.SubscribeAsync("cmd/#", (_, p, _) => { received.Enqueue(p); return Task.CompletedTask; });
            };
            Assert.True(await run2.ConnectAsync());

            Assert.True(await Net.Eventually(() => received.Count == 3, 5000), $"received {received.Count} of 3");
            Assert.Equal(new[] { "c1", "c2", "c3" }, received.ToArray());
        }

        [Fact]
        public async Task A_live_message_arriving_during_the_replay_does_not_overtake_the_queued_ones()   // #44 order
        {
            await using var broker = await TestBroker.StartAsync();
            var clientId = "order-" + Guid.NewGuid().ToString("N")[..6];
            Action<ResilientMqttOptions> cfg = o => { o.ClientId = clientId; o.CleanSession = false; };

            await using (var run1 = TestClients.Create(TestClients.Options(broker.Port, cfg)))
            {
                run1.OnConnected += () => run1.SubscribeAsync("cmd/#", (_, _, _) => Task.CompletedTask);
                Assert.True(await run1.ConnectAsync());
                Assert.True(await Net.Eventually(() => broker.Subscriptions.Any(s => s.ClientId == clientId)));
            }

            using var server = await broker.RawClientAsync();
            for (var i = 1; i <= 3; i++)
                await server.PublishStringAsync("cmd/run", $"c{i}", MqttQualityOfServiceLevel.AtLeastOnce);

            var received = new ConcurrentQueue<string>();
            await using var run2 = TestClients.Create(TestClients.Options(broker.Port, cfg));
            run2.OnConnected += async () =>
            {
                await Task.Delay(300);
                await run2.SubscribeAsync("cmd/#", async (_, p, _) => { await Task.Delay(150); received.Enqueue(p); });   // slow handler
            };
            Assert.True(await run2.ConnectAsync());
            await server.PublishStringAsync("cmd/run", "c4", MqttQualityOfServiceLevel.AtLeastOnce);   // live, during the replay

            Assert.True(await Net.Eventually(() => received.Count == 4, 6000), $"received {received.Count} of 4");
            Assert.Equal(new[] { "c1", "c2", "c3", "c4" }, received.ToArray());
        }

        [Fact]
        public async Task Unsubscribing_while_offline_is_applied_to_the_resumed_session()   // #43
        {
            await using var broker = await TestBroker.StartAsync();
            var o = TestClients.Options(broker.Port, x => x.CleanSession = false);
            var received = new ConcurrentQueue<string>();
            // Slow retry: the client stays offline long enough for the unsubscribe to happen while it is down.
            await using var client = TestClients.Create(o, retry: new FastRetry(1500));
            await client.SubscribeAsync("alerts/#", (_, p, _) => { received.Enqueue(p); return Task.CompletedTask; });
            Assert.True(await client.ConnectAsync());

            await broker.KickAsync(o.ClientId);
            Assert.True(await Net.Eventually(() => !client.IsConnected, 3000));
            await client.UnsubscribeAsync("alerts/#");   // offline: the broker's session still holds the subscription

            Assert.True(await Net.Eventually(() => client.IsConnected && client.ConnectionInfo.TotalConnects == 2));
            Assert.True(client.ConnectionInfo.SessionPresent);
            Assert.True(await Net.Eventually(() => broker.Unsubscribed.Any(u => u.Topic == "alerts/#")),
                "the resumed session still had the subscription — the UNSUBSCRIBE was never sent");

            using (var publisher = await broker.RawClientAsync())
                await publisher.PublishStringAsync("alerts/x", "should-not-arrive", MqttQualityOfServiceLevel.AtLeastOnce);
            await Task.Delay(500);
            Assert.Empty(received);
        }
    }
}

using System.Collections.Concurrent;
using MQTTnet;
using MQTTnet.Protocol;
using ResilientMqtt.Tests.Infrastructure;

namespace ResilientMqtt.Tests.Integration
{
    // Second independent review (findings on the #44 hold/replay + shutdown paths).
    public class HoldReplayTests
    {
        private static async Task<string> SeedSessionAsync(TestBroker broker, Action<IResilientMqttClient> subscribe, params (string Topic, string Payload)[] queued)
        {
            var clientId = "hold-" + Guid.NewGuid().ToString("N")[..6];
            await using (var run1 = TestClients.Create(TestClients.Options(broker.Port, o => { o.ClientId = clientId; o.CleanSession = false; })))
            {
                subscribe(run1);
                Assert.True(await run1.ConnectAsync());
                await Task.Delay(200);
            }
            using var server = await broker.RawClientAsync();
            foreach (var (topic, payload) in queued)
                await server.PublishStringAsync(topic, payload, MqttQualityOfServiceLevel.AtLeastOnce);
            return clientId;
        }

        [Fact]
        public async Task Held_and_restored_subscriptions_keep_the_arrival_order()   // review-2 #7
        {
            await using var broker = await TestBroker.StartAsync();
            var clientId = await SeedSessionAsync(broker, c =>
            {
                c.SubscribeAsync("y/#", (_, _, _) => Task.CompletedTask).Wait();
                c.OnConnected += () => c.SubscribeAsync("x/#", (_, _, _) => Task.CompletedTask);
            }, ("x/1", "x1"), ("y/1", "y1"), ("x/2", "x2"));

            var order = new ConcurrentQueue<string>();
            await using var run2 = TestClients.Create(TestClients.Options(broker.Port, o => { o.ClientId = clientId; o.CleanSession = false; }));
            await run2.SubscribeAsync("y/#", (_, p, _) => { order.Enqueue(p); return Task.CompletedTask; });   // restored at connect
            run2.OnConnected += async () =>
            {
                await Task.Delay(300);
                await run2.SubscribeAsync("x/#", (_, p, _) => { order.Enqueue(p); return Task.CompletedTask; });
            };
            Assert.True(await run2.ConnectAsync());

            Assert.True(await Net.Eventually(() => order.Count == 3, 5000), $"got {order.Count}");
            Assert.Equal(new[] { "x1", "y1", "x2" }, order.ToArray());   // y1 waits behind the held x1
        }

        [Fact]
        public async Task A_reconnect_during_a_slow_replay_loses_nothing()   // review-2 #1/#2
        {
            await using var broker = await TestBroker.StartAsync();
            var clientId = await SeedSessionAsync(broker,
                c => c.OnConnected += () => c.SubscribeAsync("cmd/#", (_, _, _) => Task.CompletedTask),
                ("cmd/a", "c1"), ("cmd/a", "c2"), ("cmd/a", "c3"), ("cmd/a", "c4"), ("cmd/a", "c5"));

            var seen = new ConcurrentQueue<string>();
            var o = TestClients.Options(broker.Port, x => { x.ClientId = clientId; x.CleanSession = false; });
            await using var run2 = TestClients.Create(o);
            run2.OnConnected += async () =>
            {
                await Task.Delay(200);
                await run2.SubscribeAsync("cmd/#", async (_, p, _) => { await Task.Delay(150); seen.Enqueue(p); });   // slow replay
            };
            Assert.True(await run2.ConnectAsync());
            Assert.True(await Net.Eventually(() => seen.Count >= 1, 5000));

            await broker.KickAsync(clientId);   // drop in the middle of the replay → new connection, same session

            Assert.True(await Net.Eventually(() => new[] { "c1", "c2", "c3", "c4", "c5" }.All(seen.Contains), 15_000),
                $"seen: {string.Join(",", seen)}");   // at-least-once: duplicates are allowed, losses are not
        }

        [Fact]
        public async Task A_shutdown_during_set_up_does_not_replay_into_the_stopping_app_and_keeps_the_messages()   // review-2 #10
        {
            await using var broker = await TestBroker.StartAsync();
            var clientId = await SeedSessionAsync(broker,
                c => c.OnConnected += () => c.SubscribeAsync("cmd/#", (_, _, _) => Task.CompletedTask),
                ("cmd/a", "c1"), ("cmd/a", "c2"));

            var handledByStoppingApp = 0;
            var run2 = TestClients.Create(TestClients.Options(broker.Port, o => { o.ClientId = clientId; o.CleanSession = false; }));
            var inOnConnected = new TaskCompletionSource();
            run2.OnConnected += async () =>
            {
                await run2.SubscribeAsync("cmd/#", (_, _, _) => { Interlocked.Increment(ref handledByStoppingApp); return Task.CompletedTask; });
                inOnConnected.TrySetResult();
                await Task.Delay(500);
            };
            var connecting = run2.ConnectAsync();
            await inOnConnected.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await run2.DisconnectAsync();   // the app is stopping while OnConnected still runs
            Assert.False(await connecting);   // review-2 #9 — the set-up did not complete
            await run2.DisposeAsync();
            Assert.Equal(0, Volatile.Read(ref handledByStoppingApp));

            var received = new ConcurrentQueue<string>();
            await using var run3 = TestClients.Create(TestClients.Options(broker.Port, o => { o.ClientId = clientId; o.CleanSession = false; }));
            await run3.SubscribeAsync("cmd/#", (_, p, _) => { received.Enqueue(p); return Task.CompletedTask; });
            Assert.True(await run3.ConnectAsync());
            Assert.True(await Net.Eventually(() => received.Contains("c1") && received.Contains("c2"), 5000),
                $"received: {string.Join(",", received)}");
        }

        [Fact]
        public async Task Dispose_is_bounded_when_concurrent_handlers_are_saturated()   // review-2 #8
        {
            await using var broker = await TestBroker.StartAsync();
            var o = TestClients.Options(broker.Port, x =>
            {
                x.Dispatch.Mode = "Concurrent";
                x.Dispatch.MaxConcurrency = 1;
                x.Dispatch.MaxPending = 0;
                x.Dispatch.ShutdownWaitSeconds = 1;
                x.TopicPrefix = "terminal/R8";
                x.LastWill.Enabled = true;
                x.LastWill.GracefulShutdownPayload = "bye";
            });
            var client = TestClients.Create(o);
            var forever = new TaskCompletionSource();
            await client.SubscribeAsync("busy", async (_, _, _) => await forever.Task);
            Assert.True(await client.ConnectAsync());

            using (var publisher = await broker.RawClientAsync())
                for (var i = 0; i < 4; i++)
                    await publisher.PublishStringAsync("busy", $"m{i}", MqttQualityOfServiceLevel.AtLeastOnce);
            await Task.Delay(500);   // one handler running, the receive loop parked on the full capacity

            var sw = System.Diagnostics.Stopwatch.StartNew();
            await client.DisposeAsync();
            Assert.True(sw.Elapsed < TimeSpan.FromSeconds(8), $"dispose took {sw.Elapsed.TotalSeconds:F1} s");
            Assert.True(await Net.Eventually(() => broker.Published.Any(p => p.Topic == "terminal/R8/status" && p.Payload == "bye")));
            forever.TrySetResult();
        }

        [Fact]
        public async Task ConnectAsync_returns_false_when_the_connection_drops_during_set_up()   // review-2 #9
        {
            await using var broker = await TestBroker.StartAsync();
            var o = TestClients.Options(broker.Port, x => x.Reconnect.Enabled = false);
            var first = true;
            await using var client = TestClients.Create(o);
            client.OnConnected += async () =>
            {
                if (!first) return;
                first = false;
                await broker.KickAsync(o.ClientId);
                await Task.Delay(300);
            };
            Assert.False(await client.ConnectAsync());
        }
    }
}

using System.Collections.Concurrent;
using Microsoft.Extensions.Logging.Abstractions;
using MQTTnet;
using MQTTnet.Protocol;
using ResilientMqtt.Buffering;
using ResilientMqtt.Internal;
using ResilientMqtt.Tests.Infrastructure;

namespace ResilientMqtt.Tests.Integration
{
    public class MessagingTests
    {
        [Fact]
        public async Task A_refused_subscription_is_reported_not_logged_as_subscribed()   // #23
        {
            await using var broker = await TestBroker.StartAsync();
            broker.DenySubscribe = t => t.StartsWith("secret/");
            var log = new ListLogger<ResilientMqttClient>();
            await using var client = TestClients.Create(TestClients.Options(broker.Port), log);
            Assert.True(await client.ConnectAsync());

            await client.SubscribeAsync("secret/#", (_, _, _) => Task.CompletedTask);   // must not throw
            await client.SubscribeAsync("open/#", (_, _, _) => Task.CompletedTask);

            var rejected = client.ConnectionInfo.RejectedSubscriptions;
            Assert.True(rejected.ContainsKey("secret/#"));
            Assert.False(rejected.ContainsKey("open/#"));
            Assert.True(log.Has(ResilientMqttLogEvents.SubscriptionRefused));
            Assert.Equal(1, log.Count(ResilientMqttLogEvents.Subscribed));   // only open/#
        }

        [Fact]
        public async Task Publish_outcomes_are_truthful()   // #24 #39
        {
            await using var broker = await TestBroker.StartAsync();
            broker.DenyPublish = t => t.StartsWith("denied/");
            await using var client = TestClients.Create(TestClients.Options(broker.Port));
            Assert.True(await client.ConnectAsync());

            Assert.Equal(ResilientMqttPublishOutcome.Sent, await client.PublishWithResultAsync("ok/1", "x"));
            Assert.Equal(ResilientMqttPublishOutcome.Denied, await client.PublishWithResultAsync("denied/1", "x"));
            Assert.Equal(ResilientMqttPublishOutcome.Sent,
                await client.PublishWithResultAsync("denied/2", "x", qos: ResilientMqttQos.AtMostOnce));   // QoS0 has no ack — can't know
            Assert.Equal(1, client.ConnectionInfo.DeniedPublishes);
        }

        [Fact]
        public async Task While_disconnected_publishes_are_Dropped_without_a_buffer_and_Buffered_with_one()
        {
            var port = Net.FreePort();
            await using (var noBuffer = TestClients.Create(TestClients.Options(port, o => o.Reconnect.Enabled = false)))
                Assert.Equal(ResilientMqttPublishOutcome.Dropped, await noBuffer.PublishWithResultAsync("a", "x"));

            await using var buffered = TestClients.Create(TestClients.Options(port, o => o.OfflineBuffer.Enabled = true));
            Assert.Equal(ResilientMqttPublishOutcome.Buffered, await buffered.PublishWithResultAsync("a/b", "kept"));
            Assert.Equal(1, buffered.ConnectionInfo.BufferedCount);

            await using var broker = await TestBroker.StartAsync(port);   // broker comes up → reconnect → drain
            Assert.True(await Net.Eventually(() => broker.Published.Any(p => p.Topic == "a/b" && p.Payload == "kept")));
            Assert.True(await Net.Eventually(() => buffered.ConnectionInfo.BufferedCount == 0));
        }

        [Fact]
        public async Task Each_filter_is_subscribed_once_per_connection_even_if_OnConnected_subscribes_again()   // #27
        {
            await using var broker = await TestBroker.StartAsync();
            var o = TestClients.Options(broker.Port);
            await using var client = TestClients.Create(o);
            // The pattern both consumers used: (re)subscribe inside OnConnected.
            client.OnConnected += () => client.SubscribeAsync("cmd/a", (_, _, _) => Task.CompletedTask);

            Assert.True(await client.ConnectAsync());
            Assert.True(await Net.Eventually(() => broker.Subscriptions.Count(s => s.Topic == "cmd/a") == 1));

            await broker.KickAsync(o.ClientId);
            Assert.True(await Net.Eventually(() => client.ConnectionInfo.TotalConnects == 2 && client.IsConnected));
            await Task.Delay(300);

            Assert.Equal(2, broker.Subscriptions.Count(s => s.Topic == "cmd/a"));   // 1 per connection, not 3
        }

        [Fact]
        public async Task Retained_messages_are_not_replayed_when_a_resumed_session_restores_its_subscription()   // MQTT 5 retain handling
        {
            await using var broker = await TestBroker.StartAsync();
            using (var publisher = await broker.RawClientAsync())
                await publisher.PublishAsync(new MqttApplicationMessageBuilder()
                    .WithTopic("status/kiosk").WithPayload("online").WithRetainFlag().WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce).Build());

            var o = TestClients.Options(broker.Port, x => x.CleanSession = false);
            var received = 0;
            await using var client = TestClients.Create(o);
            await client.SubscribeAsync("status/#", (_, _, _) => { Interlocked.Increment(ref received); return Task.CompletedTask; });
            Assert.True(await client.ConnectAsync());
            Assert.True(await Net.Eventually(() => Volatile.Read(ref received) == 1));
            // Kick only after the broker has the PUBACK — an unacknowledged QoS1 message is (correctly) redelivered
            // on the resumed session, which would be at-least-once delivery, not a retained replay.
            Assert.True(await Net.Eventually(() => broker.AckedByClients >= 1));

            await broker.KickAsync(o.ClientId);
            Assert.True(await Net.Eventually(() => client.ConnectionInfo.TotalConnects == 2 && client.IsConnected));
            Assert.True(client.ConnectionInfo.SessionPresent);
            await Task.Delay(500);

            Assert.Equal(1, Volatile.Read(ref received));
        }

        [Fact]
        public async Task Concurrent_dispatch_keeps_a_slow_handler_from_blocking_others()   // #35
        {
            await using var broker = await TestBroker.StartAsync();
            var release = new TaskCompletionSource();
            var fastSeen = new TaskCompletionSource();
            await using var client = TestClients.Create(TestClients.Options(broker.Port, o => o.Dispatch.Mode = "Concurrent"));
            await client.SubscribeAsync("slow", async (_, _, _) => await release.Task);
            await client.SubscribeAsync("fast", (_, _, _) => { fastSeen.TrySetResult(); return Task.CompletedTask; });
            Assert.True(await client.ConnectAsync());

            using var publisher = await broker.RawClientAsync();
            await publisher.PublishStringAsync("slow", "1", MqttQualityOfServiceLevel.AtLeastOnce);
            await publisher.PublishStringAsync("fast", "2", MqttQualityOfServiceLevel.AtLeastOnce);

            var done = await Task.WhenAny(fastSeen.Task, Task.Delay(3000));
            release.TrySetResult();
            Assert.Same(fastSeen.Task, done);
        }

        [Fact]
        public async Task Inline_dispatch_keeps_order()
        {
            await using var broker = await TestBroker.StartAsync();
            var seen = new ConcurrentQueue<string>();
            await using var client = TestClients.Create(TestClients.Options(broker.Port));
            await client.SubscribeAsync("seq", async (_, p, _) => { if (p == "1") await Task.Delay(300); seen.Enqueue(p); });
            Assert.True(await client.ConnectAsync());

            using var publisher = await broker.RawClientAsync();
            await publisher.PublishStringAsync("seq", "1", MqttQualityOfServiceLevel.AtLeastOnce);
            await publisher.PublishStringAsync("seq", "2", MqttQualityOfServiceLevel.AtLeastOnce);

            Assert.True(await Net.Eventually(() => seen.Count == 2));
            Assert.Equal(new[] { "1", "2" }, seen.ToArray());
        }

        [Fact]
        public async Task Graceful_offline_status_goes_out_even_when_the_breaker_is_open()   // #30
        {
            await using var broker = await TestBroker.StartAsync();
            var o = TestClients.Options(broker.Port, x =>
            {
                x.TopicPrefix = "terminal/T1";
                x.LastWill.Enabled = true;
                x.LastWill.GracefulShutdownPayload = "offline-graceful";
            });
            var breaker = new CircuitBreaker(1, TimeSpan.FromMinutes(10), NullLogger.Instance);
            await using var client = TestClients.Create(o, breaker: breaker);
            Assert.True(await client.ConnectAsync());

            Assert.True(breaker.TryAcquire(out _));
            breaker.RecordFailure();   // circuit OPEN
            Assert.Equal(ResilientMqttPublishOutcome.Dropped, await client.PublishWithResultAsync("telemetry", "x"));

            await client.DisconnectAsync();
            Assert.True(await Net.Eventually(() => broker.Published.Any(p => p.Topic == "terminal/T1/status" && p.Payload == "offline-graceful")));
        }

        [Fact]
        public async Task Disposing_without_DisconnectAsync_still_sends_the_graceful_status()   // #42
        {
            await using var broker = await TestBroker.StartAsync();
            var o = TestClients.Options(broker.Port, x =>
            {
                x.TopicPrefix = "terminal/T2";
                x.LastWill.Enabled = true;
                x.LastWill.GracefulShutdownPayload = "bye";
            });
            var client = TestClients.Create(o);
            Assert.True(await client.ConnectAsync());

            await client.DisposeAsync();
            Assert.True(await Net.Eventually(() => broker.Published.Any(p => p.Topic == "terminal/T2/status" && p.Payload == "bye")));
        }

        [Fact]
        public async Task The_backlog_is_sent_as_soon_as_the_breaker_closes()   // #29
        {
            await using var broker = await TestBroker.StartAsync();
            var o = TestClients.Options(broker.Port, x => x.OfflineBuffer.Enabled = true);
            var breaker = new CircuitBreaker(1, TimeSpan.Zero, NullLogger.Instance);   // half-opens immediately
            await using var client = TestClients.Create(o, breaker: breaker);
            Assert.True(await client.ConnectAsync());

            Assert.True(breaker.TryAcquire(out _));
            breaker.RecordFailure();                              // OPEN
            breaker.TryAcquire(out var probe);                    // someone holds the probe…
            Assert.Equal(ResilientMqttPublishOutcome.Buffered, await client.PublishWithResultAsync("backlog/1", "kept"));
            breaker.RecordAbandoned(probe);                       // …and gives it back

            Assert.Equal(ResilientMqttPublishOutcome.Sent, await client.PublishWithResultAsync("live/1", "now"));   // probe succeeds → CLOSED
            Assert.True(await Net.Eventually(() => broker.Published.Any(p => p.Topic == "backlog/1")));              // drained without a reconnect
        }
    }
}

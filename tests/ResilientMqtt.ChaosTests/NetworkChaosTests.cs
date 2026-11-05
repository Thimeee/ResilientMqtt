using System.Collections.Concurrent;
using ResilientMqtt.TestKit;

namespace ResilientMqtt.ChaosTests
{
    // Real network faults injected by Toxiproxy between the client and each broker.
    [Collection("chaos")]
    public class NetworkChaosTests(ChaosFixture fixture, ITestOutputHelper output) : ChaosTestBase(fixture, output)
    {
        private static readonly TimeSpan Long = TimeSpan.FromSeconds(30);

        [SkippableTheory, MemberData(nameof(Brokers))]
        public async Task Latency_within_the_timeouts_still_works(string brokerName)
        {
            var b = await RequireAsync(brokerName);
            await Fixture.Toxi.AddToxicAsync(b, "lat", "latency", "downstream", new { latency = 1500, jitter = 200 });

            await using var client = ChaosEnvironment.CreateClient(ChaosEnvironment.Options(b, viaProxy: true, o =>
            {
                o.Reconnect.ConnectTimeoutSeconds = 8;
                o.PublishTimeoutSeconds = 10;
            }));
            Assert.True(await client.ConnectAsync());
            Assert.Equal(ResilientMqttPublishOutcome.Sent, await client.PublishWithResultAsync($"chaos/lat/{Guid.NewGuid():N}", "x"));
        }

        [SkippableTheory, MemberData(nameof(Brokers))]
        public async Task Latency_beyond_the_connect_timeout_is_Timeout_and_recovers_when_it_clears(string brokerName)
        {
            var b = await RequireAsync(brokerName);
            await Fixture.Toxi.AddToxicAsync(b, "lat", "latency", "downstream", new { latency = 5000, jitter = 0 });

            await using var client = ChaosEnvironment.CreateClient(ChaosEnvironment.Options(b, viaProxy: true, o => o.Reconnect.ConnectTimeoutSeconds = 2),
                retryDelay: TimeSpan.FromMilliseconds(500));
            Assert.False(await client.ConnectAsync());
            LogFailure("during latency", client.LastFailure);
            Assert.Equal(ResilientMqttFailureKind.Timeout, client.LastFailure?.Kind);

            await Fixture.Toxi.RemoveToxicAsync(b, "lat");
            Assert.True(await client.WaitForConnectionAsync(Long));
        }

        [SkippableTheory, MemberData(nameof(Brokers))]
        public async Task A_black_hole_on_a_live_connection_is_detected_and_recovers(string brokerName)
        {
            var b = await RequireAsync(brokerName);
            ResilientMqttConnectionLostArgs? lost = null;
            await using var client = ChaosEnvironment.CreateClient(
                ChaosEnvironment.Options(b, viaProxy: true, o => { o.HealthCheck.PingTimeoutSeconds = 2; o.Reconnect.ConnectTimeoutSeconds = 3; }),
                healthInterval: TimeSpan.FromSeconds(1), retryDelay: TimeSpan.FromMilliseconds(500));
            client.ConnectionLost += a => { lost = a; return Task.CompletedTask; };
            Assert.True(await client.ConnectAsync());

            // Broker → client data silently dropped; the socket stays open (the classic NAT / firewall zombie).
            await Fixture.Toxi.AddToxicAsync(b, "hole", "timeout", "downstream", new { timeout = 0 });
            Assert.True(await ChaosEnvironment.Eventually(() => lost is not null, Long), "zombie never detected");
            LogFailure("lost", lost!.Failure);
            Assert.Equal(ResilientMqttFailureKind.Timeout, lost.Failure.Kind);

            await Fixture.Toxi.RemoveToxicAsync(b, "hole");
            Assert.True(await client.WaitForConnectionAsync(Long));
        }

        [SkippableTheory, MemberData(nameof(Brokers))]
        public async Task A_connection_reset_is_Network_and_recovers(string brokerName)
        {
            var b = await RequireAsync(brokerName);
            ResilientMqttConnectionLostArgs? lost = null;
            await using var client = ChaosEnvironment.CreateClient(ChaosEnvironment.Options(b, viaProxy: true),
                retryDelay: TimeSpan.FromMilliseconds(500));
            client.ConnectionLost += a => { lost = a; return Task.CompletedTask; };
            Assert.True(await client.ConnectAsync());

            await Fixture.Toxi.AddToxicAsync(b, "rst", "reset_peer", "upstream", new { timeout = 0 });
            _ = client.PublishWithResultAsync($"chaos/rst/{Guid.NewGuid():N}", "trigger");   // data → reset
            Assert.True(await ChaosEnvironment.Eventually(() => lost is not null, Long), "reset never surfaced");
            LogFailure("lost", lost!.Failure);
            Assert.Equal(ResilientMqttFailureKind.Network, lost.Failure.Kind);

            await Fixture.Toxi.RemoveToxicAsync(b, "rst");
            Assert.True(await client.WaitForConnectionAsync(Long));
        }

        [SkippableTheory, MemberData(nameof(Brokers))]
        public async Task Broker_unreachable_is_BrokerUnavailable_then_restored(string brokerName)
        {
            var b = await RequireAsync(brokerName);
            ResilientMqttConnectionRestoredArgs? restored = null;
            await using var client = ChaosEnvironment.CreateClient(ChaosEnvironment.Options(b, viaProxy: true),
                retryDelay: TimeSpan.FromMilliseconds(500));
            client.ConnectionRestored += a => { restored = a; return Task.CompletedTask; };
            Assert.True(await client.ConnectAsync());

            await Fixture.Toxi.SetEnabledAsync(b, false);   // drops live connections, refuses new ones
            Assert.True(await ChaosEnvironment.Eventually(() => !client.IsConnected && client.ConnectionInfo.ReconnectAttempt >= 2, Long));
            LogFailure("during outage", client.LastFailure);
            // Through Docker Desktop's port forwarder a stopped listener looks like "accepted, then closed" rather than
            // "refused", so the exact kind depends on the host. What matters: it is a broker/network-side kind, never
            // an auth / TLS / protocol verdict that would stop a kiosk from self-healing. (A truly refused port is
            // BrokerUnavailable, proven by the unit tests and against a real Mosquitto.)
            Assert.Contains(client.LastFailure!.Kind, new[]
            {
                ResilientMqttFailureKind.BrokerUnavailable, ResilientMqttFailureKind.Network,
                ResilientMqttFailureKind.Timeout, ResilientMqttFailureKind.Unknown,
            });

            await Fixture.Toxi.SetEnabledAsync(b, true);
            Assert.True(await client.WaitForConnectionAsync(Long));
            Assert.True(await ChaosEnvironment.Eventually(() => restored is not null, TimeSpan.FromSeconds(5)));
            Log($"restored after {restored!.Downtime.TotalSeconds:F1} s, {restored.Attempts} attempt(s)");
        }

        [SkippableTheory, MemberData(nameof(Brokers))]
        public async Task A_slow_link_never_hangs_a_publish_beyond_its_timeout(string brokerName)
        {
            var b = await RequireAsync(brokerName);
            await using var client = ChaosEnvironment.CreateClient(ChaosEnvironment.Options(b, viaProxy: true, o => o.PublishTimeoutSeconds = 3));
            Assert.True(await client.ConnectAsync());
            await Fixture.Toxi.AddToxicAsync(b, "bw", "bandwidth", "upstream", new { rate = 4 });   // 4 KB/s

            var sw = System.Diagnostics.Stopwatch.StartNew();
            var big = await client.PublishBytesWithResultAsync($"chaos/bw/{Guid.NewGuid():N}", new byte[200 * 1024]);
            Log($"200 KB at 4 KB/s with a 3 s timeout → {big} after {sw.Elapsed.TotalSeconds:F1} s");
            Assert.NotEqual(ResilientMqttPublishOutcome.Sent, big);
            Assert.True(sw.Elapsed < TimeSpan.FromSeconds(10), "the publish timeout did not bound the call");

            await Fixture.Toxi.RemoveToxicAsync(b, "bw");
            // The client must still be usable (no publish lock left held). MQTTnet may still be flushing the big packet.
            Assert.True(await ChaosEnvironment.Eventually(() => client.IsConnected, Long));
            var after = await client.PublishWithResultAsync($"chaos/bw/{Guid.NewGuid():N}", "small");
            Log($"next publish → {after}");
            Assert.True(after is ResilientMqttPublishOutcome.Sent || await client.WaitForConnectionAsync(Long));
        }

        [SkippableTheory, MemberData(nameof(Brokers))]
        public async Task Fragmented_packets_arrive_intact_and_in_order(string brokerName)
        {
            var b = await RequireAsync(brokerName);
            await Fixture.Toxi.AddToxicAsync(b, "slice-up", "slicer", "upstream", new { average_size = 7, size_variation = 3, delay = 100 });
            await Fixture.Toxi.AddToxicAsync(b, "slice-down", "slicer", "downstream", new { average_size = 7, size_variation = 3, delay = 100 });

            var received = new ConcurrentQueue<string>();
            var topic = $"chaos/frag/{Guid.NewGuid():N}";
            await using var client = ChaosEnvironment.CreateClient(ChaosEnvironment.Options(b, viaProxy: true, o => o.PublishTimeoutSeconds = 20));
            await client.SubscribeAsync(topic, (_, p, _) => { received.Enqueue(p); return Task.CompletedTask; });
            Assert.True(await client.ConnectAsync());

            for (var i = 0; i < 50; i++)
                Assert.Equal(ResilientMqttPublishOutcome.Sent, await client.PublishWithResultAsync(topic, $"m{i:D3}"));

            Assert.True(await ChaosEnvironment.Eventually(() => received.Count == 50, Long), $"received {received.Count} of 50");
            Assert.Equal(Enumerable.Range(0, 50).Select(i => $"m{i:D3}"), received.ToArray());
        }
    }
}

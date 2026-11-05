using System.Collections.Concurrent;
using ResilientMqtt.TestKit;

namespace ResilientMqtt.ChaosTests
{
    // The same behaviour checked on Mosquitto (production), EMQX and HiveMQ — brokers differ in how they close,
    // refuse and resume, and the library must classify and recover the same way on all of them.
    [Collection("chaos")]
    public class CrossBrokerTests(ChaosFixture fixture, ITestOutputHelper output) : ChaosTestBase(fixture, output)
    {
        [SkippableTheory, MemberData(nameof(Brokers))]
        public async Task A_persistent_session_receives_what_was_published_while_offline(string brokerName)
        {
            var b = await RequireAsync(brokerName);
            var clientId = $"chaos-sess-{brokerName}-{Guid.NewGuid():N}"[..40];
            var topic = $"chaos/sess/{Guid.NewGuid():N}";
            Action<ResilientMqttOptions> persistent = o => { o.ClientId = clientId; o.CleanSession = false; };

            await using (var first = ChaosEnvironment.CreateClient(ChaosEnvironment.Options(b, false, persistent)))
            {
                await first.SubscribeAsync(topic, (_, _, _) => Task.CompletedTask);
                Assert.True(await first.ConnectAsync());
            }

            await using (var publisher = ChaosEnvironment.CreateClient(ChaosEnvironment.Options(b, false)))
            {
                Assert.True(await publisher.ConnectAsync());
                Assert.Equal(ResilientMqttPublishOutcome.Sent, await publisher.PublishWithResultAsync(topic, "while-offline", useTopicPrefix: false));
            }

            var received = new ConcurrentQueue<string>();
            await using var second = ChaosEnvironment.CreateClient(ChaosEnvironment.Options(b, false, persistent));
            await second.SubscribeAsync(topic, (_, p, _) => { received.Enqueue(p); return Task.CompletedTask; });
            Assert.True(await second.ConnectAsync());

            Assert.True(await ChaosEnvironment.Eventually(() => received.Contains("while-offline"), TimeSpan.FromSeconds(15)));
            Assert.True(second.ConnectionInfo.SessionPresent);
        }

        [SkippableTheory, MemberData(nameof(Brokers))]
        public async Task A_duplicate_client_id_is_reported_as_SessionTakenOver(string brokerName)
        {
            var b = await RequireAsync(brokerName);
            var clientId = $"chaos-dup-{Guid.NewGuid():N}"[..30];
            ResilientMqttConnectionLostArgs? lost = null;

            await using var first = ChaosEnvironment.CreateClient(ChaosEnvironment.Options(b, false, o => { o.ClientId = clientId; o.Reconnect.Enabled = false; }));
            first.ConnectionLost += a => { lost = a; return Task.CompletedTask; };
            Assert.True(await first.ConnectAsync());

            await using var intruder = ChaosEnvironment.CreateClient(ChaosEnvironment.Options(b, false, o => { o.ClientId = clientId; o.Reconnect.Enabled = false; }));
            Assert.True(await intruder.ConnectAsync());

            Assert.True(await ChaosEnvironment.Eventually(() => lost is not null, TimeSpan.FromSeconds(15)));
            LogFailure($"{brokerName} takeover", lost!.Failure);
            Assert.Equal(ResilientMqttFailureKind.SessionTakenOver, lost.Failure.Kind);
        }

        [SkippableTheory, MemberData(nameof(Brokers))]
        public async Task Restored_subscriptions_do_not_replay_retained_messages(string brokerName)
        {
            var b = await RequireAsync(brokerName);
            var topic = $"chaos/ret/{Guid.NewGuid():N}";
            await using (var publisher = ChaosEnvironment.CreateClient(ChaosEnvironment.Options(b, false)))
            {
                Assert.True(await publisher.ConnectAsync());
                await publisher.PublishWithResultAsync(topic, "retained", retain: true, useTopicPrefix: false);
            }

            var count = 0;
            var clientId = $"chaos-ret-{Guid.NewGuid():N}"[..30];
            await using var client = ChaosEnvironment.CreateClient(
                ChaosEnvironment.Options(b, viaProxy: true, o => { o.ClientId = clientId; o.CleanSession = false; }),
                retryDelay: TimeSpan.FromMilliseconds(300));
            await client.SubscribeAsync(topic, (_, _, _) => { Interlocked.Increment(ref count); return Task.CompletedTask; });
            Assert.True(await client.ConnectAsync());
            Assert.True(await ChaosEnvironment.Eventually(() => Volatile.Read(ref count) == 1, TimeSpan.FromSeconds(10)));
            await Task.Delay(500);   // let the PUBACK reach the broker before the cut

            await Fixture.Toxi.SetEnabledAsync(b, false);
            Assert.True(await ChaosEnvironment.Eventually(() => !client.IsConnected, TimeSpan.FromSeconds(10)));
            await Fixture.Toxi.SetEnabledAsync(b, true);
            Assert.True(await client.WaitForConnectionAsync(TimeSpan.FromSeconds(30)));
            await Task.Delay(1000);

            Log($"{brokerName}: sessionPresent={client.ConnectionInfo.SessionPresent}, retained deliveries={count}");
            Assert.Equal(1, Volatile.Read(ref count));
        }
    }
}

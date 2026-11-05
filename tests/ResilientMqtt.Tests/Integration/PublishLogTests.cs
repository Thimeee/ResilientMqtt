using Microsoft.Extensions.Logging.Abstractions;
using MQTTnet;
using MQTTnet.Exceptions;
using ResilientMqtt.Buffering;
using ResilientMqtt.Internal;
using ResilientMqtt.Tests.Infrastructure;

namespace ResilientMqtt.Tests.Integration
{
    // The logs a failing or misconfigured client produces must stay bounded: at 500 kiosks one line per message fills disks.
    public class PublishLogTests
    {
        private static async Task<(TestBroker Broker, ResilientMqttClient Client, FaultyMqttClient Faulty, ListLogger<ResilientMqttClient> Log)> ConnectedAsync()
        {
            var broker = await TestBroker.StartAsync();
            var faulty = new FaultyMqttClient(new MqttClientFactory().CreateMqttClient());
            var log = new ListLogger<ResilientMqttClient>();
            var client = TestClients.Create(TestClients.Options(broker.Port), log, mqttClient: faulty);
            Assert.True(await client.ConnectAsync());
            return (broker, client, faulty, log);
        }

        private static void FailPublishes(FaultyMqttClient faulty)
            => faulty.PublishOverride = (_, _) => throw new MqttCommunicationException("send failed");

        [Fact]
        public async Task A_run_of_publish_failures_logs_one_error_and_reports_the_rest_after_recovery()   // #68
        {
            var (broker, client, faulty, log) = await ConnectedAsync();
            await using (broker)
            await using (client)
            {
                FailPublishes(faulty);
                for (var i = 0; i < 5; i++)
                    Assert.Equal(ResilientMqttPublishOutcome.Dropped, await client.PublishWithResultAsync("t/1", "x"));
                Assert.Equal(1, log.Count(ResilientMqttLogEvents.PublishFailed));

                faulty.PublishOverride = null;
                Assert.Equal(ResilientMqttPublishOutcome.Sent, await client.PublishWithResultAsync("t/1", "x"));
                var report = Assert.Single(log.Entries, e => e.Id.Id == ResilientMqttLogEvents.PublishFailuresNotLogged.Id);
                Assert.Contains("4", report.Message);

                // Reported once, not on every later success.
                Assert.Equal(ResilientMqttPublishOutcome.Sent, await client.PublishWithResultAsync("t/1", "x"));
                Assert.Equal(1, log.Count(ResilientMqttLogEvents.PublishFailuresNotLogged));
            }
        }

        [Fact]
        public async Task A_long_outage_logs_again_each_window_with_the_count_of_what_was_skipped()   // #68
        {
            var (broker, client, faulty, log) = await ConnectedAsync();
            await using (broker)
            await using (client)
            {
                client.PublishLogWindow = TimeSpan.FromMilliseconds(200);
                FailPublishes(faulty);
                for (var i = 0; i < 3; i++) await client.PublishWithResultAsync("t/1", "x");
                Assert.Equal(1, log.Count(ResilientMqttLogEvents.PublishFailed));

                await Task.Delay(300);
                await client.PublishWithResultAsync("t/1", "x");

                var errors = log.Entries.Where(e => e.Id.Id == ResilientMqttLogEvents.PublishFailed.Id).ToList();
                Assert.Equal(2, errors.Count);
                Assert.Contains("2 more", errors[1].Message);
            }
        }

        [Fact]
        public async Task A_failure_is_still_buffered_when_its_log_line_is_skipped()   // #68 — only the log is limited
        {
            var broker = await TestBroker.StartAsync();
            await using (broker)
            {
                var faulty = new FaultyMqttClient(new MqttClientFactory().CreateMqttClient());
                var log = new ListLogger<ResilientMqttClient>();
                await using var client = TestClients.Create(TestClients.Options(broker.Port, o => o.OfflineBuffer.Enabled = true), log, mqttClient: faulty);
                Assert.True(await client.ConnectAsync());

                FailPublishes(faulty);
                for (var i = 0; i < 3; i++)
                    Assert.Equal(ResilientMqttPublishOutcome.Buffered, await client.PublishWithResultAsync("t/1", "x"));
                Assert.Equal(1, log.Count(ResilientMqttLogEvents.PublishFailed));
                Assert.Equal(3, client.ConnectionInfo.BufferedCount);
            }
        }

        [Fact]
        public async Task A_run_of_publishes_blocked_by_an_open_circuit_logs_one_warning()   // #69
        {
            await using var broker = await TestBroker.StartAsync();
            var log = new ListLogger<ResilientMqttClient>();
            var breaker = new CircuitBreaker(1, TimeSpan.FromMinutes(10), NullLogger.Instance);
            await using var client = TestClients.Create(TestClients.Options(broker.Port), log, breaker: breaker);
            Assert.True(await client.ConnectAsync());

            Assert.True(breaker.TryAcquire(out _));
            breaker.RecordFailure();   // circuit OPEN
            for (var i = 0; i < 5; i++)
                Assert.Equal(ResilientMqttPublishOutcome.Dropped, await client.PublishWithResultAsync("t/1", "x"));

            Assert.Equal(1, log.Count(ResilientMqttLogEvents.PublishBlockedCircuitOpen));
        }

        [Fact]
        public async Task Shutdown_reports_what_was_not_logged_exactly_once()   // #70
        {
            var (broker, client, faulty, log) = await ConnectedAsync();
            await using (broker)
            await using (client)
            {
                FailPublishes(faulty);
                for (var i = 0; i < 5; i++) await client.PublishWithResultAsync("t/1", "x");
                faulty.PublishOverride = null;

                await client.DisconnectAsync();
                await client.DisconnectAsync();

                var report = Assert.Single(log.Entries, e => e.Id.Id == ResilientMqttLogEvents.PublishFailuresNotLogged.Id);
                Assert.Contains("4", report.Message);
            }
        }

        private static MqttApplicationMessage Message() => new MqttApplicationMessageBuilder().WithTopic("t/1").Build();

        [Fact]
        public void A_full_buffer_logs_one_warning_per_window_and_reports_the_rest_when_it_drains()   // #71
        {
            var log = new ListLogger<ResilientMqttClient>();
            var buffer = new InMemoryOfflineBuffer(1, log);

            for (var i = 0; i < 6; i++) Assert.True(buffer.TryEnqueue(Message()));   // 5 overflows

            Assert.Equal(1, buffer.Count);
            Assert.Equal(1, log.Count(ResilientMqttLogEvents.BufferOverflow));

            Assert.True(buffer.TryDequeue(out _));
            var report = Assert.Single(log.Entries, e => e.Id.Id == ResilientMqttLogEvents.BufferDropsNotLogged.Id);
            Assert.Contains("4", report.Message);
        }

        [Fact]
        public async Task A_buffer_that_stays_full_logs_again_each_window_with_the_count()   // #71
        {
            var log = new ListLogger<ResilientMqttClient>();
            var buffer = new InMemoryOfflineBuffer(1, log, TimeSpan.FromMilliseconds(200));

            for (var i = 0; i < 4; i++) buffer.TryEnqueue(Message());   // 3 overflows: 1 logged, 2 counted
            await Task.Delay(300);
            buffer.TryEnqueue(Message());

            var warnings = log.Entries.Where(e => e.Id.Id == ResilientMqttLogEvents.BufferOverflow.Id).ToList();
            Assert.Equal(2, warnings.Count);
            Assert.Contains("2 more", warnings[1].Message);
        }

        [Fact]
        public async Task The_denied_topic_log_map_stays_bounded()   // #60
        {
            await using var broker = await TestBroker.StartAsync();
            broker.DenyPublish = _ => true;
            await using var client = TestClients.Create(TestClients.Options(broker.Port));
            Assert.True(await client.ConnectAsync());

            var total = ResilientMqttClient.MaxDeniedTopicsTracked + 5;
            for (var i = 0; i < total; i++)
                Assert.Equal(ResilientMqttPublishOutcome.Denied, await client.PublishWithResultAsync($"denied/{i}", "x"));

            Assert.Equal(total, client.ConnectionInfo.DeniedPublishes);
            Assert.True(client.DeniedTopicsTracked <= ResilientMqttClient.MaxDeniedTopicsTracked,
                $"tracked {client.DeniedTopicsTracked} topics");
        }
    }
}

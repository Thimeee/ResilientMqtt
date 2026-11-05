using MQTTnet;
using MQTTnet.Exceptions;
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
                client.PublishFailureLogWindow = TimeSpan.FromMilliseconds(200);
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

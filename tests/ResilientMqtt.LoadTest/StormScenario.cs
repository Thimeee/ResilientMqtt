using System.Collections.Concurrent;
using System.Diagnostics;
using ResilientMqtt.TestKit;

namespace ResilientMqtt.LoadTest
{
    /// <summary>
    /// Reconnect storm: N clients (kiosk-like settings) lose the broker for a while — via a proxy cut or a real broker
    /// restart — and must all come back on their own, with their subscriptions working.
    /// PASS = no client stuck, every client receives a command after the outage, slowest reconnect within the limit.
    /// </summary>
    internal sealed class StormScenario(int clients, int outageSeconds, string mode, string outDir)
    {
        private const int MaxReconnectSecondsAllowed = 180;   // MaxDelay 60 s + jitter + connect timeout, with margin

        private sealed class Kiosk
        {
            public required string Id;
            public required IResilientMqttClient Client;
            public DateTime? RestoredAt;
            public int Commands;
        }

        public async Task<bool> RunAsync()
        {
            var runId = DateTime.UtcNow.ToString("HHmmss");
            var broker = ChaosEnvironment.Mosquitto;
            var log = new CountingLogger<ResilientMqttClient>();
            using var toxi = new Toxiproxy();
            await toxi.EnsureProxiesAsync(new[] { broker });
            await toxi.ResetAsync();

            Console.WriteLine($"[storm] {clients} clients · outage {outageSeconds}s · mode {mode}");
            var before = ProcessStats.Sample();
            var kiosks = Enumerable.Range(0, clients).Select(i =>
            {
                var id = $"storm-{runId}-{i:D4}";
                var options = ChaosEnvironment.Options(broker, viaProxy: mode == "proxy", o =>
                {
                    o.ClientId = id;
                    o.KeepAliveSeconds = 40;              // like the kiosk agent
                    o.CleanSession = true;
                    o.Reconnect.BaseDelaySeconds = 1;
                    o.Reconnect.MaxDelaySeconds = 60;
                    o.Reconnect.UseJitter = true;
                    o.Reconnect.ConnectTimeoutSeconds = 15;
                });
                var k = new Kiosk { Id = id, Client = ChaosEnvironment.CreateClient(options, log, healthInterval: TimeSpan.FromSeconds(60), realBackoff: true) };
                return k;
            }).ToList();

            var restoreAt = DateTime.MaxValue;
            foreach (var k in kiosks)
            {
                var kiosk = k;
                await k.Client.SubscribeAsync($"load/{k.Id}/cmd", (_, _, _) => { Interlocked.Increment(ref kiosk.Commands); return Task.CompletedTask; });
                k.Client.ConnectionRestored += _ =>
                {
                    if (DateTime.UtcNow >= restoreAt && kiosk.RestoredAt is null) kiosk.RestoredAt = DateTime.UtcNow;
                    return Task.CompletedTask;
                };
            }

            // 1. Connect everyone (50 at a time, like a fleet booting)
            var sw = Stopwatch.StartNew();
            foreach (var batch in kiosks.Chunk(50))
                await Task.WhenAll(batch.Select(k => k.Client.ConnectAsync()));
            var allUp = await ChaosEnvironment.Eventually(() => kiosks.All(k => k.Client.IsConnected), TimeSpan.FromMinutes(3));
            var connectAllSeconds = sw.Elapsed.TotalSeconds;
            Console.WriteLine($"[storm] all connected: {allUp} in {connectAllSeconds:F1}s");

            // Telemetry like the agent heartbeat (QoS 0 every 5 s)
            using var telemetryStop = new CancellationTokenSource();
            var telemetry = Task.Run(async () =>
            {
                while (!telemetryStop.IsCancellationRequested)
                {
                    foreach (var k in kiosks) _ = k.Client.PublishAsync("telemetry", new { cpu = 7 }, qos: ResilientMqttQos.AtMostOnce);
                    try { await Task.Delay(5000, telemetryStop.Token); } catch (OperationCanceledException) { }
                }
            });
            await Task.Delay(TimeSpan.FromSeconds(15));

            // 2. Outage
            Console.WriteLine($"[storm] outage starts ({mode})");
            if (mode == "proxy") await toxi.SetEnabledAsync(broker, false);
            else await ChaosEnvironment.DockerAsync($"stop {broker.Container}");
            var downAll = await ChaosEnvironment.Eventually(() => kiosks.All(k => !k.Client.IsConnected), TimeSpan.FromSeconds(60));
            await Task.Delay(TimeSpan.FromSeconds(outageSeconds));
            var brokerDuring = await ProcessStats.DockerStatsAsync(broker.Container);

            restoreAt = DateTime.UtcNow;
            if (mode == "proxy") await toxi.SetEnabledAsync(broker, true);
            else await ChaosEnvironment.DockerAsync($"start {broker.Container}");
            Console.WriteLine("[storm] broker reachable again — waiting for every client");

            // 3. Recovery
            var recovered = await ChaosEnvironment.Eventually(() => kiosks.All(k => k.Client.IsConnected), TimeSpan.FromMinutes(5));
            await Task.Delay(TimeSpan.FromSeconds(5));
            var brokerAfter = await ProcessStats.DockerStatsAsync(broker.Container);

            // 4. Every client must still receive commands (subscriptions restored)
            await using (var server = ChaosEnvironment.CreateClient(ChaosEnvironment.Options(broker, viaProxy: false, o => o.ClientId = $"storm-server-{runId}")))
            {
                await server.ConnectAsync();
                foreach (var batch in kiosks.Chunk(100))
                    await Task.WhenAll(batch.Select(k => server.PublishWithResultAsync($"load/{k.Id}/cmd", "reboot?", useTopicPrefix: false)));
            }
            await ChaosEnvironment.Eventually(() => kiosks.All(k => k.Commands >= 1), TimeSpan.FromSeconds(30));
            telemetryStop.Cancel();
            await telemetry;

            // 5. Measure
            var reconnect = kiosks.Where(k => k.RestoredAt is not null)
                .Select(k => (k.RestoredAt!.Value - restoreAt).TotalSeconds).OrderBy(x => x).ToList();
            var stuck = kiosks.Where(k => !k.Client.IsConnected).ToList();
            var missingCommand = kiosks.Where(k => k.Commands == 0).ToList();
            var duplicates = kiosks.Sum(k => Math.Max(0, k.Commands - 1));
            var after = ProcessStats.Sample();
            var max = reconnect.Count > 0 ? reconnect[^1] : double.NaN;

            var pass = allUp && downAll && recovered && stuck.Count == 0 && missingCommand.Count == 0
                       && reconnect.Count == clients && max <= MaxReconnectSecondsAllowed;

            var data = new
            {
                scenario = "storm", mode, clients, outageSeconds, pass,
                connectAllSeconds, allConnectedAtStart = allUp, allDroppedInOutage = downAll, recoveredAll = recovered,
                reconnectSeconds = new
                {
                    p50 = Percentiles.Of(reconnect, 50), p95 = Percentiles.Of(reconnect, 95), p99 = Percentiles.Of(reconnect, 99), max,
                    measured = reconnect.Count,
                },
                stuck = stuck.Select(k => new { k.Id, failure = k.Client.LastFailure?.ToString() }),
                missingCommand = missingCommand.Select(k => k.Id), duplicateCommands = duplicates,
                process = new { before, after }, broker = new { during = brokerDuring, after = brokerAfter },
                logCounts = log.Counts, recentWarnings = log.Recent.TakeLast(50),
            };
            var md = $"""
                # Reconnect storm — {mode} — {(pass ? "PASS ✅" : "FAIL ❌")}

                | | |
                |---|---|
                | Clients | {clients} |
                | Outage | {outageSeconds} s ({(mode == "proxy" ? "network cut (Toxiproxy)" : "broker container stopped + started")}) |
                | All connected at start | {allUp} in {connectAllSeconds:F1} s |
                | All dropped in the outage | {downAll} |
                | **All reconnected** | **{recovered}** (stuck: {stuck.Count}) |
                | Reconnect time after the broker came back | p50 {Percentiles.Of(reconnect, 50):F1} s · p95 {Percentiles.Of(reconnect, 95):F1} s · p99 {Percentiles.Of(reconnect, 99):F1} s · **max {max:F1} s** (limit {MaxReconnectSecondsAllowed} s) |
                | **Command after the outage** | missing on {missingCommand.Count} client(s) · duplicates {duplicates} |
                | Test process | {before.WorkingSetMb:F0} → {after.WorkingSetMb:F0} MB · threads {before.Threads} → {after.Threads} |
                | Broker (docker stats) | during: {brokerDuring} · after: {brokerAfter} |

                Library log events: {string.Join(", ", log.Counts.OrderByDescending(kv => kv.Value).Select(kv => $"{kv.Key}={kv.Value}"))}

                """;
            await Report.WriteAsync(outDir, $"storm-{mode}", data, md);

            foreach (var batch in kiosks.Chunk(100))
                await Task.WhenAll(batch.Select(async k => { try { await k.Client.DisposeAsync(); } catch { } }));
            return pass;
        }
    }
}

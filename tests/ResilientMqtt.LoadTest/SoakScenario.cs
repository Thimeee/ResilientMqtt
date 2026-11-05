using System.Diagnostics;
using System.Globalization;
using System.Text;
using ResilientMqtt.TestKit;

namespace ResilientMqtt.LoadTest
{
    /// <summary>
    /// Soak: N clients run for hours while a fault is injected every few minutes (latency, black hole, connection reset,
    /// network cut, slow link, broker restart). Every client publishes a QoS1 sequence number every 5 s (offline buffer
    /// on) and receives a QoS1 command every minute (persistent sessions; half of the clients subscribe in OnConnected).
    /// PASS = nothing lost in either direction, every client connected at the end, memory and threads flat.
    /// </summary>
    internal sealed class SoakScenario(int clients, TimeSpan duration, TimeSpan faultEvery, string outDir)
    {
        private const double MaxGrowthPercent = 25;

        private sealed class Kiosk
        {
            public required string Id;
            public required IResilientMqttClient Client;
            public int LastSeq;            // last sequence number handed to the library (Sent or Buffered)
            public long DroppedPublishes;  // outcome Dropped / Denied (should stay 0)
            public readonly Seen Commands = new();
        }

        public async Task<bool> RunAsync()
        {
            var runId = DateTime.UtcNow.ToString("MMddHHmm");
            var broker = ChaosEnvironment.Mosquitto;
            var log = new CountingLogger<ResilientMqttClient>();
            using var toxi = new Toxiproxy();
            await toxi.EnsureProxiesAsync(new[] { broker });
            await toxi.ResetAsync();
            Directory.CreateDirectory(outDir);

            Console.WriteLine($"[soak] {clients} clients for {duration} · a fault every {faultEvery}");

            // Verifier = the "server": direct port, persistent session, receives every sequence number.
            var seqSeen = new Dictionary<string, Seen>();
            var verifier = ChaosEnvironment.CreateClient(ChaosEnvironment.Options(broker, viaProxy: false, o =>
            {
                o.ClientId = $"soak-verifier-{runId}";
                o.CleanSession = false;
            }), log, retryDelay: TimeSpan.FromSeconds(1));
            await verifier.SubscribeAsync("load/+/seq", (topic, payload, _) =>
            {
                var id = topic.Split('/')[1];
                if (seqSeen.TryGetValue(id, out var seen) && int.TryParse(payload, out var n)) seen.Mark(n);
                return Task.CompletedTask;
            });
            if (!await verifier.ConnectAsync()) throw new InvalidOperationException("verifier could not connect");

            var kiosks = Enumerable.Range(0, clients).Select(i =>
            {
                var id = $"soak-{runId}-{i:D3}";
                seqSeen[id] = new Seen();
                var options = ChaosEnvironment.Options(broker, viaProxy: true, o =>
                {
                    o.ClientId = id;
                    o.KeepAliveSeconds = 40;
                    o.CleanSession = false;               // persistent: commands sent while it is offline must arrive later
                    o.OfflineBuffer.Enabled = true;       // its own publishes survive outages
                    o.OfflineBuffer.MaxSize = 2000;
                    o.PublishTimeoutSeconds = 15;
                    o.HealthCheck.PingTimeoutSeconds = 10;
                    o.Reconnect.MaxDelaySeconds = 30;
                    o.Reconnect.ConnectTimeoutSeconds = 15;
                });
                return new Kiosk { Id = id, Client = ChaosEnvironment.CreateClient(options, log, healthInterval: TimeSpan.FromSeconds(30), realBackoff: true) };
            }).ToList();

            for (var i = 0; i < kiosks.Count; i++)
            {
                var k = kiosks[i];
                Task Handler(string _, string p, CancellationToken __) { if (int.TryParse(p, out var n)) k.Commands.Mark(n); return Task.CompletedTask; }
                if (i % 2 == 0) await k.Client.SubscribeAsync($"load/{k.Id}/cmd", Handler);                     // registered up front
                else k.Client.OnConnected += () => k.Client.SubscribeAsync($"load/{k.Id}/cmd", Handler);         // subscribed in OnConnected (#44 path)
            }
            foreach (var batch in kiosks.Chunk(50)) await Task.WhenAll(batch.Select(k => k.Client.ConnectAsync()));

            var metrics = new StringBuilder("utc,elapsed_min,connected,fault,working_set_mb,gc_heap_mb,threads,handles,seq_published,commands_sent\n");
            var samples = new List<(double Minutes, double WorkingSet, int Threads)>();
            var commandsSent = 0;
            var lastFault = "none";
            var faultsDone = new List<string>();
            var faultsStarted = 0;
            var faultRunning = 0;
            var started = Stopwatch.StartNew();
            var nextFault = faultEvery;
            var nextCommand = TimeSpan.FromMinutes(1);
            var nextMetric = TimeSpan.Zero;
            var faults = new Func<Task<string>>[]
            {
                async () => { await toxi.AddToxicAsync(broker, "f", "latency", "downstream", new { latency = 3000, jitter = 500 }); await Task.Delay(60_000); return "latency 3 s × 60 s"; },
                async () => { await toxi.AddToxicAsync(broker, "f", "timeout", "downstream", new { timeout = 0 }); await Task.Delay(90_000); return "black hole (broker → kiosks) × 90 s"; },
                async () => { await toxi.AddToxicAsync(broker, "f", "reset_peer", "upstream", new { timeout = 0 }); await Task.Delay(5_000); return "connection reset × 5 s"; },
                async () => { await toxi.SetEnabledAsync(broker, false); await Task.Delay(60_000); return "network cut × 60 s"; },
                async () => { await toxi.AddToxicAsync(broker, "f", "bandwidth", "upstream", new { rate = 2 }); await Task.Delay(60_000); return "slow link 2 KB/s × 60 s"; },
                async () => { await ChaosEnvironment.DockerAsync($"restart {broker.Container}"); return "broker restart"; },
            };

            while (started.Elapsed < duration)
            {
                // QoS1 sequence number from every kiosk
                foreach (var k in kiosks)
                {
                    var n = Interlocked.Increment(ref k.LastSeq);
                    var kiosk = k;
                    _ = k.Client.PublishWithResultAsync($"load/{k.Id}/seq", n.ToString(CultureInfo.InvariantCulture), qos: ResilientMqttQos.AtLeastOnce, useTopicPrefix: false)
                        .ContinueWith(t =>
                        {
                            if (t.Result is ResilientMqttPublishOutcome.Dropped or ResilientMqttPublishOutcome.Denied)
                                Interlocked.Increment(ref kiosk.DroppedPublishes);
                        }, TaskScheduler.Default);
                }

                if (started.Elapsed >= nextCommand)
                {
                    commandsSent++;
                    foreach (var k in kiosks)
                        _ = verifier.PublishWithResultAsync($"load/{k.Id}/cmd", commandsSent.ToString(CultureInfo.InvariantCulture), useTopicPrefix: false);
                    nextCommand += TimeSpan.FromMinutes(1);
                }

                if (started.Elapsed >= nextFault && Interlocked.CompareExchange(ref faultRunning, 1, 0) == 0)   // one fault at a time
                {
                    var fault = faults[faultsStarted++ % faults.Length];   // counted at START
                    _ = Task.Run(async () =>
                    {
                        try { lastFault = await fault(); }
                        catch (Exception ex) { lastFault = "fault failed: " + ex.Message; }
                        finally { try { await toxi.ResetAsync(); } catch { } Interlocked.Exchange(ref faultRunning, 0); }
                        lock (faultsDone) faultsDone.Add($"{DateTime.Now:HH:mm} {lastFault}");
                        Console.WriteLine($"[soak] fault done: {lastFault}");
                    });
                    nextFault += faultEvery;
                }

                if (started.Elapsed >= nextMetric)
                {
                    var s = ProcessStats.Sample();
                    var connected = kiosks.Count(k => k.Client.IsConnected);
                    samples.Add((started.Elapsed.TotalMinutes, s.WorkingSetMb, s.Threads));
                    metrics.Append(CultureInfo.InvariantCulture, $"{DateTime.UtcNow:O},{started.Elapsed.TotalMinutes:F1},{connected},\"{lastFault}\",{s.WorkingSetMb:F1},{s.GcHeapMb:F1},{s.Threads},{s.Handles},{kiosks.Sum(k => (long)k.LastSeq)},{commandsSent}\n");
                    await File.WriteAllTextAsync(Path.Combine(outDir, "soak-metrics.csv"), metrics.ToString());
                    Console.WriteLine($"[soak] {started.Elapsed:hh\\:mm\\:ss} connected {connected}/{clients} · {s.WorkingSetMb:F0} MB · {s.Threads} threads · last fault: {lastFault}");
                    nextMetric += TimeSpan.FromMinutes(1);
                }

                await Task.Delay(5000);
            }

            // Settle: no more faults, let everyone reconnect and drain
            await toxi.ResetAsync();
            var allConnected = await ChaosEnvironment.Eventually(() => kiosks.All(k => k.Client.IsConnected) && verifier.IsConnected, TimeSpan.FromMinutes(5));
            await Task.Delay(TimeSpan.FromMinutes(2));

            var seqGaps = kiosks.Select(k => (k.Id, Missing: seqSeen[k.Id].Missing(1, k.LastSeq))).Where(x => x.Missing.Count > 0).ToList();
            var cmdGaps = kiosks.Select(k => (k.Id, Missing: k.Commands.Missing(1, commandsSent))).Where(x => x.Missing.Count > 0).ToList();
            var seqDuplicates = seqSeen.Values.Sum(s => s.Duplicates);
            var cmdDuplicates = kiosks.Sum(k => k.Commands.Duplicates);
            var dropped = kiosks.Sum(k => k.DroppedPublishes);

            // Leak check: average of the first 10 % (after a 10-minute warm-up) vs the last 10 % of samples
            var warm = samples.Where(s => s.Minutes >= Math.Min(10, duration.TotalMinutes / 4)).ToList();
            var slice = Math.Max(1, warm.Count / 10);
            double Avg(IEnumerable<double> x) => x.DefaultIfEmpty(double.NaN).Average();
            var wsStart = Avg(warm.Take(slice).Select(s => s.WorkingSet));
            var wsEnd = Avg(warm.TakeLast(slice).Select(s => s.WorkingSet));
            var thStart = Avg(warm.Take(slice).Select(s => (double)s.Threads));
            var thEnd = Avg(warm.TakeLast(slice).Select(s => (double)s.Threads));
            var wsGrowth = (wsEnd - wsStart) / wsStart * 100;
            var thGrowth = (thEnd - thStart) / thStart * 100;

            var pass = allConnected && seqGaps.Count == 0 && cmdGaps.Count == 0 && dropped == 0
                       && !(wsGrowth > MaxGrowthPercent) && !(thGrowth > MaxGrowthPercent);

            var data = new
            {
                scenario = "soak", clients, duration = duration.ToString(), faultEvery = faultEvery.ToString(), pass,
                allConnectedAtEnd = allConnected,
                sequence = new { published = kiosks.Sum(k => (long)k.LastSeq), clientsWithGaps = seqGaps.Count, gaps = seqGaps.Take(20).Select(g => new { g.Id, missing = g.Missing.Take(20) }), duplicates = seqDuplicates, droppedOrDenied = dropped },
                commands = new { sent = commandsSent, clientsWithGaps = cmdGaps.Count, gaps = cmdGaps.Take(20).Select(g => new { g.Id, missing = g.Missing.Take(20) }), duplicates = cmdDuplicates },
                memory = new { workingSetStartMb = wsStart, workingSetEndMb = wsEnd, growthPercent = wsGrowth, threadsStart = thStart, threadsEnd = thEnd, threadGrowthPercent = thGrowth },
                faults = faultsDone, logCounts = log.Counts, recentWarnings = log.Recent.TakeLast(50),
            };
            var md = $"""
                # Soak — {duration} — {(pass ? "PASS ✅" : "FAIL ❌")}

                | | |
                |---|---|
                | Clients | {clients} (persistent sessions, offline buffer on; half subscribe in OnConnected) |
                | Faults injected | {faultsDone.Count} (every {faultEvery.TotalMinutes:F0} min, rotating) |
                | All connected at the end | {allConnected} |
                | **QoS1 kiosk → server** | {kiosks.Sum(k => (long)k.LastSeq)} published · **clients with gaps: {seqGaps.Count}** · duplicates {seqDuplicates} · dropped/denied {dropped} |
                | **QoS1 server → kiosk (commands)** | {commandsSent} rounds · **clients with gaps: {cmdGaps.Count}** · duplicates {cmdDuplicates} |
                | Working set (after warm-up → end) | {wsStart:F0} → {wsEnd:F0} MB ({wsGrowth:+0.0;-0.0} %, limit {MaxGrowthPercent} %) |
                | Threads | {thStart:F0} → {thEnd:F0} ({thGrowth:+0.0;-0.0} %) |

                Duplicates are allowed (QoS1 is at-least-once); gaps are not.

                Faults: {string.Join(" · ", faultsDone)}

                Library log events: {string.Join(", ", log.Counts.OrderByDescending(kv => kv.Value).Select(kv => $"{kv.Key}={kv.Value}"))}

                Per-minute metrics: soak-metrics.csv
                """;
            await Report.WriteAsync(outDir, "soak", data, md);

            foreach (var batch in kiosks.Chunk(50))
                await Task.WhenAll(batch.Select(async k => { try { await k.Client.DisposeAsync(); } catch { } }));
            await verifier.DisposeAsync();
            return pass;
        }
    }
}

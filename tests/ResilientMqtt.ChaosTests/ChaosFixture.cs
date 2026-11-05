using ResilientMqtt.TestKit;

// Real brokers + a shared Toxiproxy: one test at a time.
[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace ResilientMqtt.ChaosTests
{
    /// <summary>Creates the Toxiproxy proxies once; every test resets toxics before it starts.</summary>
    public sealed class ChaosFixture : IAsyncLifetime
    {
        internal Toxiproxy Toxi { get; } = new();

        public async Task InitializeAsync()
        {
            if (!ChaosEnvironment.Enabled) return;
            if (!await Toxi.IsUpAsync())
                throw new InvalidOperationException("Toxiproxy is not reachable on " + ChaosEnvironment.ToxiproxyApi + " — start the environment with tests/run-all.ps1.");
            await Toxi.EnsureProxiesAsync(ChaosEnvironment.All.Where(b => ChaosEnvironment.BrokerEnabled(b.Name)));
        }

        public async Task DisposeAsync()
        {
            if (ChaosEnvironment.Enabled) { try { await Toxi.ResetAsync(); } catch { } }
            Toxi.Dispose();
        }
    }

    [CollectionDefinition("chaos")]
    public sealed class ChaosCollection : ICollectionFixture<ChaosFixture> { }

    public abstract class ChaosTestBase
    {
        private protected readonly ChaosFixture Fixture;
        protected readonly ITestOutputHelper Output;

        protected ChaosTestBase(ChaosFixture fixture, ITestOutputHelper output)
        {
            Fixture = fixture;
            Output = output;
        }

        public static IEnumerable<object[]> Brokers => ChaosEnvironment.All.Select(b => new object[] { b.Name });

        /// <summary>Skips unless the chaos environment (and this broker) is up; resets all toxics.</summary>
        private protected async Task<BrokerTarget> RequireAsync(string broker = "mosquitto")
        {
            Skip.IfNot(ChaosEnvironment.Enabled, "Chaos environment not enabled (run tests/run-all.ps1).");
            Skip.IfNot(ChaosEnvironment.BrokerEnabled(broker), $"Broker '{broker}' not part of this run (RMQTT_BROKERS).");
            await Fixture.Toxi.ResetAsync();
            return ChaosEnvironment.Broker(broker);
        }

        protected void Log(string message) => Output.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] {message}");

        protected void LogFailure(string label, ResilientMqttFailure? f)
            => Log($"{label}: {(f is null ? "none" : $"{f.Kind} | code={f.Code} | {f.ExceptionType} | {f.Detail}")}");
    }
}

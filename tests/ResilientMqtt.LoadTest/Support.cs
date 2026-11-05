using System.Collections;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace ResilientMqtt.LoadTest
{
    /// <summary>Shared logger for hundreds of clients: counts events by id and keeps the last warnings/errors.</summary>
    internal sealed class CountingLogger<T> : ILogger<T>
    {
        private readonly ConcurrentDictionary<string, long> _counts = new();
        private readonly ConcurrentQueue<string> _recent = new();

        public IReadOnlyDictionary<string, long> Counts => _counts;
        public IEnumerable<string> Recent => _recent;

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Information;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            var key = string.IsNullOrEmpty(eventId.Name) ? $"{logLevel}" : eventId.Name!;
            _counts.AddOrUpdate(key, 1, (_, n) => n + 1);
            if (logLevel >= LogLevel.Warning)
            {
                _recent.Enqueue($"{DateTime.Now:HH:mm:ss} {logLevel} {formatter(state, exception)}");
                while (_recent.Count > 300) _recent.TryDequeue(out _);
            }
        }
    }

    /// <summary>A growable bit set — which sequence numbers / commands arrived (cheap for 24 h runs).</summary>
    internal sealed class Seen
    {
        private readonly object _gate = new();
        private BitArray _bits = new(1024);
        public long Duplicates;

        public void Mark(int n)
        {
            lock (_gate)
            {
                if (n >= _bits.Length) _bits.Length = Math.Max(n + 1, _bits.Length * 2);
                if (_bits[n]) Duplicates++;
                else _bits[n] = true;
            }
        }

        /// <summary>Numbers in [from, to] that never arrived.</summary>
        public List<int> Missing(int from, int to)
        {
            var missing = new List<int>();
            lock (_gate)
                for (var i = from; i <= to; i++)
                    if (i >= _bits.Length || !_bits[i]) missing.Add(i);
            return missing;
        }

        public bool Has(int n) { lock (_gate) return n < _bits.Length && _bits[n]; }
    }

    internal static class ProcessStats
    {
        public static (double WorkingSetMb, double GcHeapMb, int Threads, int Handles) Sample()
        {
            using var p = Process.GetCurrentProcess();
            p.Refresh();
            return (p.WorkingSet64 / 1024d / 1024d, GC.GetTotalMemory(false) / 1024d / 1024d, p.Threads.Count, p.HandleCount);
        }

        public static async Task<string> DockerStatsAsync(string container)
        {
            try
            {
                using var p = Process.Start(new ProcessStartInfo("docker", $"stats --no-stream --format \"{{{{.CPUPerc}}}} cpu, {{{{.MemUsage}}}}\" {container}")
                {
                    RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false,
                })!;
                var output = await p.StandardOutput.ReadToEndAsync();
                await p.WaitForExitAsync();
                return output.Trim();
            }
            catch (Exception ex) { return "n/a (" + ex.Message + ")"; }
        }
    }

    internal static class Percentiles
    {
        public static double Of(IReadOnlyList<double> sorted, double p)
            => sorted.Count == 0 ? double.NaN : sorted[Math.Clamp((int)Math.Ceiling(p / 100d * sorted.Count) - 1, 0, sorted.Count - 1)];
    }

    internal static class Report
    {
        private static readonly JsonSerializerOptions Json = new() { WriteIndented = true };

        public static async Task WriteAsync(string outDir, string name, object data, string markdown)
        {
            Directory.CreateDirectory(outDir);
            await File.WriteAllTextAsync(Path.Combine(outDir, name + ".json"), JsonSerializer.Serialize(data, Json));
            await File.WriteAllTextAsync(Path.Combine(outDir, name + ".md"), markdown);
            Console.WriteLine(markdown);
        }
    }

    /// <summary>Generates the throwaway PKI the docker Mosquitto uses (ca.pem, server.pem/.key, client.pfx).</summary>
    internal static class Certs
    {
        public static void Generate(string outDir, string pfxPassword)
        {
            Directory.CreateDirectory(outDir);
            using var caKey = RSA.Create(2048);
            var caReq = new CertificateRequest("CN=ResilientMqtt Chaos CA", caKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            caReq.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
            caReq.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
            caReq.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(caReq.PublicKey, false));
            using var ca = caReq.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(5));
            File.WriteAllText(Path.Combine(outDir, "ca.pem"), ca.ExportCertificatePem());

            using var serverKey = RSA.Create(2048);
            var serverReq = new CertificateRequest("CN=localhost", serverKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            serverReq.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, false));
            serverReq.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
            serverReq.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(new OidCollection { new("1.3.6.1.5.5.7.3.1") }, false));
            var san = new SubjectAlternativeNameBuilder();
            san.AddDnsName("localhost");
            san.AddDnsName("mosquitto");
            san.AddIpAddress(IPAddress.Loopback);
            serverReq.CertificateExtensions.Add(san.Build());
            using var server = serverReq.Create(ca, DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(2), RandomNumberGenerator.GetBytes(8));
            File.WriteAllText(Path.Combine(outDir, "server.pem"), server.ExportCertificatePem());
            File.WriteAllText(Path.Combine(outDir, "server.key"), serverKey.ExportPkcs8PrivateKeyPem());

            using var clientKey = RSA.Create(2048);
            var clientReq = new CertificateRequest("CN=chaos-client", clientKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            clientReq.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, false));
            clientReq.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
            clientReq.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(new OidCollection { new("1.3.6.1.5.5.7.3.2") }, false));
            using var client = clientReq.Create(ca, DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(2), RandomNumberGenerator.GetBytes(8));
            using var clientWithKey = client.CopyWithPrivateKey(clientKey);
            File.WriteAllBytes(Path.Combine(outDir, "client.pfx"), clientWithKey.Export(X509ContentType.Pfx, pfxPassword));

            Console.WriteLine($"Certificates written to {outDir} (server thumbprint {server.Thumbprint}).");
        }
    }
}

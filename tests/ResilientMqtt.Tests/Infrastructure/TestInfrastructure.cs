using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MQTTnet;
using MQTTnet.Protocol;
using MQTTnet.Server;
using ResilientMqtt.Buffering;
using ResilientMqtt.Internal;

namespace ResilientMqtt.Tests.Infrastructure
{
    internal static class Net
    {
        public static int FreePort()
        {
            var l = new TcpListener(IPAddress.Loopback, 0);
            l.Start();
            var port = ((IPEndPoint)l.LocalEndpoint).Port;
            l.Stop();
            return port;
        }

        public static async Task<bool> Eventually(Func<bool> condition, int timeoutMs = 8000)
        {
            var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
            while (DateTime.UtcNow < deadline)
            {
                if (condition()) return true;
                await Task.Delay(25);
            }
            return condition();
        }
    }

    /// <summary>An in-process MQTTnet broker with hooks to refuse sign-ins, subscriptions and publishes.</summary>
    internal sealed class TestBroker : IAsyncDisposable
    {
        public int Port { get; }
        public MqttServer Server { get; }
        public string? RequiredPassword { get; set; }
        public Func<string, bool> DenySubscribe { get; set; } = _ => false;
        public Func<string, bool> DenyPublish { get; set; } = _ => false;

        public ConcurrentQueue<ValidatingSnapshot> Connects { get; } = new();
        public ConcurrentQueue<(string ClientId, string Topic)> Subscriptions { get; } = new();
        public ConcurrentQueue<(string ClientId, string Topic)> Unsubscribed { get; } = new();
        /// <summary>QoS1/2 messages a client has acknowledged (PUBACK) — lets tests wait for an ack instead of guessing.</summary>
        public int AckedByClients => Volatile.Read(ref _acked);
        private int _acked;
        public ConcurrentQueue<(string Topic, string Payload)> Published { get; } = new();

        public sealed record ValidatingSnapshot(string ClientId, bool? CleanSession, uint SessionExpiry, MQTTnet.Formatter.MqttProtocolVersion Protocol, string? Password);

        private TestBroker(int port, MqttServer server) { Port = port; Server = server; }

        /// <summary>Client certificate the broker saw on the last TLS connect (mutual TLS tests).</summary>
        public string? LastClientCertificateThumbprint { get; private set; }

        /// <param name="tlsCertificate">When set: the broker listens ONLY on a TLS endpoint with this certificate.</param>
        /// <param name="requireClientCertificate">TLS only: demand a client certificate (mutual TLS) issued by <paramref name="clientCa"/>.</param>
        public static async Task<TestBroker> StartAsync(
            int? port = null, bool persistentSessions = true,
            System.Security.Cryptography.X509Certificates.X509Certificate2? tlsCertificate = null,
            bool requireClientCertificate = false,
            System.Security.Cryptography.X509Certificates.X509Certificate2? clientCa = null)
        {
            var p = port ?? Net.FreePort();
            var factory = new MqttServerFactory();
            var builder = factory.CreateServerOptionsBuilder().WithPersistentSessions(persistentSessions);
            if (tlsCertificate is null)
            {
                builder.WithDefaultEndpoint()
                       .WithDefaultEndpointBoundIPAddress(IPAddress.Loopback)
                       .WithDefaultEndpointPort(p);
            }
            else
            {
                builder.WithoutDefaultEndpoint()
                       .WithEncryptedEndpoint()
                       .WithEncryptedEndpointBoundIPAddress(IPAddress.Loopback)
                       .WithEncryptedEndpointPort(p)
                       .WithEncryptionCertificate(tlsCertificate);
                if (requireClientCertificate)
                    builder.WithClientCertificate((_, cert, _, _) =>
                        cert is not null && clientCa is not null && TestPki.IssuedBy(cert, clientCa), false);
            }
            var server = factory.CreateMqttServer(builder.Build());
            var broker = new TestBroker(p, server);

            server.ValidatingConnectionAsync += e =>
            {
                if (e.ClientCertificate is not null) broker.LastClientCertificateThumbprint = e.ClientCertificate.Thumbprint;
                broker.Connects.Enqueue(new ValidatingSnapshot(e.ClientId, e.CleanSession, e.SessionExpiryInterval, e.ProtocolVersion, e.Password));
                if (broker.RequiredPassword is not null && e.Password != broker.RequiredPassword)
                    e.ReasonCode = MqttConnectReasonCode.BadUserNameOrPassword;
                return Task.CompletedTask;
            };
            server.InterceptingSubscriptionAsync += e =>
            {
                if (broker.DenySubscribe(e.TopicFilter.Topic))
                {
                    e.ProcessSubscription = false;
                    e.Response.ReasonCode = MqttSubscribeReasonCode.NotAuthorized;
                }
                return Task.CompletedTask;
            };
            server.ClientSubscribedTopicAsync += e =>
            {
                broker.Subscriptions.Enqueue((e.ClientId, e.TopicFilter.Topic));
                return Task.CompletedTask;
            };
            server.ClientAcknowledgedPublishPacketAsync += _ =>
            {
                Interlocked.Increment(ref broker._acked);
                return Task.CompletedTask;
            };
            server.ClientUnsubscribedTopicAsync += e =>
            {
                broker.Unsubscribed.Enqueue((e.ClientId, e.TopicFilter));
                return Task.CompletedTask;
            };
            server.InterceptingPublishAsync += e =>
            {
                if (broker.DenyPublish(e.ApplicationMessage.Topic))
                {
                    e.ProcessPublish = false;
                    e.Response.ReasonCode = MqttPubAckReasonCode.NotAuthorized;
                    return Task.CompletedTask;
                }
                broker.Published.Enqueue((e.ApplicationMessage.Topic, e.ApplicationMessage.ConvertPayloadToString()));
                return Task.CompletedTask;
            };

            await server.StartAsync();
            return broker;
        }

        public Task KickAsync(string clientId, MqttDisconnectReasonCode reason = MqttDisconnectReasonCode.AdministrativeAction)
            => Server.DisconnectClientAsync(clientId,
                new MqttServerFactory().CreateMqttServerClientDisconnectOptionsBuilder().WithReasonCode(reason).Build());

        /// <summary>A plain MQTTnet client (publisher / intruder) for tests.</summary>
        public async Task<IMqttClient> RawClientAsync(string? clientId = null, bool cleanSession = true)
        {
            var c = new MqttClientFactory().CreateMqttClient();
            await c.ConnectAsync(new MqttClientOptionsBuilder()
                .WithTcpServer("127.0.0.1", Port)
                .WithClientId(clientId ?? "raw-" + Guid.NewGuid().ToString("N")[..8])
                .WithCredentials("u", RequiredPassword ?? "p")
                .WithCleanSession(cleanSession)
                .Build());
            return c;
        }

        public async ValueTask DisposeAsync()
        {
            try { await Server.StopAsync(new MqttServerFactory().CreateMqttServerStopOptionsBuilder().Build()); } catch { }
            Server.Dispose();
        }
    }

    /// <summary>Accepts TCP connections and never answers — a broker that hangs during the handshake.</summary>
    internal sealed class SilentListener : IDisposable
    {
        private readonly TcpListener _listener;
        private readonly List<TcpClient> _accepted = new();
        public int Port { get; }

        public SilentListener()
        {
            _listener = new TcpListener(IPAddress.Loopback, 0);
            _listener.Start();
            Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
            _ = AcceptLoop();
        }

        private async Task AcceptLoop()
        {
            try { while (true) { var c = await _listener.AcceptTcpClientAsync(); lock (_accepted) _accepted.Add(c); } }
            catch { /* stopped */ }
        }

        public void Dispose()
        {
            _listener.Stop();
            lock (_accepted) foreach (var c in _accepted) c.Dispose();
        }
    }

    /// <summary>
    /// TCP proxy in front of a broker. <see cref="FreezeDownstream"/> silently drops everything the broker sends on
    /// the connections that exist NOW (client → broker still flows) — a "zombie" connection: the socket stays open,
    /// the client can still write, but nothing comes back. Connections opened later work normally.
    /// </summary>
    internal sealed class FreezableProxy : IDisposable
    {
        private sealed class Link { public volatile bool Frozen; public TcpClient? A; public TcpClient? B; }

        private readonly TcpListener _listener;
        private readonly int _targetPort;
        private readonly List<Link> _links = new();
        public int Port { get; }
        public int ConnectionCount { get { lock (_links) return _links.Count; } }

        public FreezableProxy(int targetPort)
        {
            _targetPort = targetPort;
            _listener = new TcpListener(IPAddress.Loopback, 0);
            _listener.Start();
            Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
            _ = AcceptLoop();
        }

        public void FreezeDownstream() { lock (_links) foreach (var l in _links) l.Frozen = true; }

        private async Task AcceptLoop()
        {
            try
            {
                while (true)
                {
                    var client = await _listener.AcceptTcpClientAsync();
                    var upstream = new TcpClient();
                    await upstream.ConnectAsync(IPAddress.Loopback, _targetPort);
                    var link = new Link { A = client, B = upstream };
                    lock (_links) _links.Add(link);
                    _ = Pump(client.GetStream(), upstream.GetStream(), () => false);
                    _ = Pump(upstream.GetStream(), client.GetStream(), () => link.Frozen);
                }
            }
            catch { /* stopped */ }
        }

        private static async Task Pump(Stream from, Stream to, Func<bool> drop)
        {
            var buffer = new byte[8192];
            try
            {
                while (true)
                {
                    var n = await from.ReadAsync(buffer);
                    if (n == 0) break;
                    if (drop()) continue;
                    await to.WriteAsync(buffer.AsMemory(0, n));
                }
            }
            catch { /* closed */ }
            finally { try { to.Close(); } catch { } }
        }

        public void Dispose()
        {
            _listener.Stop();
            lock (_links) foreach (var l in _links) { l.A?.Dispose(); l.B?.Dispose(); }
        }
    }
    /// <summary>Constant small delay — keeps reconnect tests fast.</summary>
    internal sealed class FastRetry(int ms = 50) : IRetryStrategy
    {
        public TimeSpan GetNextDelay(int attempt) => TimeSpan.FromMilliseconds(ms);
    }

    internal sealed class ListLogger<T> : ILogger<T>
    {
        public ConcurrentQueue<(LogLevel Level, EventId Id, string Message)> Entries { get; } = new();
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => Entries.Enqueue((logLevel, eventId, formatter(state, exception)));
        public bool Has(EventId id) => Entries.Any(e => e.Id.Id == id.Id);
        public int Count(EventId id) => Entries.Count(e => e.Id.Id == id.Id);
    }

    internal static class TestClients
    {
        public static ResilientMqttOptions Options(int port, Action<ResilientMqttOptions>? configure = null)
        {
            var o = new ResilientMqttOptions
            {
                Host = "127.0.0.1",
                Port = port,
                ClientId = "test-" + Guid.NewGuid().ToString("N")[..8],
                Username = "u",
                Password = "p",
                KeepAliveSeconds = 60,
                CleanSession = true,
            };
            o.LastWill.Enabled = false;
            o.OfflineBuffer.Enabled = false;
            o.HealthCheck.Enabled = false;
            o.Reconnect.ConnectTimeoutSeconds = 2;
            o.Reconnect.StillDownAfterSeconds = 0;
            configure?.Invoke(o);
            return o;
        }

        public static ResilientMqttClient Create(
            ResilientMqttOptions options,
            ListLogger<ResilientMqttClient>? logger = null,
            IHealthCheck? healthCheck = null,
            ICircuitBreaker? breaker = null,
            IOfflineBuffer? buffer = null,
            IRetryStrategy? retry = null,
            IMqttClient? mqttClient = null,
            int heldMessageLimit = ResilientMqttClient.DefaultHeldMessageLimit)
            => new(
                mqttClient ?? new MqttClientFactory().CreateMqttClient(),
                Microsoft.Extensions.Options.Options.Create(options),
                retry ?? new FastRetry(),
                healthCheck ?? new NullHealthCheck(),
                buffer ?? (options.OfflineBuffer.Enabled
                    ? new InMemoryOfflineBuffer(options.OfflineBuffer.MaxSize, Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance)
                    : new NullOfflineBuffer()),
                breaker ?? new NullCircuitBreaker(),
                logger ?? new ListLogger<ResilientMqttClient>(),
                heldMessageLimit);
    }

    /// <summary>
    /// Fault-injection decorator around a real MQTTnet client: lets a test replace ConnectAsync / SubscribeAsync to
    /// produce exact failures (a hang, a wrapped cancellation, a late CONNACK, a slow SUBACK) on demand, deterministically.
    /// </summary>
    internal sealed class FaultyMqttClient(IMqttClient inner) : IMqttClient
    {
        public IMqttClient Inner { get; } = inner;
        public Func<MqttClientOptions, CancellationToken, Task<MqttClientConnectResult>>? ConnectOverride { get; set; }
        public Func<MqttClientSubscribeOptions, CancellationToken, Task<MqttClientSubscribeResult>>? SubscribeOverride { get; set; }

        public bool IsConnected => Inner.IsConnected;
        public MqttClientOptions Options => Inner.Options;

        public event Func<MqttApplicationMessageReceivedEventArgs, Task> ApplicationMessageReceivedAsync { add => Inner.ApplicationMessageReceivedAsync += value; remove => Inner.ApplicationMessageReceivedAsync -= value; }
        public event Func<MqttClientConnectedEventArgs, Task> ConnectedAsync { add => Inner.ConnectedAsync += value; remove => Inner.ConnectedAsync -= value; }
        public event Func<MqttClientConnectingEventArgs, Task> ConnectingAsync { add => Inner.ConnectingAsync += value; remove => Inner.ConnectingAsync -= value; }
        public event Func<MqttClientDisconnectedEventArgs, Task> DisconnectedAsync { add => Inner.DisconnectedAsync += value; remove => Inner.DisconnectedAsync -= value; }
        public event Func<MQTTnet.Diagnostics.PacketInspection.InspectMqttPacketEventArgs, Task> InspectPacketAsync { add => Inner.InspectPacketAsync += value; remove => Inner.InspectPacketAsync -= value; }

        public Task<MqttClientConnectResult> ConnectAsync(MqttClientOptions options, CancellationToken cancellationToken = default)
            => ConnectOverride is { } o ? o(options, cancellationToken) : Inner.ConnectAsync(options, cancellationToken);
        public Task<MqttClientSubscribeResult> SubscribeAsync(MqttClientSubscribeOptions options, CancellationToken cancellationToken = default)
            => SubscribeOverride is { } o ? o(options, cancellationToken) : Inner.SubscribeAsync(options, cancellationToken);
        public Task DisconnectAsync(MqttClientDisconnectOptions options, CancellationToken cancellationToken = default) => Inner.DisconnectAsync(options, cancellationToken);
        public Task PingAsync(CancellationToken cancellationToken = default) => Inner.PingAsync(cancellationToken);
        public Task<MqttClientPublishResult> PublishAsync(MqttApplicationMessage applicationMessage, CancellationToken cancellationToken = default) => Inner.PublishAsync(applicationMessage, cancellationToken);
        public Task SendEnhancedAuthenticationExchangeDataAsync(MqttEnhancedAuthenticationExchangeData data, CancellationToken cancellationToken = default) => Inner.SendEnhancedAuthenticationExchangeDataAsync(data, cancellationToken);
        public Task<MqttClientUnsubscribeResult> UnsubscribeAsync(MqttClientUnsubscribeOptions options, CancellationToken cancellationToken = default) => Inner.UnsubscribeAsync(options, cancellationToken);
        public void Dispose() => Inner.Dispose();
    }
}

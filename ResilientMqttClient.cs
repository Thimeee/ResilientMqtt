using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using ResilientMqtt.Buffering;
using ResilientMqtt.Internal;
using MQTTnet;
using MQTTnet.Protocol;

namespace ResilientMqtt
{
    internal sealed class ResilientMqttClient : IResilientMqttClient
    {
        private readonly IMqttClient _client;
        private readonly ResilientMqttOptions _options;
        private readonly IRetryStrategy _retry;
        private readonly IHealthCheck _healthCheck;
        private readonly IOfflineBuffer _buffer;
        private readonly ICircuitBreaker _breaker;
        private readonly ILogger<ResilientMqttClient> _logger;

        private readonly ConcurrentDictionary<string, SubscriptionEntry> _subscriptions = new();
        private readonly SemaphoreSlim _connectLock = new(1, 1);
        private readonly SemaphoreSlim _publishLock = new(1, 1);

        private CancellationTokenSource? _reconnectCts;
        private readonly object _ctsGate = new();          // guards all _reconnectCts access (#3)
        private MqttClientOptions? _mqttOptions;
        private int _isReconnecting;
        private int _isShuttingDown;
        private int _reconnectAttempt;
        private int _disposed;
        private int _isDraining;                 // #16 — single-flight guard for the buffer drain
        private long _downSinceTicks;            // #4 — when the current outage began (0 = up)
        private int _stillDownFired;             // #4 — StillDown raised once per outage
        private readonly CancellationTokenSource _lifetimeCts = new();  // #12 — cancelled on dispose; handed to message handlers
        private volatile ResilientMqttConnectionState _state = ResilientMqttConnectionState.Disconnected;  // #12 — visible across threads

        private static readonly JsonSerializerOptions JsonOpts = new()
        {
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };

        public bool IsConnected => _client?.IsConnected ?? false;
        public ResilientMqttConnectionState State => _state;
        public event EventHandler<ResilientMqttConnectionState>? StateChanged;
        public event Func<Task>? OnConnected;
        public event Func<Task>? OnDisconnected;
        public event Action<int, TimeSpan>? StillDown;

        public ResilientMqttClient(
            IMqttClient client,
            IOptions<ResilientMqttOptions> options,
            IRetryStrategy retry,
            IHealthCheck healthCheck,
            IOfflineBuffer buffer,
            ICircuitBreaker breaker,
            ILogger<ResilientMqttClient> logger)
        {
            _client = client;
            _options = options.Value;
            _retry = retry;
            _healthCheck = healthCheck;
            _buffer = buffer;
            _breaker = breaker;
            _logger = logger;

            _mqttOptions = BuildMqttOptions();

            _client.DisconnectedAsync += OnDisconnectedAsync;
            _client.ApplicationMessageReceivedAsync += OnMessageReceivedAsync;
        }

        // ════════════════════════════════════════════════════════════════
        private MqttClientOptions BuildMqttOptions()
        {
            var builder = new MqttClientOptionsBuilder()
                .WithClientId(_options.ClientId)
                .WithTcpServer(_options.Host, _options.Port)
                .WithCleanSession(_options.CleanSession)
                .WithKeepAlivePeriod(TimeSpan.FromSeconds(_options.KeepAliveSeconds));

            if (!string.IsNullOrEmpty(_options.Username))
                builder.WithCredentials(_options.Username, _options.Password);

            // Last Will & Testament (broker-side).
            // The broker stores this and publishes it ONLY on UNGRACEFUL disconnect
            // (process crash, network drop, TCP timeout). On a graceful DisconnectAsync
            // the broker suppresses it, and we publish GracefulShutdownPayload instead.
            if (_options.LastWill.Enabled)
            {
                builder
                    .WithWillTopic(BuildFullTopic(_options.LastWill.Topic))
                    .WithWillPayload(_options.LastWill.OfflinePayload)
                    .WithWillRetain(_options.LastWill.Retain)
                    .WithWillQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce);
            }

            // TLS
            if (_options.Tls.Enabled)
            {
                builder.WithTlsOptions(o =>
                {
                    switch (_options.Tls.Mode)
                    {
                        case "Normal":
                            // #11 — accepts ANY server certificate (MITM-open). Fine for dev/internal LAN,
                            // NOT for production. Warn loudly so an insecure prod config isn't silent.
                            _logger.LogWarning(
                                "[ResilientMqttClient] TLS Mode='Normal' accepts ANY broker certificate " +
                                "(no MITM protection). Use Mode='Hard' with a pinned thumbprint in production.");
                            o.WithCertificateValidationHandler(_ => true);
                            break;
                        case "Hard":
                            o.WithCertificateValidationHandler(ctx =>
                            {
                                if (ctx.Certificate is null) return false;
                                var actual = ctx.Certificate.GetCertHashString();
                                return actual.Equals(
                                    _options.Tls.TrustedThumbprint,
                                    StringComparison.OrdinalIgnoreCase);
                            });
                            break;
                    }
                });
            }

            return builder.Build();
        }

        // ════════════════════════════════════════════════════════════════
        // Bounds any MQTTnet call with a linked-CTS timeout so a half-open / stalled
        // operation can't hang forever while a lock or the reconnect latch is held.
        private static async Task<T> WithTimeout<T>(
            Func<CancellationToken, Task<T>> op, TimeSpan timeout, CancellationToken ct)
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(timeout);
            return await op(cts.Token).ConfigureAwait(false);
        }

        // ════════════════════════════════════════════════════════════════
        public Task<bool> ConnectAsync(CancellationToken ct = default)
            => ConnectAsync(ct, isReconnectLoop: false);

        // isReconnectLoop = true when called from TryReconnectAsync's own loop (so a failed
        // attempt doesn't re-trigger the loop — the loop handles its own retry). When false
        // (auto-start / user / publish-path), a failed connect schedules the reconnect loop (#13).
        private async Task<bool> ConnectAsync(CancellationToken ct, bool isReconnectLoop)
        {
            if (IsConnected) return true;
            if (Volatile.Read(ref _isShuttingDown) == 1) return false;

            await _connectLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                if (IsConnected) return true;

                SetState(ResilientMqttConnectionState.Connecting);
                _mqttOptions ??= BuildMqttOptions();

                // #1 — bound the connect (TCP + CONNECT/CONNACK handshake). A stalled handshake
                // throws OperationCanceledException after ConnectTimeoutSeconds instead of hanging
                // forever and stranding the reconnect latch.
                var connectTimeout = TimeSpan.FromSeconds(Math.Max(1, _options.Reconnect.ConnectTimeoutSeconds));
                var result = await WithTimeout(
                    t => _client.ConnectAsync(_mqttOptions, t), connectTimeout, ct).ConfigureAwait(false);

                if (result.ResultCode != MqttClientConnectResultCode.Success)
                {
                    _logger.LogWarning("[ResilientMqttClient] Connect rejected — ResultCode: {Code}", result.ResultCode);
                    SetState(ResilientMqttConnectionState.Disconnected);
                    return false;
                }

                Interlocked.Exchange(ref _reconnectAttempt, 0);
                Interlocked.Exchange(ref _downSinceTicks, 0);   // #4 — outage over
                Interlocked.Exchange(ref _stillDownFired, 0);
                _breaker.Reset();
                SetState(ResilientMqttConnectionState.Connected);

                _logger.LogInformation(
                    "[ResilientMqttClient] Connected — {Host}:{Port} as {ClientId}",
                    _options.Host, _options.Port, _options.ClientId);

                // 1. Resubscribe topics first
                await ResubscribeAllAsync(ct).ConfigureAwait(false);

                // 2. Publish ONLINE
                if (_options.LastWill.Enabled)
                {
                    await PublishRawAsync(
                        _options.LastWill.Topic,
                        _options.LastWill.OnlinePayload,
                        retain: _options.LastWill.Retain,
                        qos: ResilientMqttQos.AtLeastOnce,
                        ct: ct).ConfigureAwait(false);
                }

                // 3. Notify user handlers — they may register additional subscriptions
                //    or publish app-specific "ready" messages.
                //    Done BEFORE buffer drain so any user-added subscriptions are live
                //    when buffered messages start flowing.
                await RaiseOnConnectedAsync().ConfigureAwait(false);

                // 4. Drain offline buffer in background
                if (_options.OfflineBuffer.Enabled)
                    _ = Task.Run(() => DrainOfflineBufferAsync(CancellationToken.None));

                // 5. Start health monitor
                if (_options.HealthCheck.Enabled)
                    _healthCheck.Start(_client, () => Task.Run(TryReconnectAsync));

                return true;
            }
            catch (OperationCanceledException) { return false; }   // timeout OR shutdown cancel
            catch (Exception ex)
            {
                _logger.LogError(ex, "[ResilientMqttClient] ConnectAsync threw");
                SetState(ResilientMqttConnectionState.Disconnected);
                return false;
            }
            finally
            {
                _connectLock.Release();

                // #13 — if this attempt didn't connect and it wasn't the reconnect loop itself,
                // start the loop so a cold-start / one-off failure still recovers on its own
                // (previously nothing scheduled a retry unless a publish or a disconnect happened).
                if (!IsConnected
                    && !isReconnectLoop
                    && _options.Reconnect.Enabled
                    && Volatile.Read(ref _isShuttingDown) == 0)
                {
                    _ = Task.Run(TryReconnectAsync);
                }
            }
        }

        // ════════════════════════════════════════════════════════════════
        public Task PublishAsync<T>(
            string topic,
            T payload,
            bool retain = false,
            ResilientMqttQos qos = ResilientMqttQos.AtLeastOnce,
            bool useTopicPrefix = true,
            CancellationToken ct = default)
        {
            var json = payload is string s ? s : JsonSerializer.Serialize(payload, JsonOpts);
            return PublishRawAsync(topic, json, retain, qos, useTopicPrefix, ct);
        }

        public Task PublishRawAsync(
            string topic,
            string payload,
            bool retain = false,
            ResilientMqttQos qos = ResilientMqttQos.AtLeastOnce,
            bool useTopicPrefix = true,
            CancellationToken ct = default)
        {
            var bytes = System.Text.Encoding.UTF8.GetBytes(payload ?? string.Empty);
            return PublishBytesAsync(topic, bytes, retain, qos, useTopicPrefix, ct);
        }

        public async Task PublishBytesAsync(
            string topic,
            byte[] payload,
            bool retain = false,
            ResilientMqttQos qos = ResilientMqttQos.AtLeastOnce,
            bool useTopicPrefix = true,
            CancellationToken ct = default)
        {
            if (Volatile.Read(ref _disposed) == 1) return;

            var fullTopic = useTopicPrefix ? BuildFullTopic(topic) : NormalizeTopic(topic);

            var message = new MqttApplicationMessageBuilder()
                .WithTopic(fullTopic)
                .WithPayload(payload)
                .WithRetainFlag(retain)
                .WithQualityOfServiceLevel((MqttQualityOfServiceLevel)qos)
                .Build();

            if (!_breaker.TryAcquire())
            {
                _logger.LogWarning("[ResilientMqttClient] Publish blocked — circuit OPEN. Topic: {Topic}", message.Topic);
                _buffer.TryEnqueue(message);
                return;
            }

            if (!IsConnected)
            {
                _buffer.TryEnqueue(message);
                if (_options.Reconnect.Enabled)
                    _ = Task.Run(TryReconnectAsync);
                return;
            }

            try { await _publishLock.WaitAsync(ct).ConfigureAwait(false); }
            catch (ObjectDisposedException) { _buffer.TryEnqueue(message); return; }   // #17 — disposed mid-flight
            try
            {
                // #7 — bound the publish so a half-open socket can't hang forever holding _publishLock
                // (which would freeze every other publish + the buffer drain behind it).
                var pubTimeout = TimeSpan.FromSeconds(Math.Max(1, _options.PublishTimeoutSeconds));
                await WithTimeout(t => _client.PublishAsync(message, t), pubTimeout, ct).ConfigureAwait(false);
                _breaker.RecordSuccess();
                _healthCheck.NotifyActivity();
            }
            catch (Exception ex)
            {
                // A timed-out publish (OperationCanceledException) is treated exactly like a failed one.
                _logger.LogError(ex, "[ResilientMqttClient] Publish failed — buffering. Topic: {Topic}", message.Topic);
                _breaker.RecordFailure();
                _buffer.TryEnqueue(message);
            }
            finally
            {
                try { _publishLock.Release(); } catch (ObjectDisposedException) { }
            }
        }

        // ════════════════════════════════════════════════════════════════
        private async Task DrainOfflineBufferAsync(CancellationToken ct)
        {
            // #16 — single-flight: two quick reconnects would otherwise start two drains that both
            // TryPeek the same head (duplicate send) and then each TryDequeue — dropping the message
            // in between. Only one drain runs at a time.
            if (Interlocked.CompareExchange(ref _isDraining, 1, 0) != 0) return;
            try
            {
                if (_buffer.Count == 0 || !IsConnected) return;
                _logger.LogInformation("[ResilientMqttClient] Draining offline buffer — {Count}", _buffer.Count);

                while (_buffer.Count > 0 &&
                       IsConnected &&
                       Volatile.Read(ref _isShuttingDown) == 0 &&
                       !ct.IsCancellationRequested)
                {
                    if (!_buffer.TryPeek(out var msg) || msg is null) break;

                    try { await _publishLock.WaitAsync(ct).ConfigureAwait(false); }
                    catch (ObjectDisposedException) { break; }   // #17 — disposed mid-drain
                    try
                    {
                        // #7 — bound the drained publish too (same half-open hazard as the live path).
                        var pubTimeout = TimeSpan.FromSeconds(Math.Max(1, _options.PublishTimeoutSeconds));
                        await WithTimeout(t => _client.PublishAsync(msg, t), pubTimeout, ct).ConfigureAwait(false);
                        _buffer.TryDequeue(out _);
                        _healthCheck.NotifyActivity();
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "[ResilientMqttClient] Drain failed — stopping");
                        break;
                    }
                    finally { try { _publishLock.Release(); } catch (ObjectDisposedException) { } }

                    try { await Task.Delay(_options.OfflineBuffer.DrainPauseMs, ct).ConfigureAwait(false); }
                    catch (OperationCanceledException) { break; }
                }
            }
            finally { Interlocked.Exchange(ref _isDraining, 0); }
        }

        // ════════════════════════════════════════════════════════════════
        public async Task SubscribeAsync(
            string topicPattern,
            IResilientMqttMessageHandler handler,
            ResilientMqttQos qos = ResilientMqttQos.AtLeastOnce,
            bool useTopicPrefix = false,
            CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(topicPattern))
                throw new ArgumentException("Topic pattern required", nameof(topicPattern));
            if (handler is null) throw new ArgumentNullException(nameof(handler));

            // Resolve to final wire topic. We store this as the registry key so
            // ResubscribeAllAsync sends the SAME topic the broker already knows.
            var resolvedTopic = useTopicPrefix
                ? BuildFullTopic(topicPattern)
                : NormalizeTopic(topicPattern);

            _subscriptions[resolvedTopic] = new SubscriptionEntry(handler, qos);

            if (IsConnected)
            {
                await _client.SubscribeAsync(resolvedTopic, (MqttQualityOfServiceLevel)qos, ct)
                             .ConfigureAwait(false);
                _logger.LogInformation("[ResilientMqttClient] Subscribed — {Topic}", resolvedTopic);
            }
        }

        public Task SubscribeAsync(
            string topicPattern,
            Func<string, string, CancellationToken, Task> handler,
            ResilientMqttQos qos = ResilientMqttQos.AtLeastOnce,
            bool useTopicPrefix = false,
            CancellationToken ct = default)
            => SubscribeAsync(topicPattern, new DelegateHandler(handler), qos, useTopicPrefix, ct);

        public async Task UnsubscribeAsync(
            string topicPattern,
            bool useTopicPrefix = false,
            CancellationToken ct = default)
        {
            var resolvedTopic = useTopicPrefix
                ? BuildFullTopic(topicPattern)
                : NormalizeTopic(topicPattern);

            _subscriptions.TryRemove(resolvedTopic, out _);

            if (IsConnected)
            {
                await _client.UnsubscribeAsync(resolvedTopic, ct).ConfigureAwait(false);
                _logger.LogInformation("[ResilientMqttClient] Unsubscribed — {Topic}", resolvedTopic);
            }
        }

        private async Task ResubscribeAllAsync(CancellationToken ct)
        {
            if (_subscriptions.IsEmpty) return;
            _logger.LogInformation("[ResilientMqttClient] Resubscribing to {Count} topic(s)", _subscriptions.Count);
            // Keys are already resolved (post-prefix) topics — just send them.
            foreach (var (resolvedTopic, entry) in _subscriptions)
            {
                try
                {
                    await _client.SubscribeAsync(resolvedTopic, (MqttQualityOfServiceLevel)entry.Qos, ct)
                                 .ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "[ResilientMqttClient] Resubscribe failed — {Topic}", resolvedTopic);
                }
            }
        }

        // ════════════════════════════════════════════════════════════════
        private async Task OnMessageReceivedAsync(MqttApplicationMessageReceivedEventArgs e)
        {
            _healthCheck.NotifyActivity();

            var topic = e.ApplicationMessage.Topic;
            var payload = e.ApplicationMessage.ConvertPayloadToString();

            foreach (var (pattern, entry) in _subscriptions)
            {
                if (!IsTopicMatch(pattern, topic)) continue;
                try
                {
                    // #12 — hand handlers a real token (cancelled on dispose) instead of None,
                    // so inbound work can be cancelled at shutdown.
                    await entry.Handler.HandleAsync(topic, payload, _lifetimeCts.Token)
                                       .ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "[ResilientMqttClient] Handler threw — Pattern: {Pattern}, Topic: {Topic}",
                        pattern, topic);
                }
            }
        }

        private static bool IsTopicMatch(string pattern, string topic)
        {
            if (pattern == topic) return true;
            return MqttTopicFilterComparer.Compare(topic, pattern)
                   == MqttTopicFilterCompareResult.IsMatch;
        }

        // ════════════════════════════════════════════════════════════════
        private Task OnDisconnectedAsync(MqttClientDisconnectedEventArgs e)
        {
            if (Volatile.Read(ref _isShuttingDown) == 1) return Task.CompletedTask;

            _logger.LogWarning("[ResilientMqttClient] Disconnected — Reason: {Reason}", e.Reason);
            SetState(ResilientMqttConnectionState.Reconnecting);

            // Notify user handlers (fire-and-forget — don't block MQTTnet's event loop)
            _ = Task.Run(RaiseOnDisconnectedAsync);

            if (_options.Reconnect.Enabled)
                _ = Task.Run(TryReconnectAsync);

            return Task.CompletedTask;
        }

        private async Task TryReconnectAsync()
        {
            if (!_options.Reconnect.Enabled) return;
            if (Volatile.Read(ref _isShuttingDown) == 1) return;
            if (Interlocked.CompareExchange(ref _isReconnecting, 1, 0) != 0) return;

            try
            {
                var token = ResetReconnectCts();   // #3 — thread-safe swap

                // #4 — record when this outage started (only if not already tracking one).
                Interlocked.CompareExchange(ref _downSinceTicks, DateTime.UtcNow.Ticks, 0);

                while (!IsConnected && Volatile.Read(ref _isShuttingDown) == 0 && !token.IsCancellationRequested)
                {
                    var attempt = Interlocked.Increment(ref _reconnectAttempt);
                    var delay = _retry.GetNextDelay(attempt);

                    _logger.LogInformation("[ResilientMqttClient] Reconnect #{Attempt} in {Seconds}s",
                        attempt, delay.TotalSeconds);

                    RaiseStillDownIfDue(attempt);   // #4

                    try { await Task.Delay(delay, token).ConfigureAwait(false); }
                    catch (OperationCanceledException) { break; }

                    if (await ConnectAsync(token, isReconnectLoop: true).ConfigureAwait(false)) break;
                }
            }
            finally { Interlocked.Exchange(ref _isReconnecting, 0); }
        }

        // #3 — all _reconnectCts access is serialized through these two helpers.
        // Cancel+dispose the old source and hand back a fresh token, atomically.
        private CancellationToken ResetReconnectCts()
        {
            lock (_ctsGate)
            {
                try { _reconnectCts?.Cancel(); } catch (ObjectDisposedException) { }
                _reconnectCts?.Dispose();
                _reconnectCts = new CancellationTokenSource();
                return _reconnectCts.Token;
            }
        }

        private void CancelReconnectCts()
        {
            lock (_ctsGate)
            {
                try { _reconnectCts?.Cancel(); } catch (ObjectDisposedException) { }
            }
        }

        // #4 — fire StillDown once per outage after StillDownAfterSeconds so a host can alert on a
        // client that's still offline. Invoked on a background task so a slow handler can't stall
        // the reconnect loop; exceptions are isolated.
        private void RaiseStillDownIfDue(int attempt)
        {
            var threshold = _options.Reconnect.StillDownAfterSeconds;
            if (threshold <= 0) return;
            if (Volatile.Read(ref _stillDownFired) == 1) return;

            var sinceTicks = Interlocked.Read(ref _downSinceTicks);
            if (sinceTicks == 0) return;

            var down = DateTime.UtcNow - new DateTime(sinceTicks, DateTimeKind.Utc);
            if (down < TimeSpan.FromSeconds(threshold)) return;
            if (Interlocked.Exchange(ref _stillDownFired, 1) == 1) return;   // fire exactly once

            var handler = StillDown;
            if (handler is null) return;
            _ = Task.Run(() =>
            {
                try { handler(attempt, down); }
                catch (Exception ex) { _logger.LogError(ex, "[ResilientMqttClient] StillDown handler threw"); }
            });
        }

        // ════════════════════════════════════════════════════════════════
        public async Task DisconnectAsync(CancellationToken ct = default)
        {
            Interlocked.Exchange(ref _isShuttingDown, 1);
            SetState(ResilientMqttConnectionState.ShuttingDown);
            CancelReconnectCts();
            _healthCheck.Stop();   // #14 — stop the timer so it doesn't keep ticking/logging after a graceful disconnect

            if (!IsConnected) return;

            try
            {
                if (_options.LastWill.Enabled)
                {
                    // Graceful path: prefer GracefulShutdownPayload if set, else
                    // fall back to OfflinePayload for backward compatibility.
                    // (The broker's stored Will — OfflinePayload — is suppressed
                    //  because we send a clean DISCONNECT packet below.)
                    var gracefulPayload =
                        _options.LastWill.GracefulShutdownPayload
                        ?? _options.LastWill.OfflinePayload;

                    // #7 — isolate the OFFLINE publish so a failure/timeout here can NEVER skip the
                    // clean DISCONNECT below (a skipped DISCONNECT would make the broker fire the LWT
                    // as if we crashed). PublishBytesAsync already swallows + buffers internally; the
                    // extra guard covers the pre-publish lock-wait cancel edge.
                    try
                    {
                        await PublishRawAsync(
                            _options.LastWill.Topic,
                            gracefulPayload,
                            retain: _options.LastWill.Retain,
                            qos: ResilientMqttQos.AtLeastOnce,
                            ct: ct).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "[ResilientMqttClient] Graceful OFFLINE publish failed — disconnecting anyway");
                    }
                }

                await _client.DisconnectAsync(
                    new MqttClientDisconnectOptionsBuilder().Build(),
                    ct).ConfigureAwait(false);

                SetState(ResilientMqttConnectionState.Disconnected);
                _logger.LogInformation("[ResilientMqttClient] Disconnected gracefully");

                // Notify user handlers — fire-and-forget so a slow user handler
                // can't block app shutdown beyond reason.
                _ = Task.Run(RaiseOnDisconnectedAsync);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[ResilientMqttClient] Graceful disconnect failed");
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 1) return;
            Interlocked.Exchange(ref _isShuttingDown, 1);
            CancelReconnectCts();
            _healthCheck.Stop();
            try { _lifetimeCts.Cancel(); } catch (ObjectDisposedException) { }   // #12 — signal handlers to stop

            try { await DisconnectAsync().ConfigureAwait(false); }
            catch { /* logged */ }

            _client.DisconnectedAsync -= OnDisconnectedAsync;
            _client.ApplicationMessageReceivedAsync -= OnMessageReceivedAsync;
            _client.Dispose();

            // #17 — a background publish/drain/reconnect may still be mid-flight; swallow the
            // ObjectDisposedException that a disposed semaphore/CTS would throw during shutdown.
            try { _publishLock.Dispose(); } catch (ObjectDisposedException) { }
            try { _connectLock.Dispose(); } catch (ObjectDisposedException) { }
            lock (_ctsGate) { try { _reconnectCts?.Dispose(); } catch (ObjectDisposedException) { } }
            try { _lifetimeCts.Dispose(); } catch (ObjectDisposedException) { }
            _healthCheck.Dispose();
        }

        // ════════════════════════════════════════════════════════════════
        // Topic resolution helpers.
        // BuildFullTopic   → prepends TopicPrefix (used when useTopicPrefix = true)
        // NormalizeTopic   → just trims leading slash (used when useTopicPrefix = false)
        // ════════════════════════════════════════════════════════════════
        private string BuildFullTopic(string subTopic)
        {
            var trimmed = NormalizeTopic(subTopic);
            return string.IsNullOrEmpty(_options.TopicPrefix)
                ? trimmed
                : $"{_options.TopicPrefix.TrimEnd('/')}/{trimmed}";
        }

        private static string NormalizeTopic(string topic)
            => topic?.TrimStart('/') ?? string.Empty;

        private void SetState(ResilientMqttConnectionState newState)
        {
            if (_state == newState) return;
            _state = newState;
            try { StateChanged?.Invoke(this, newState); } catch { /* user code */ }
        }

        // ════════════════════════════════════════════════════════════════
        // Safe event raisers.
        // - Snapshot the delegate list (multicast-safe — avoids races with += / -=).
        // - Await each handler sequentially so user code can rely on ordering.
        // - Isolate exceptions per-handler so one bad subscriber can't break others.
        // ════════════════════════════════════════════════════════════════
        private async Task RaiseOnConnectedAsync()
        {
            var handler = OnConnected;
            if (handler is null) return;

            foreach (var single in handler.GetInvocationList())
            {
                try
                {
                    var task = ((Func<Task>)single).Invoke();
                    if (task is not null) await task.ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "[ResilientMqttClient] OnConnected handler threw");
                }
            }
        }

        private async Task RaiseOnDisconnectedAsync()
        {
            var handler = OnDisconnected;
            if (handler is null) return;

            foreach (var single in handler.GetInvocationList())
            {
                try
                {
                    var task = ((Func<Task>)single).Invoke();
                    if (task is not null) await task.ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "[ResilientMqttClient] OnDisconnected handler threw");
                }
            }
        }

        private readonly record struct SubscriptionEntry(IResilientMqttMessageHandler Handler, ResilientMqttQos Qos);

        private sealed class DelegateHandler : IResilientMqttMessageHandler
        {
            private readonly Func<string, string, CancellationToken, Task> _fn;
            public DelegateHandler(Func<string, string, CancellationToken, Task> fn) => _fn = fn;
            public Task HandleAsync(string topic, string payload, CancellationToken ct) => _fn(topic, payload, ct);
        }
    }
}
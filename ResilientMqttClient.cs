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
        private MqttClientOptions? _mqttOptions;
        private int _isReconnecting;
        private int _isShuttingDown;
        private int _reconnectAttempt;
        private int _disposed;
        private ResilientMqttConnectionState _state = ResilientMqttConnectionState.Disconnected;

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

            // Last Will & Testament (broker-side)
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
        public async Task<bool> ConnectAsync(CancellationToken ct = default)
        {
            if (IsConnected) return true;
            if (Volatile.Read(ref _isShuttingDown) == 1) return false;

            await _connectLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                if (IsConnected) return true;

                SetState(ResilientMqttConnectionState.Connecting);
                _mqttOptions ??= BuildMqttOptions();
                var result = await _client.ConnectAsync(_mqttOptions, ct).ConfigureAwait(false);

                if (result.ResultCode != MqttClientConnectResultCode.Success)
                {
                    _logger.LogWarning("[ResilientMqttClient] Connect rejected — ResultCode: {Code}", result.ResultCode);
                    SetState(ResilientMqttConnectionState.Disconnected);
                    return false;
                }

                Interlocked.Exchange(ref _reconnectAttempt, 0);
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
            catch (OperationCanceledException) { return false; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[ResilientMqttClient] ConnectAsync threw");
                SetState(ResilientMqttConnectionState.Disconnected);
                return false;
            }
            finally
            {
                _connectLock.Release();
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

            await _publishLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                await _client.PublishAsync(message, ct).ConfigureAwait(false);
                _breaker.RecordSuccess();
                _healthCheck.NotifyActivity();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[ResilientMqttClient] Publish failed — buffering. Topic: {Topic}", message.Topic);
                _breaker.RecordFailure();
                _buffer.TryEnqueue(message);
            }
            finally
            {
                _publishLock.Release();
            }
        }

        // ════════════════════════════════════════════════════════════════
        private async Task DrainOfflineBufferAsync(CancellationToken ct)
        {
            if (_buffer.Count == 0 || !IsConnected) return;
            _logger.LogInformation("[ResilientMqttClient] Draining offline buffer — {Count}", _buffer.Count);

            while (_buffer.Count > 0 &&
                   IsConnected &&
                   Volatile.Read(ref _isShuttingDown) == 0 &&
                   !ct.IsCancellationRequested)
            {
                if (!_buffer.TryPeek(out var msg) || msg is null) break;

                await _publishLock.WaitAsync(ct).ConfigureAwait(false);
                try
                {
                    await _client.PublishAsync(msg, ct).ConfigureAwait(false);
                    _buffer.TryDequeue(out _);
                    _healthCheck.NotifyActivity();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "[ResilientMqttClient] Drain failed — stopping");
                    break;
                }
                finally { _publishLock.Release(); }

                try { await Task.Delay(_options.OfflineBuffer.DrainPauseMs, ct).ConfigureAwait(false); }
                catch (OperationCanceledException) { break; }
            }
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
                    await entry.Handler.HandleAsync(topic, payload, CancellationToken.None)
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
                _reconnectCts?.Cancel();
                _reconnectCts = new CancellationTokenSource();
                var token = _reconnectCts.Token;

                while (!IsConnected && Volatile.Read(ref _isShuttingDown) == 0 && !token.IsCancellationRequested)
                {
                    var attempt = Interlocked.Increment(ref _reconnectAttempt);
                    var delay = _retry.GetNextDelay(attempt);

                    _logger.LogInformation("[ResilientMqttClient] Reconnect #{Attempt} in {Seconds}s",
                        attempt, delay.TotalSeconds);

                    try { await Task.Delay(delay, token).ConfigureAwait(false); }
                    catch (OperationCanceledException) { break; }

                    if (await ConnectAsync(token).ConfigureAwait(false)) break;
                }
            }
            finally { Interlocked.Exchange(ref _isReconnecting, 0); }
        }

        // ════════════════════════════════════════════════════════════════
        public async Task DisconnectAsync(CancellationToken ct = default)
        {
            Interlocked.Exchange(ref _isShuttingDown, 1);
            SetState(ResilientMqttConnectionState.ShuttingDown);
            _reconnectCts?.Cancel();

            if (!IsConnected) return;

            try
            {
                if (_options.LastWill.Enabled)
                {
                    await PublishRawAsync(
                        _options.LastWill.Topic,
                        _options.LastWill.OfflinePayload,
                        retain: _options.LastWill.Retain,
                        qos: ResilientMqttQos.AtLeastOnce,
                        ct: ct).ConfigureAwait(false);
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
            _reconnectCts?.Cancel();
            _healthCheck.Stop();

            try { await DisconnectAsync().ConfigureAwait(false); }
            catch { /* logged */ }

            _client.DisconnectedAsync -= OnDisconnectedAsync;
            _client.ApplicationMessageReceivedAsync -= OnMessageReceivedAsync;
            _client.Dispose();

            _publishLock.Dispose();
            _connectLock.Dispose();
            _reconnectCts?.Dispose();
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
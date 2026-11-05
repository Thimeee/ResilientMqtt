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
using MQTTnet.Formatter;
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

        // Protocol / session (#19) and dispatch (#35) — resolved once from options.
        private readonly MqttProtocolVersion _protocol;
        private readonly bool _isV5;
        private readonly uint _sessionExpirySeconds;
        private readonly bool _concurrentDispatch;
        private readonly SemaphoreSlim? _dispatchGate;        // Concurrent: handlers running at once (MaxConcurrency)
        private readonly SemaphoreSlim? _dispatchCapacity;    // Concurrent: running + waiting (MaxConcurrency + MaxPending) — R9
        private readonly int _dispatchCapacityTotal;
        private int _connectionGeneration;                    // R4 — bumps on every connect AND every drop

        private CancellationTokenSource? _reconnectCts;
        private readonly object _ctsGate = new();          // guards all _reconnectCts access (#3)
        private MqttClientOptions? _mqttOptions;
        private int _isReconnecting;
        private int _isShuttingDown;
        private int _reconnectAttempt;
        private int _disposed;                   // DisposeAsync entered (idempotency)
        private int _resourcesDisposed;          // locks/client actually disposed (#42 — graceful OFFLINE must still go out)
        private int _isDraining;                 // #16 — single-flight guard for the buffer drain
        private long _downSinceTicks;            // #4 — when the current outage began (0 = up)
        private int _stillDownFired;             // #4 — StillDown raised once per outage
        private readonly CancellationTokenSource _lifetimeCts = new();  // #12 — cancelled on dispose; handed to message handlers
        private readonly CancellationTokenSource _shutdownCts = new();  // cancelled as soon as shutdown starts (unblocks waits)
        private readonly object _healthGate = new();                    // R3 — health Start/Stop vs. shutdown, atomically
        private volatile ResilientMqttConnectionState _state = ResilientMqttConnectionState.Disconnected;  // #12 — visible across threads

        // Failure visibility + counters (#20, #23, #24).
        private volatile ResilientMqttFailure? _lastFailure;
        private ResilientMqttFailure? _forcedResetFailure;  // set before a zombie reset so the drop reads "Timeout" (#25)
        private long _connectedSinceTicks;
        private long _totalConnects;
        private long _deniedPublishes;
        private volatile bool _sessionPresent;
        private readonly ConcurrentDictionary<string, string> _rejectedSubscriptions = new();
        private readonly ConcurrentDictionary<string, ResilientMqttQos> _activeSubscriptions = new();  // per connection (#27)
        private readonly ConcurrentDictionary<string, long> _deniedLogTicks = new();
        private volatile TaskCompletionSource<bool> _connectedSignal = NewSignal();
        private volatile bool _ready;                 // connected AND subscriptions restored + OnConnected done (#46)
        private readonly object _stateGate = new();   // makes state transitions atomic (#45)

        // #43 — unsubscribes made while offline, to apply to a RESUMED broker session on the next connect.
        private readonly ConcurrentDictionary<string, byte> _pendingUnsubscribes = new();

        // #44 — while a connection is being set up (CONNACK → end of OnConnected), messages that match no
        // subscription YET are held instead of dropped: a resumed session delivers its queue right after the
        // CONNACK, often before the app has (re)subscribed in OnConnected. Replayed once OnConnected is done.
        // While replaying, EVERY new message is queued behind the held ones so arrival order is kept (a command
        // queued while offline must not be overtaken by a newer one).
        internal const int DefaultHeldMessageLimit = 1000;
        private readonly int _heldMessageLimit;
        private readonly object _holdGate = new();
        private bool _holding;     // set-up in progress, or its held queue still replaying (guarded by _holdGate)
        private bool _replaying;   // the held queue is being replayed (guarded by _holdGate)
        private int _holdEpoch;    // bumps at every connect attempt; held items and their acks are only valid in their epoch
        private readonly Queue<HeldMessage> _heldMessages = new();
        // A full hold queue applies back-pressure (the next message waits) — it never drops. Held QoS1/2 messages are
        // acknowledged only after they reached a handler, in arrival order (R1); anything set aside stays UNACKNOWLEDGED
        // so the broker redelivers it on the next session.
        private readonly SemaphoreSlim _heldSlots;
        private int _dispatchBackPressureLogged;
        internal const int MaxDeniedTopicsTracked = 1000;
        internal int DeniedTopicsTracked => _deniedLogTicks.Count;

        // #68 / #69 — one line per window for each repeating publish problem. Settable so tests don't wait a minute.
        internal TimeSpan PublishLogWindow { get; set; } = TimeSpan.FromMinutes(1);
        private readonly LogThrottle _publishFailedLog = new();
        private readonly LogThrottle _publishBlockedLog = new();

        private static readonly JsonSerializerOptions JsonOpts = new()
        {
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };

        public bool IsConnected => _client?.IsConnected ?? false;
        public ResilientMqttConnectionState State => _state;
        public ResilientMqttFailure? LastFailure => _lastFailure;
        public event EventHandler<ResilientMqttConnectionState>? StateChanged;
        public event Func<Task>? OnConnected;
        public event Func<Task>? OnDisconnected;
        public event Action<int, TimeSpan>? StillDown;
        public event Func<ResilientMqttConnectionLostArgs, Task>? ConnectionLost;
        public event Func<ResilientMqttConnectionRestoredArgs, Task>? ConnectionRestored;

        public ResilientMqttClient(
            IMqttClient client,
            IOptions<ResilientMqttOptions> options,
            IRetryStrategy retry,
            IHealthCheck healthCheck,
            IOfflineBuffer buffer,
            ICircuitBreaker breaker,
            ILogger<ResilientMqttClient> logger,
            int heldMessageLimit = DefaultHeldMessageLimit)   // tests use a small limit to reach the back-pressure path
        {
            _heldMessageLimit = Math.Max(1, heldMessageLimit);
            _heldSlots = new SemaphoreSlim(_heldMessageLimit, _heldMessageLimit);
            _client = client;
            _options = options.Value;
            _retry = retry;
            _healthCheck = healthCheck;
            _buffer = buffer;
            _breaker = breaker;
            _logger = logger;

            _protocol = ParseProtocolVersion(_options.ProtocolVersion);
            _isV5 = _protocol == MqttProtocolVersion.V500;
            _sessionExpirySeconds = EffectiveSessionExpirySeconds(_options);
            _concurrentDispatch = ParseDispatchMode(_options.Dispatch.Mode);
            if (_concurrentDispatch)
            {
                var concurrency = Math.Max(1, _options.Dispatch.MaxConcurrency);
                _dispatchCapacityTotal = concurrency + Math.Max(0, _options.Dispatch.MaxPending);
                _dispatchGate = new SemaphoreSlim(concurrency);
                _dispatchCapacity = new SemaphoreSlim(_dispatchCapacityTotal);
            }

            _mqttOptions = BuildMqttOptions();
            LogSessionMode();

            _client.DisconnectedAsync += OnDisconnectedAsync;
            _client.ApplicationMessageReceivedAsync += OnMessageReceivedAsync;
        }

        // ════════════════════════════════════════════════════════════════
        // Options → MQTTnet
        // ════════════════════════════════════════════════════════════════

        internal static MqttProtocolVersion ParseProtocolVersion(string? value)
            => (value ?? "V500").Trim().ToUpperInvariant() switch
            {
                "V500" or "V5" or "5" or "5.0" or "MQTT5" => MqttProtocolVersion.V500,
                "V311" or "3.1.1" or "4" or "MQTT311" => MqttProtocolVersion.V311,
                _ => throw new InvalidOperationException(
                    $"ResilientMqtt: unknown ProtocolVersion '{value}'. Use \"V500\" (MQTT 5.0) or \"V311\" (MQTT 3.1.1)."),
            };

        internal static bool ParseDispatchMode(string? value)
            => (value ?? "Inline").Trim().ToUpperInvariant() switch
            {
                "INLINE" => false,
                "CONCURRENT" => true,
                _ => throw new InvalidOperationException(
                    $"ResilientMqtt: unknown Dispatch.Mode '{value}'. Use \"Inline\" or \"Concurrent\"."),
            };

        // #19 — MQTTnet 5 defaults to MQTT 5 with SessionExpiryInterval = 0, which silently turns
        // CleanSession=false into "no session". Resolve the expiry explicitly so the option means what it says.
        internal static uint EffectiveSessionExpirySeconds(ResilientMqttOptions o)
        {
            if (o.SessionExpirySeconds is { } explicitSeconds) return (uint)Math.Max(0, explicitSeconds);
            return o.CleanSession ? 0u : (uint)ResilientMqttOptions.DefaultPersistentSessionSeconds;
        }

        // R7 — on MQTT 5, CleanStart (CleanSession=true) discards the session at EVERY connect, so messages queued while
        // offline never survive a reconnect even with an expiry > 0. Only CleanSession=false + expiry > 0 is persistent.
        private bool PersistentSession => !_options.CleanSession && (!_isV5 || _sessionExpirySeconds > 0);

        private MqttClientOptions BuildMqttOptions()
        {
            var builder = new MqttClientOptionsBuilder()
                .WithClientId(_options.ClientId)
                .WithTcpServer(_options.Host, _options.Port)
                .WithProtocolVersion(_protocol)
                .WithCleanSession(_options.CleanSession)
                .WithKeepAlivePeriod(TimeSpan.FromSeconds(_options.KeepAliveSeconds));

            if (_isV5)
                builder.WithSessionExpiryInterval(_sessionExpirySeconds);

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

            // TLS (#37 — server-certificate modes None/Normal/Hard/Ca + optional mutual-TLS client certificate)
            if (_options.Tls.Enabled)
                builder.WithTlsOptions(o => TlsConfigurator.Configure(o.UseTls(true), _options.Tls, _logger));

            return builder.Build();
        }

        private void LogSessionMode()
        {
            var proto = _isV5 ? "MQTT 5.0" : "MQTT 3.1.1";
            string mode;
            if (_isV5)
                mode = PersistentSession
                    ? $"persistent session ({_sessionExpirySeconds} s after disconnect) — messages for this client are queued while it is offline"
                    : _options.CleanSession
                        ? "clean start — nothing queued while offline survives a reconnect"
                        : "no session after disconnect — nothing is queued while offline";
            else
                mode = _options.CleanSession
                    ? "clean session — nothing is queued while offline"
                    : "persistent session (expiry decided by the broker) — messages are queued while offline";

            _logger.LogInformation(ResilientMqttLogEvents.SessionMode,
                "[ResilientMqttClient] {Protocol} · {Mode} · client id {ClientId}", proto, mode, _options.ClientId);

            if (_isV5 && !_options.CleanSession && _options.SessionExpirySeconds == 0)
                _logger.LogWarning(ResilientMqttLogEvents.SessionMode,
                    "[ResilientMqttClient] CleanSession=false with SessionExpirySeconds=0 — on MQTT 5 the session ends with the " +
                    "connection, so CleanSession=false has no effect. Set SessionExpirySeconds > 0 or CleanSession=true.");

            if (_isV5 && _options.CleanSession && _sessionExpirySeconds > 0)
                _logger.LogWarning(ResilientMqttLogEvents.SessionMode,
                    "[ResilientMqttClient] CleanSession=true with SessionExpirySeconds={Seconds} — the broker keeps the session after a " +
                    "disconnect but DISCARDS it at the next connect (clean start), so offline messages are still lost. For offline " +
                    "queueing use CleanSession=false.", _sessionExpirySeconds);

            if (!_isV5 && _options.SessionExpirySeconds is > 0)
                _logger.LogWarning(ResilientMqttLogEvents.SessionSettingIgnored,
                    "[ResilientMqttClient] SessionExpirySeconds is ignored on MQTT 3.1.1 (the broker decides how long sessions live).");

            if (PersistentSession && _options.ClientId.StartsWith(ResilientMqttOptions.AutoClientIdPrefix, StringComparison.Ordinal))
                _logger.LogWarning(ResilientMqttLogEvents.SessionWithRandomClientId,
                    "[ResilientMqttClient] A persistent session is used with the auto-generated ClientId, which changes on every " +
                    "start — each restart leaves an abandoned session (and its queued messages) on the broker. Set a stable ClientId.");
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

        private static async Task WithTimeout(Func<CancellationToken, Task> op, TimeSpan timeout, CancellationToken ct)
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(timeout);
            await op(cts.Token).ConfigureAwait(false);
        }

        private TimeSpan ConnectTimeout => TimeSpan.FromSeconds(Math.Max(1, _options.Reconnect.ConnectTimeoutSeconds));
        private TimeSpan PublishTimeout => TimeSpan.FromSeconds(Math.Max(1, _options.PublishTimeoutSeconds));
        private bool ShuttingDown => Volatile.Read(ref _isShuttingDown) == 1;
        private static DateTime Now => DateTime.UtcNow;
        private static TaskCompletionSource<bool> NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

        // ════════════════════════════════════════════════════════════════
        // CONNECT
        // ════════════════════════════════════════════════════════════════
        public Task<bool> ConnectAsync(CancellationToken ct = default)
            => ConnectAsync(ct, isReconnectLoop: false, attempt: 0);

        // isReconnectLoop = true when called from TryReconnectAsync's own loop (so a failed
        // attempt doesn't re-trigger the loop — the loop handles its own retry). When false
        // (auto-start / user / publish-path), a failed connect schedules the reconnect loop (#13).
        private async Task<bool> ConnectAsync(CancellationToken ct, bool isReconnectLoop, int attempt)
        {
            if (IsConnected) return true;
            if (ShuttingDown) return false;

            try { await _connectLock.WaitAsync(ct).ConfigureAwait(false); }
            catch (OperationCanceledException) { return false; }
            catch (ObjectDisposedException) { return false; }

            if (ShuttingDown)   // R3 — DisconnectAsync may have run while we waited for the lock
            {
                try { _connectLock.Release(); } catch (ObjectDisposedException) { }
                return false;
            }

            var connectTimeout = ConnectTimeout;
            // #1 — bound the connect (TCP + CONNECT/CONNACK handshake). Our own source, so we can tell OUR timeout
            // apart from a caller/shutdown cancel no matter how MQTTnet wraps the cancellation (#22).
            // #67 — linked to the shutdown token too: DisconnectAsync / Dispose abort an in-flight connect at once
            // instead of waiting for ConnectTimeout (the catch below treats it as a cancel, not a broker failure).
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct, _shutdownCts.Token);
            var connectReturned = false;   // #47 — after the CONNACK, a cancellation is not "the connect timed out"
            var holdEpoch = 0;
            try
            {
                if (IsConnected) return true;

                _ready = false;
                SetState(ResilientMqttConnectionState.Connecting);
                _mqttOptions ??= BuildMqttOptions();
                lock (_holdGate)   // #44 — this attempt owns the hold queue. Anything left from an older connection is set
                {                  // aside UNACKNOWLEDGED (its packet ids mean nothing on a new connection) → redelivered.
                    holdEpoch = ++_holdEpoch;
                    DiscardHeldLocked();
                    _holding = true;
                    _replaying = false;
                }

                timeoutCts.CancelAfter(connectTimeout);
                var result = await _client.ConnectAsync(_mqttOptions, timeoutCts.Token).ConfigureAwait(false);
                connectReturned = true;
                timeoutCts.CancelAfter(Timeout.InfiniteTimeSpan);   // connected: post-connect steps use their own bounds

                if (result.ResultCode != MqttClientConnectResultCode.Success)
                {
                    // MQTTnet 5 RETURNS a refused CONNACK (wrong credentials, banned, server busy…) — no exception.
                    var failure = FailureClassifier.FromConnectResult(result.ResultCode, result.ReasonString, Now);
                    RecordFailure(failure);
                    LogConnectFailure(failure, attempt, null);
                    SetState(StateAfterFailedConnect());
                    return false;
                }

                // R2 — the set-up after the CONNACK must not be cut in half by the CALLER's token (that left a
                // connected-but-uninitialised client). Each step has its own timeout; only dispose cancels it.
                // #49 — true only when the set-up really completed (not dropped / shutting down half-way).
                return await OnConnectSucceededAsync(result, holdEpoch, _lifetimeCts.Token).ConfigureAwait(false);
            }
            catch (Exception) when (!connectReturned && timeoutCts.IsCancellationRequested && !ct.IsCancellationRequested && !ShuttingDown && !IsConnected)
            {
                // #22 — our ConnectTimeout fired (MQTTnet may surface it as OperationCanceledException OR wrapped in a
                // communication exception): log it (it used to be silent) and leave a truthful state.
                var failure = FailureClassifier.ConnectTimeout(connectTimeout, Now);
                RecordFailure(failure);
                LogConnectFailure(failure, attempt, null);
                SetState(StateAfterFailedConnect());
                return false;
            }
            catch (Exception ex) when (ex is OperationCanceledException || ct.IsCancellationRequested || ShuttingDown)
            {
                // Shutdown or the caller cancelled — not a broker problem, even when MQTTnet wrapped the cancellation in
                // a communication exception (#50: that used to be recorded as a failure, or even as a TLS failure).
                if (!ShuttingDown && !IsConnected) SetState(StateAfterFailedConnect());
                return IsConnected && _ready;
            }
            catch (Exception ex)
            {
                if (IsConnected)
                {
                    // Connected, but a post-connect step threw — the connection itself is fine.
                    _logger.LogError(ResilientMqttLogEvents.ConnectFailed, ex, "[ResilientMqttClient] Post-connect step threw");
                    return true;
                }

                var failure = FailureClassifier.FromException(ex, _options.Tls.Enabled, duringConnect: true, Now);
                RecordFailure(failure);
                LogConnectFailure(failure, attempt, ex);
                SetState(StateAfterFailedConnect());
                return false;
            }
            finally
            {
                if (!IsConnected) ReleaseHeldMessages(holdEpoch, replay: false);   // #44 — failed attempt: nothing to replay
                try { _connectLock.Release(); } catch (ObjectDisposedException) { }

                // #13 — if this attempt didn't connect and it wasn't the reconnect loop itself,
                // start the loop so a cold-start / one-off failure still recovers on its own.
                if (!IsConnected && !isReconnectLoop)
                    KickReconnect();
            }
        }

        private async Task<bool> OnConnectSucceededAsync(MqttClientConnectResult result, int holdEpoch, CancellationToken ct)
        {
            var now = Now;
            var generation = Interlocked.Increment(ref _connectionGeneration);    // R4
            var outageStartTicks = Interlocked.Exchange(ref _downSinceTicks, 0);   // #4 — outage over
            var attempts = Interlocked.Exchange(ref _reconnectAttempt, 0);
            var outageFailure = _lastFailure;
            _lastFailure = null;
            Volatile.Write(ref _forcedResetFailure, null);   // a zombie reset that never produced a drop must not mislabel the next one
            Interlocked.Exchange(ref _stillDownFired, 0);
            Interlocked.Exchange(ref _connectedSinceTicks, now.Ticks);
            Interlocked.Increment(ref _totalConnects);
            _sessionPresent = result.IsSessionPresent;
            _activeSubscriptions.Clear();
            _breaker.Reset();
            SetState(ResilientMqttConnectionState.Connected);

            _logger.LogInformation(ResilientMqttLogEvents.Connected,
                "[ResilientMqttClient] Connected — {Host}:{Port} as {ClientId}{Session}",
                _options.Host, _options.Port, _options.ClientId, result.IsSessionPresent ? " (session resumed)" : string.Empty);

            ResilientMqttConnectionRestoredArgs? restored = null;
            if (outageStartTicks != 0)
            {
                var downtime = now - new DateTime(outageStartTicks, DateTimeKind.Utc);
                restored = new ResilientMqttConnectionRestoredArgs(downtime, attempts, result.IsSessionPresent, outageFailure);
                _logger.LogInformation(ResilientMqttLogEvents.ConnectionRestored,
                    "[ResilientMqttClient] Connection restored after {Downtime} ({Attempts} attempt(s); last problem: {Kind})",
                    FormatDuration(downtime), attempts, outageFailure?.Kind ?? ResilientMqttFailureKind.None);
            }

            var heldReleased = false;
            try
            {
            // 0. #43 — apply unsubscribes made while offline to a resumed session (a clean session has nothing to undo)
            await ApplyPendingUnsubscribesAsync(result.IsSessionPresent, ct).ConfigureAwait(false);

            // 1. Restore subscriptions first (one SUBSCRIBE per filter; refusals are logged + listed — #23, #27)
            await ResubscribeAllAsync(ct).ConfigureAwait(false);

            // R3 — a DisconnectAsync that arrived during set-up wins: no ONLINE after its OFFLINE, no health timer restart.
            if (ShuttingDown || !StillCurrent(generation)) return false;

            // 2. Publish ONLINE (bypasses the breaker; buffered if it can't go now)
            if (_options.LastWill.Enabled)
            {
                await PublishCoreAsync(
                    BuildMessage(BuildFullTopic(_options.LastWill.Topic),
                        Encoding.UTF8.GetBytes(_options.LastWill.OnlinePayload ?? string.Empty),
                        _options.LastWill.Retain, ResilientMqttQos.AtLeastOnce),
                    bypassBreaker: true, allowBuffer: true, ct, skipIfShuttingDown: true).ConfigureAwait(false);
            }

            // 3. Notify user handlers — they may register additional subscriptions
            //    or publish app-specific "ready" messages.
            //    Done BEFORE buffer drain so any user-added subscriptions are live
            //    when buffered messages start flowing.
            await RaiseOnConnectedAsync().ConfigureAwait(false);

            // #44 — subscriptions are now complete: hand the held messages (e.g. a resumed session's queue) to them.
            heldReleased = true;
            ReleaseHeldMessages(holdEpoch, replay: !ShuttingDown && StillCurrent(generation));   // #51 — never into a stopping app

            if (ShuttingDown || !StillCurrent(generation)) return false;   // R3/R4 — dropped or stopping during OnConnected

            // #46 — "ready" = connected AND subscriptions restored AND OnConnected done; WaitForConnectionAsync waits for this.
            _ready = true;
            _connectedSignal.TrySetResult(true);

            if (restored is not null)
                _ = Task.Run(() => RaiseAsync(ConnectionRestored, restored, nameof(ConnectionRestored)));

            // 4. Drain offline buffer in background
            StartDrain();

            // 5. Start health monitor — under the same lock DisconnectAsync stops it with, so it can't restart after (R3)
            lock (_healthGate)
            {
                if (ShuttingDown || !StillCurrent(generation)) return false;
                _healthCheck.Start(_client,
                    onDisconnected: () => { KickReconnect(); return Task.CompletedTask; },
                    onPingFailed: ResetZombieConnectionAsync);
            }
            return true;
            }
            finally
            {
                // R2 — whatever interrupted the set-up, never leave the client stuck in "holding" mode.
                if (!heldReleased) ReleaseHeldMessages(holdEpoch, replay: !ShuttingDown && StillCurrent(generation));
            }
        }

        private bool StillCurrent(int generation) => Volatile.Read(ref _connectionGeneration) == generation && IsConnected;

        private ResilientMqttConnectionState StateAfterFailedConnect()
            => _options.Reconnect.Enabled ? ResilientMqttConnectionState.Reconnecting : ResilientMqttConnectionState.Disconnected;

        private void RecordFailure(ResilientMqttFailure failure)
        {
            _lastFailure = failure;
            Interlocked.CompareExchange(ref _downSinceTicks, failure.AtUtc.Ticks, 0);
        }

        private void LogConnectFailure(ResilientMqttFailure f, int attempt, Exception? ex)
        {
            var what = attempt > 0 ? $"Reconnect #{attempt} failed" : "Connect failed";

            // Expected, classified failures get ONE line (no stack trace); only Unknown keeps the exception.
            if (f.Kind == ResilientMqttFailureKind.Unknown && ex is not null)
                _logger.LogError(ResilientMqttLogEvents.ConnectFailed, ex,
                    "[ResilientMqttClient] {What} — {Kind}: {Detail}", what, f.Kind, f.Detail);
            else
                _logger.LogWarning(
                    f.Kind == ResilientMqttFailureKind.Timeout ? ResilientMqttLogEvents.ConnectTimedOut : ResilientMqttLogEvents.ConnectFailed,
                    "[ResilientMqttClient] {What} — {Kind}: {Detail}", what, f.Kind, f.Detail);
        }

        public async Task<bool> WaitForConnectionAsync(TimeSpan timeout, CancellationToken ct = default)
        {
            var deadline = Now + timeout;
            while (true)
            {
                if (IsConnected && _ready) return true;
                if (ShuttingDown) return false;
                var remaining = deadline - Now;
                if (remaining <= TimeSpan.Zero) return false;

                var signal = _connectedSignal;
                try { await signal.Task.WaitAsync(remaining, ct).ConfigureAwait(false); }
                catch (TimeoutException) { return IsConnected && _ready; }
                if (signal.Task.IsCompleted && !(IsConnected && _ready) && ReferenceEquals(signal, _connectedSignal))
                    await Task.Delay(50, ct).ConfigureAwait(false);   // dropped right after connecting — wait for the next signal
            }
        }

        // ════════════════════════════════════════════════════════════════
        // PUBLISH
        // ════════════════════════════════════════════════════════════════
        public Task PublishAsync<T>(
            string topic,
            T payload,
            bool retain = false,
            ResilientMqttQos qos = ResilientMqttQos.AtLeastOnce,
            bool useTopicPrefix = true,
            CancellationToken ct = default)
            => PublishWithResultAsync(topic, payload, retain, qos, useTopicPrefix, ct);

        public Task PublishRawAsync(
            string topic,
            string payload,
            bool retain = false,
            ResilientMqttQos qos = ResilientMqttQos.AtLeastOnce,
            bool useTopicPrefix = true,
            CancellationToken ct = default)
            => PublishBytesWithResultAsync(topic, Encoding.UTF8.GetBytes(payload ?? string.Empty), retain, qos, useTopicPrefix, ct);

        public Task PublishBytesAsync(
            string topic,
            byte[] payload,
            bool retain = false,
            ResilientMqttQos qos = ResilientMqttQos.AtLeastOnce,
            bool useTopicPrefix = true,
            CancellationToken ct = default)
            => PublishBytesWithResultAsync(topic, payload, retain, qos, useTopicPrefix, ct);

        public Task<ResilientMqttPublishOutcome> PublishWithResultAsync<T>(
            string topic,
            T payload,
            bool retain = false,
            ResilientMqttQos qos = ResilientMqttQos.AtLeastOnce,
            bool useTopicPrefix = true,
            CancellationToken ct = default)
        {
            var json = payload is string s ? s : JsonSerializer.Serialize(payload, JsonOpts);
            return PublishBytesWithResultAsync(topic, Encoding.UTF8.GetBytes(json ?? string.Empty), retain, qos, useTopicPrefix, ct);
        }

        public Task<ResilientMqttPublishOutcome> PublishBytesWithResultAsync(
            string topic,
            byte[] payload,
            bool retain = false,
            ResilientMqttQos qos = ResilientMqttQos.AtLeastOnce,
            bool useTopicPrefix = true,
            CancellationToken ct = default)
        {
            if (Volatile.Read(ref _resourcesDisposed) == 1) return Task.FromResult(ResilientMqttPublishOutcome.Dropped);

            var fullTopic = useTopicPrefix ? BuildFullTopic(topic) : NormalizeTopic(topic);
            return PublishCoreAsync(BuildMessage(fullTopic, payload, retain, qos), bypassBreaker: false, allowBuffer: true, ct);
        }

        private static MqttApplicationMessage BuildMessage(string fullTopic, byte[] payload, bool retain, ResilientMqttQos qos)
            => new MqttApplicationMessageBuilder()
                .WithTopic(fullTopic)
                .WithPayload(payload ?? Array.Empty<byte>())
                .WithRetainFlag(retain)
                .WithQualityOfServiceLevel((MqttQualityOfServiceLevel)qos)
                .Build();

        // The one publish path. bypassBreaker: the ONLINE / graceful OFFLINE status messages must go out even when
        // the breaker is open (#30). allowBuffer: false for the graceful OFFLINE (no point buffering at shutdown).
        private async Task<ResilientMqttPublishOutcome> PublishCoreAsync(
            MqttApplicationMessage message, bool bypassBreaker, bool allowBuffer, CancellationToken ct, bool skipIfShuttingDown = false)
        {
            if (Volatile.Read(ref _resourcesDisposed) == 1) return ResilientMqttPublishOutcome.Dropped;

            var isProbe = false;
            if (!bypassBreaker && !_breaker.TryAcquire(out isProbe))
            {
                if (_publishBlockedLog.ShouldLog(PublishLogWindow, out var skippedBlocked))   // #69
                    _logger.LogWarning(ResilientMqttLogEvents.PublishBlockedCircuitOpen,
                        "[ResilientMqttClient] Publish blocked — circuit OPEN. Topic: {Topic}{Skipped}",
                        message.Topic, LogThrottle.Suffix(skippedBlocked));
                return Keep(message, allowBuffer);
            }

            var verdict = false;   // the breaker got a success/failure verdict for this acquisition
            try
            {
                if (!IsConnected)
                {
                    KickReconnect();
                    return Keep(message, allowBuffer);
                }

                try { await _publishLock.WaitAsync(ct).ConfigureAwait(false); }
                catch (OperationCanceledException) { return ResilientMqttPublishOutcome.Dropped; }   // caller cancelled
                catch (ObjectDisposedException) { return Keep(message, allowBuffer); }               // #17 — disposed mid-flight

                try
                {
                    // R3 — the ONLINE status must never go out after a shutdown started (it would follow our OFFLINE).
                    if (skipIfShuttingDown && ShuttingDown) return ResilientMqttPublishOutcome.Dropped;

                    // #7 — bound the publish so a half-open socket can't hang forever holding _publishLock
                    // (which would freeze every other publish + the buffer drain behind it).
                    var result = await WithTimeout(t => _client.PublishAsync(message, t), PublishTimeout, ct).ConfigureAwait(false);

                    if (!bypassBreaker)
                    {
                        verdict = true;
                        if (_breaker.RecordSuccess()) StartDrain();   // #29 — breaker just closed: send the backlog
                    }

                    // #25 — only an acknowledged (QoS1/2) publish proves the broker → client direction works.
                    if (message.QualityOfServiceLevel != MqttQualityOfServiceLevel.AtMostOnce)
                        _healthCheck.NotifyActivity();

                    if (!result.IsSuccess)
                    {
                        // #24 — the broker refused it (e.g. ACL). Not a connectivity problem: not buffered, no breaker hit.
                        RecordDenied(message.Topic, result.ReasonCode.ToString(), result.ReasonString);
                        return ResilientMqttPublishOutcome.Denied;
                    }
                    ReportPublishProblemsNotLogged("Publishing works again");
                    return ResilientMqttPublishOutcome.Sent;
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    return ResilientMqttPublishOutcome.Dropped;   // #32 — the caller cancelled; not a broker failure
                }
                catch (Exception ex)
                {
                    // A timed-out publish (OperationCanceledException) is treated exactly like a failed one.
                    if (!bypassBreaker) { verdict = true; _breaker.RecordFailure(); }
                    var outcome = Keep(message, allowBuffer);
                    LogPublishFailure(ex, message.Topic, outcome);
                    return outcome;
                }
                finally
                {
                    try { _publishLock.Release(); } catch (ObjectDisposedException) { }
                }
            }
            finally
            {
                if (!bypassBreaker && !verdict) _breaker.RecordAbandoned(isProbe);   // #28 — never leak the probe
            }
        }

        private ResilientMqttPublishOutcome Keep(MqttApplicationMessage message, bool allowBuffer)
            => allowBuffer && _buffer.TryEnqueue(message) ? ResilientMqttPublishOutcome.Buffered : ResilientMqttPublishOutcome.Dropped;

        // #68 — when the link stalls every publish fails. Log the first per window (with the exception) and count the
        // rest, so a fleet in trouble doesn't fill its disks. Only the log line is limited, never the buffering.
        private void LogPublishFailure(Exception ex, string topic, ResilientMqttPublishOutcome outcome)
        {
            if (!_publishFailedLog.ShouldLog(PublishLogWindow, out var skipped)) return;

            _logger.LogError(ResilientMqttLogEvents.PublishFailed, ex,
                "[ResilientMqttClient] Publish failed — {Outcome}. Topic: {Topic}{Skipped}",
                outcome == ResilientMqttPublishOutcome.Buffered ? "buffered for retry" : "message dropped (offline buffer disabled)",
                topic, LogThrottle.Suffix(skipped));
        }

        // Called when publishing recovers and at shutdown (#70), so the skipped lines are never silently lost.
        private void ReportPublishProblemsNotLogged(string when)
        {
            var failed = _publishFailedLog.TakeSkipped();
            var blocked = _publishBlockedLog.TakeSkipped();
            if (failed > 0 || blocked > 0)
                _logger.LogInformation(ResilientMqttLogEvents.PublishFailuresNotLogged,
                    "[ResilientMqttClient] {When} — {Failed} publish failure(s) and {Blocked} blocked publish(es) were not logged individually.",
                    when, failed, blocked);
        }

        private void RecordDenied(string topic, string code, string? reason)
        {
            Interlocked.Increment(ref _deniedPublishes);

            // Rate-limited per topic (once a minute) so a misconfigured ACL can't flood the log.
            var now = Now.Ticks;
            if (_deniedLogTicks.Count >= MaxDeniedTopicsTracked && !_deniedLogTicks.ContainsKey(topic))
                _deniedLogTicks.Clear();   // R8 — bounded: unique per-request topics can't grow it forever
            var last = _deniedLogTicks.GetOrAdd(topic, 0);
            if (now - last < TimeSpan.TicksPerMinute) return;
            _deniedLogTicks[topic] = now;

            _logger.LogWarning(ResilientMqttLogEvents.PublishDenied,
                "[ResilientMqttClient] Publish REFUSED by the broker — {Topic}: {Code}{Reason}. The message was not delivered (check the broker ACL).",
                topic, code, string.IsNullOrWhiteSpace(reason) ? string.Empty : $" ({reason})");
        }

        // ════════════════════════════════════════════════════════════════
        private void StartDrain()
        {
            if (_buffer.Count > 0 && IsConnected && !ShuttingDown)
                _ = Task.Run(() => DrainOfflineBufferAsync(CancellationToken.None));
        }

        private async Task DrainOfflineBufferAsync(CancellationToken ct)
        {
            // #16 — single-flight: two quick reconnects would otherwise start two drains that both
            // TryPeek the same head (duplicate send) and then each TryDequeue — dropping the message
            // in between. Only one drain runs at a time.
            if (Interlocked.CompareExchange(ref _isDraining, 1, 0) != 0) return;
            try
            {
                if (_buffer.Count == 0 || !IsConnected) return;
                _logger.LogInformation(ResilientMqttLogEvents.BufferDraining,
                    "[ResilientMqttClient] Draining offline buffer — {Count}", _buffer.Count);

                while (_buffer.Count > 0 && IsConnected && !ShuttingDown && !ct.IsCancellationRequested)
                {
                    if (!_buffer.TryPeek(out var msg) || msg is null) break;

                    try { await _publishLock.WaitAsync(ct).ConfigureAwait(false); }
                    catch (ObjectDisposedException) { break; }   // #17 — disposed mid-drain
                    try
                    {
                        // #7 — bound the drained publish too (same half-open hazard as the live path).
                        var result = await WithTimeout(t => _client.PublishAsync(msg, t), PublishTimeout, ct).ConfigureAwait(false);
                        _buffer.TryDequeue(out _);
                        if (msg.QualityOfServiceLevel != MqttQualityOfServiceLevel.AtMostOnce) _healthCheck.NotifyActivity();
                        if (!result.IsSuccess) RecordDenied(msg.Topic, result.ReasonCode.ToString(), result.ReasonString);   // refused → drop, never retry
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ResilientMqttLogEvents.BufferDrainFailed, ex, "[ResilientMqttClient] Drain failed — stopping");
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
        // SUBSCRIBE
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
            _pendingUnsubscribes.TryRemove(resolvedTopic, out _);   // #43 — wanted again

            if (!IsConnected) return;   // remembered — subscribed on the next connect

            // #27 — already active on THIS connection with the same QoS (e.g. restored by the library, then
            // subscribed again from OnConnected): the handler is updated above; don't send a second SUBSCRIBE
            // (it would only make the broker replay retained messages).
            if (_activeSubscriptions.TryGetValue(resolvedTopic, out var activeQos) && activeQos == qos) return;

            await SubscribeOnWireAsync(resolvedTopic, qos, isRestore: false, ct).ConfigureAwait(false);
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
            _activeSubscriptions.TryRemove(resolvedTopic, out _);
            _rejectedSubscriptions.TryRemove(resolvedTopic, out _);

            if (!IsConnected)
            {
                // #43 — a persistent broker session still holds this subscription; undo it on the next connect.
                _pendingUnsubscribes[resolvedTopic] = 0;
                return;
            }

            try
            {
                await UnsubscribeOnWireAsync(resolvedTopic, ct).ConfigureAwait(false);   // #31 bounded
            }
            catch (Exception) when (!IsConnected)
            {
                _pendingUnsubscribes[resolvedTopic] = 0;   // dropped mid-call — apply on the next connect
            }
        }

        private async Task UnsubscribeOnWireAsync(string topic, CancellationToken ct)
        {
            var options = new MqttClientUnsubscribeOptionsBuilder().WithTopicFilter(topic).Build();
            await WithTimeout(t => _client.UnsubscribeAsync(options, t), ConnectTimeout, ct).ConfigureAwait(false);
            _logger.LogInformation(ResilientMqttLogEvents.Unsubscribed, "[ResilientMqttClient] Unsubscribed — {Topic}", topic);
        }

        private async Task ApplyPendingUnsubscribesAsync(bool sessionPresent, CancellationToken ct)
        {
            if (_pendingUnsubscribes.IsEmpty) return;
            foreach (var topic in _pendingUnsubscribes.Keys)
            {
                if (!_pendingUnsubscribes.TryRemove(topic, out _)) continue;
                if (!sessionPresent || _subscriptions.ContainsKey(topic)) continue;   // new session / re-subscribed since
                try { await UnsubscribeOnWireAsync(topic, ct).ConfigureAwait(false); }
                catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
                catch (Exception ex)
                {
                    _pendingUnsubscribes[topic] = 0;   // try again on the next connect
                    _logger.LogError(ResilientMqttLogEvents.SubscribeFailed, ex,
                        "[ResilientMqttClient] Applying an offline unsubscribe failed — {Topic}", topic);
                }
            }
        }

        // Sends one SUBSCRIBE and checks the broker's answer (#23 — a refusal used to be logged as "Subscribed").
        private async Task<bool> SubscribeOnWireAsync(string topic, ResilientMqttQos qos, bool isRestore, CancellationToken ct)
        {
            var filter = new MqttTopicFilterBuilder()
                .WithTopic(topic)
                .WithQualityOfServiceLevel((MqttQualityOfServiceLevel)qos);

            // MQTT 5: retained messages only for a NEW subscription — restoring a subscription the broker still
            // holds (resumed session) doesn't replay every retained message.
            if (_isV5) filter.WithRetainHandling(MqttRetainHandling.SendAtSubscribeIfNewSubscriptionOnly);

            var options = new MqttClientSubscribeOptionsBuilder().WithTopicFilter(filter.Build()).Build();
            var result = await WithTimeout(t => _client.SubscribeAsync(options, t), ConnectTimeout, ct).ConfigureAwait(false);   // #31

            var code = result.Items.FirstOrDefault()?.ResultCode ?? MqttClientSubscribeResultCode.UnspecifiedError;
            if (code is MqttClientSubscribeResultCode.GrantedQoS0
                     or MqttClientSubscribeResultCode.GrantedQoS1
                     or MqttClientSubscribeResultCode.GrantedQoS2)
            {
                _activeSubscriptions[topic] = qos;
                _rejectedSubscriptions.TryRemove(topic, out _);
                if (!isRestore)
                    _logger.LogInformation(ResilientMqttLogEvents.Subscribed, "[ResilientMqttClient] Subscribed — {Topic}", topic);
                return true;
            }

            _rejectedSubscriptions[topic] = code.ToString();
            _logger.LogError(ResilientMqttLogEvents.SubscriptionRefused,
                "[ResilientMqttClient] Subscription REFUSED by the broker — {Topic}: {Code}. Messages for it will NOT arrive (check the broker ACL).",
                topic, code);
            return false;
        }

        private async Task ResubscribeAllAsync(CancellationToken ct)
        {
            if (_subscriptions.IsEmpty) return;
            _logger.LogInformation(ResilientMqttLogEvents.Resubscribing,
                "[ResilientMqttClient] Restoring {Count} subscription(s)", _subscriptions.Count);

            // Keys are already resolved (post-prefix) topics — just send them.
            foreach (var (resolvedTopic, entry) in _subscriptions)
            {
                try
                {
                    await SubscribeOnWireAsync(resolvedTopic, entry.Qos, isRestore: true, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
                catch (Exception ex)
                {
                    _logger.LogError(ResilientMqttLogEvents.SubscribeFailed, ex,
                        "[ResilientMqttClient] Restoring subscription failed — {Topic}", resolvedTopic);
                }
            }
        }

        // ════════════════════════════════════════════════════════════════
        // INBOUND
        // ════════════════════════════════════════════════════════════════
        private async Task OnMessageReceivedAsync(MqttApplicationMessageReceivedEventArgs e)
        {
            _healthCheck.NotifyActivity();   // inbound = proof the broker → client direction works

            var topic = e.ApplicationMessage.Topic;
            var payload = e.ApplicationMessage.ConvertPayloadToString();
            var matched = HasSubscriptionFor(topic);

            // #44 — hold it while the connection is being set up if nothing can handle it YET (the app subscribes in
            // OnConnected), or if something is already held / being replayed (keeps arrival order, and so PUBACK order).
            // With nothing unmatched, nothing is ever held — normal live behaviour.
            int epoch;
            lock (_holdGate)
                epoch = _holding && (!matched || _heldMessages.Count > 0 || _replaying) ? _holdEpoch : 0;

            if (epoch != 0)
            {
                // Back-pressure: a full queue makes this message wait (MQTTnet hands messages over one at a time, so
                // arrival order is kept). Shutdown cancels the wait; the message stays UNACKNOWLEDGED → redelivered.
                try { await _heldSlots.WaitAsync(_shutdownCts.Token).ConfigureAwait(false); }
                catch (Exception ex) when (ex is OperationCanceledException or ObjectDisposedException)
                {
                    e.AutoAcknowledge = false;
                    return;
                }

                bool stale;
                lock (_holdGate)
                {
                    if (epoch == _holdEpoch && _holding)
                    {
                        var ack = e.ApplicationMessage.QualityOfServiceLevel != MqttQualityOfServiceLevel.AtMostOnce ? e : null;
                        if (ack is not null) e.AutoAcknowledge = false;   // R1 — acknowledged after it reached a handler
                        _heldMessages.Enqueue(new HeldMessage(topic, payload, ack, epoch));
                        return;
                    }
                    stale = epoch != _holdEpoch;
                }
                _heldSlots.Release();
                if (stale) { e.AutoAcknowledge = false; return; }   // from an older connection → leave it to redelivery
                matched = HasSubscriptionFor(topic);                // the replay finished while we waited → handle it live
            }

            if (!matched)
            {
                _logger.LogDebug(ResilientMqttLogEvents.MessageUnmatched,
                    "[ResilientMqttClient] Message on {Topic} matched no subscription — ignored", topic);
                return;
            }

            await DispatchAsync(topic, payload).ConfigureAwait(false);
        }

        private bool HasSubscriptionFor(string topic)
        {
            foreach (var pattern in _subscriptions.Keys)
                if (IsTopicMatch(pattern, topic)) return true;
            return false;
        }

        private Task DispatchAsync(string topic, string payload)
        {
            if (!_concurrentDispatch)
                return DispatchInlineAsync(topic, payload);

            // #35 — Concurrent: hand each handler to the thread pool and return at once, so a slow handler can
            // never stall the receive loop or the keep-alive.
            return DispatchConcurrentAsync(topic, payload);
        }

        private async Task DispatchConcurrentAsync(string topic, string payload)
        {
            // Back-pressure (R9 — one bounded semaphore, no polling): each handler call takes a slot of
            // MaxConcurrency + MaxPending. When all are taken the receive loop waits here — that slows intake (and so
            // the broker) instead of growing an unbounded backlog in memory.
            foreach (var (pattern, entry) in _subscriptions)
            {
                if (!IsTopicMatch(pattern, topic)) continue;

                if (_dispatchCapacity!.Wait(0))
                {
                    Interlocked.Exchange(ref _dispatchBackPressureLogged, 0);
                }
                else
                {
                    if (Interlocked.Exchange(ref _dispatchBackPressureLogged, 1) == 0)
                        _logger.LogWarning(ResilientMqttLogEvents.DispatchBackPressure,
                            "[ResilientMqttClient] Handlers can't keep up ({Pending} pending) — slowing intake", _dispatchCapacityTotal);
                    try { await _dispatchCapacity.WaitAsync(_shutdownCts.Token).ConfigureAwait(false); }   // #52 — shutdown unblocks it
                    catch (OperationCanceledException) { return; }
                    catch (ObjectDisposedException) { return; }
                }

                _ = Task.Run(() => RunHandlerConcurrentAsync(pattern, entry.Handler, topic, payload));
            }
        }

        private async Task DispatchInlineAsync(string topic, string payload)
        {
            foreach (var (pattern, entry) in _subscriptions)
            {
                if (!IsTopicMatch(pattern, topic)) continue;
                await RunHandlerAsync(pattern, entry.Handler, topic, payload).ConfigureAwait(false);
            }
        }

        // #44 — end of connection set-up for attempt <epoch>. Success → replay what was held, in arrival order (new
        // messages queue behind it until the queue is empty), acknowledging each QoS1/2 message after it reached its
        // handler. Failed set-up / shutdown → set aside UNACKNOWLEDGED: with a persistent session the broker redelivers
        // them on the next session; with a clean session it discards them (nothing would have been kept anyway).
        private void ReleaseHeldMessages(int epoch, bool replay)
        {
            int count;
            lock (_holdGate)
            {
                if (epoch != _holdEpoch || !_holding || _replaying) return;   // a newer attempt owns the queue / already replaying
                count = _heldMessages.Count;
                if (!replay || count == 0)
                {
                    DiscardHeldLocked();
                    _holding = false;
                    if (!replay && count > 0)
                        _logger.LogWarning(ResilientMqttLogEvents.BufferOverflow,
                            "[ResilientMqttClient] {Count} message(s) received during an interrupted connection set-up were set aside unacknowledged (the broker redelivers them on the next session)", count);
                    return;
                }
                _replaying = true;
            }

            _logger.LogInformation(ResilientMqttLogEvents.HeldMessagesReplayed,
                "[ResilientMqttClient] Delivering {Count} message(s) that arrived before subscriptions were ready", count);
            _ = Task.Run(async () =>
            {
                while (true)
                {
                    HeldMessage next;
                    lock (_holdGate)
                    {
                        if (epoch != _holdEpoch) return;   // a new connection took over (and set aside what was left)
                        if (!_heldMessages.TryDequeue(out next))
                        {
                            _replaying = false;
                            _holding = false;              // queue empty → live dispatch resumes, order intact
                            return;
                        }
                    }
                    _heldSlots.Release();

                    if (!HasSubscriptionFor(next.Topic))
                    {
                        _logger.LogWarning(ResilientMqttLogEvents.MessageUnmatched,
                            "[ResilientMqttClient] Message on {Topic} matched no subscription after connecting — dropped", next.Topic);
                    }
                    else
                    {
                        try { await DispatchAsync(next.Topic, next.Payload).ConfigureAwait(false); }
                        catch (Exception ex) { _logger.LogError(ResilientMqttLogEvents.HandlerThrew, ex, "[ResilientMqttClient] Replaying a held message failed"); }
                    }
                    await AcknowledgeAsync(next).ConfigureAwait(false);
                }
            });
        }

        // Caller holds _holdGate. Held messages are dropped WITHOUT acknowledging them on purpose → redelivered.
        private void DiscardHeldLocked()
        {
            var n = _heldMessages.Count;
            _heldMessages.Clear();
            if (n > 0) { try { _heldSlots.Release(n); } catch (SemaphoreFullException) { } }
        }

        private async Task AcknowledgeAsync(HeldMessage m)
        {
            if (m.Ack is null) return;
            bool current;
            lock (_holdGate) current = m.Epoch == _holdEpoch;
            if (!current || !IsConnected) return;   // its packet id belongs to an older connection — leave it to redelivery
            try { await m.Ack.AcknowledgeAsync(CancellationToken.None).ConfigureAwait(false); }
            catch (Exception ex) { _logger.LogDebug(ex, "[ResilientMqttClient] Acknowledging a held message failed"); }
        }

        private async Task RunHandlerConcurrentAsync(string pattern, IResilientMqttMessageHandler handler, string topic, string payload)
        {
            try
            {
                await _dispatchGate!.WaitAsync(_lifetimeCts.Token).ConfigureAwait(false);
                try { await RunHandlerAsync(pattern, handler, topic, payload).ConfigureAwait(false); }
                finally { try { _dispatchGate.Release(); } catch (ObjectDisposedException) { } }
            }
            catch (OperationCanceledException) { /* shutting down */ }
            catch (ObjectDisposedException) { /* shutting down */ }
            finally { try { _dispatchCapacity!.Release(); } catch (ObjectDisposedException) { } }
        }

        private async Task RunHandlerAsync(string pattern, IResilientMqttMessageHandler handler, string topic, string payload)
        {
            try
            {
                // #12 — hand handlers a real token (cancelled on dispose) instead of None,
                // so inbound work can be cancelled at shutdown.
                await handler.HandleAsync(topic, payload, _lifetimeCts.Token).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ResilientMqttLogEvents.HandlerThrew, ex,
                    "[ResilientMqttClient] Handler threw — Pattern: {Pattern}, Topic: {Topic}", pattern, topic);
            }
        }

        private static bool IsTopicMatch(string pattern, string topic)
        {
            if (pattern == topic) return true;
            return MqttTopicFilterComparer.Compare(topic, pattern)
                   == MqttTopicFilterCompareResult.IsMatch;
        }

        // ════════════════════════════════════════════════════════════════
        // DISCONNECT / RECONNECT
        // ════════════════════════════════════════════════════════════════
        private Task OnDisconnectedAsync(MqttClientDisconnectedEventArgs e)
        {
            if (ShuttingDown) return Task.CompletedTask;

            // #21 — MQTTnet raises DisconnectedAsync after every FAILED connect attempt too
            // (ClientWasConnected = false). ConnectAsync already recorded that failure; treating it as a
            // "drop" made OnDisconnected fire once per attempt for the whole outage.
            if (!e.ClientWasConnected) return Task.CompletedTask;

            var now = Now;
            var failure = Interlocked.Exchange(ref _forcedResetFailure, null)
                          ?? FailureClassifier.FromDisconnect(e.Reason, e.ReasonString, e.Exception, _options.Tls.Enabled, now);
            var sinceTicks = Interlocked.Exchange(ref _connectedSinceTicks, 0);
            var wasConnectedFor = sinceTicks == 0 ? TimeSpan.Zero : now - new DateTime(sinceTicks, DateTimeKind.Utc);

            RecordFailure(failure);
            _activeSubscriptions.Clear();
            Interlocked.Increment(ref _connectionGeneration);   // R4 — an in-flight set-up must not declare "ready"
            _ready = false;
            if (_connectedSignal.Task.IsCompleted) _connectedSignal = NewSignal();

            _logger.LogWarning(ResilientMqttLogEvents.ConnectionLost,
                "[ResilientMqttClient] Connection lost — {Kind}: {Detail} (was up {Duration})",
                failure.Kind, failure.Detail, FormatDuration(wasConnectedFor));
            SetState(StateAfterFailedConnect());

            // Notify user handlers (fire-and-forget — don't block MQTTnet's event loop)
            _ = Task.Run(RaiseOnDisconnectedAsync);
            var args = new ResilientMqttConnectionLostArgs(failure, wasConnectedFor);
            _ = Task.Run(() => RaiseAsync(ConnectionLost, args, nameof(ConnectionLost)));

            KickReconnect();
            return Task.CompletedTask;
        }

        private void KickReconnect()
        {
            if (_options.Reconnect.Enabled && !ShuttingDown)
                _ = Task.Run(TryReconnectAsync);
        }

        private async Task TryReconnectAsync()
        {
            while (true)
            {
                if (!_options.Reconnect.Enabled || ShuttingDown) return;
                if (Interlocked.CompareExchange(ref _isReconnecting, 1, 0) != 0) return;

                try
                {
                    var token = ResetReconnectCts();   // #3 — thread-safe swap

                    // #4 — record when this outage started (only if not already tracking one).
                    Interlocked.CompareExchange(ref _downSinceTicks, Now.Ticks, 0);

                    while (!IsConnected && !ShuttingDown && !token.IsCancellationRequested)
                    {
                        var attempt = Interlocked.Increment(ref _reconnectAttempt);
                        var delay = NextDelay(attempt);

                        _logger.LogInformation(ResilientMqttLogEvents.ReconnectScheduled,
                            "[ResilientMqttClient] Reconnect #{Attempt} in {Seconds:F1}s", attempt, delay.TotalSeconds);

                        RaiseStillDownIfDue(attempt);   // #4

                        try { await Task.Delay(delay, token).ConfigureAwait(false); }
                        catch (OperationCanceledException) { break; }

                        if (await ConnectAsync(token, isReconnectLoop: true, attempt).ConfigureAwait(false)) break;
                    }
                }
                catch (ObjectDisposedException) { return; }
                finally { Interlocked.Exchange(ref _isReconnecting, 0); }

                // #26 — a drop that happened while we still held the latch (right after connecting, or during
                // OnConnected) found the latch taken and gave up. Re-check now that it is released.
                if (IsConnected || ShuttingDown) return;
            }
        }

        private TimeSpan NextDelay(int attempt)
        {
            var delay = _retry.GetNextDelay(attempt);

            // #36 — wrong credentials don't fix themselves; don't hammer the broker's authentication.
            var authDelay = _options.Reconnect.AuthRejectedDelaySeconds;
            if (authDelay > 0 && _lastFailure?.Kind == ResilientMqttFailureKind.AuthRejected)
            {
                var min = TimeSpan.FromSeconds(authDelay);
                if (delay < min) delay = min;
            }
            return delay;
        }

        // #25 — the health ping got no answer while MQTTnet still reports "connected": the connection is a
        // zombie (e.g. the broker → client direction died). Close it so the normal drop → reconnect path runs.
        private async Task ResetZombieConnectionAsync()
        {
            if (!IsConnected || ShuttingDown) return;

            Volatile.Write(ref _forcedResetFailure, FailureClassifier.PingTimeout(Now));
            _logger.LogWarning(ResilientMqttLogEvents.HealthForcedReconnect,
                "[ResilientMqttClient] Broker did not answer the health ping — resetting the connection");

            // R5 — MQTT 3.1.1 has no DISCONNECT reason: any DISCONNECT makes the broker DISCARD our Will, and MQTTnet has
            // no "close without DISCONNECT". So publish the Will payload ourselves first (QoS 0 — no ack to wait for on a
            // half-dead link). If the client → broker path is dead too, neither arrives and the broker's keep-alive fires
            // the real Will instead.
            if (!_isV5 && _options.LastWill.Enabled)
            {
                await PublishCoreAsync(
                    BuildMessage(BuildFullTopic(_options.LastWill.Topic),
                        Encoding.UTF8.GetBytes(_options.LastWill.OfflinePayload ?? string.Empty),
                        _options.LastWill.Retain, ResilientMqttQos.AtMostOnce),
                    bypassBreaker: true, allowBuffer: false, CancellationToken.None).ConfigureAwait(false);
            }

            try
            {
                // MQTT 5: a non-normal reason makes the broker publish our Last Will (the outage is real).
                var options = _isV5
                    ? new MqttClientDisconnectOptionsBuilder()
                        .WithReason(MqttClientDisconnectOptionsReason.UnspecifiedError)
                        .WithReasonString("health ping unanswered")
                        .Build()
                    : new MqttClientDisconnectOptionsBuilder().Build();
                await WithTimeout(t => _client.DisconnectAsync(options, t), TimeSpan.FromSeconds(5), CancellationToken.None)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "[ResilientMqttClient] Closing the dead connection threw (expected on a dead socket)");
            }

            // MQTTnet raises DisconnectedAsync (→ reconnect). Belt and braces if it didn't.
            if (!IsConnected) KickReconnect();
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

            var down = Now - new DateTime(sinceTicks, DateTimeKind.Utc);
            if (down < TimeSpan.FromSeconds(threshold)) return;
            if (Interlocked.Exchange(ref _stillDownFired, 1) == 1) return;   // fire exactly once

            _logger.LogWarning(ResilientMqttLogEvents.StillDown,
                "[ResilientMqttClient] Still disconnected after {Duration} ({Attempts} attempts) — last problem: {Kind}: {Detail}",
                FormatDuration(down), attempt, _lastFailure?.Kind ?? ResilientMqttFailureKind.Unknown, _lastFailure?.Detail ?? "-");

            var handler = StillDown;
            if (handler is null) return;
            _ = Task.Run(() =>
            {
                foreach (var single in handler.GetInvocationList())
                {
                    try { ((Action<int, TimeSpan>)single)(attempt, down); }
                    catch (Exception ex) { _logger.LogError(ResilientMqttLogEvents.EventHandlerThrew, ex, "[ResilientMqttClient] StillDown handler threw"); }
                }
            });
        }

        // ════════════════════════════════════════════════════════════════
        public async Task DisconnectAsync(CancellationToken ct = default)
        {
            Interlocked.Exchange(ref _isShuttingDown, 1);
            ReportPublishProblemsNotLogged("Shutting down");   // #70
            try { _shutdownCts.Cancel(); } catch (ObjectDisposedException) { }   // unblock held-queue / dispatch waits (#52)
            SetState(ResilientMqttConnectionState.ShuttingDown);
            CancelReconnectCts();
            // #14 / R3 — stop the timer. A connection set-up starts it under the same lock after checking ShuttingDown,
            // so it can never be (re)started after this point; its ONLINE status is skipped once shutdown started.
            lock (_healthGate) _healthCheck.Stop();
            _connectedSignal.TrySetResult(false);

            if (!IsConnected)
            {
                SetState(ResilientMqttConnectionState.Disconnected);
                return;
            }

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

                    // #7/#30 — bounded, bypasses the breaker (an open breaker must not keep the retained status
                    // "online" after a normal stop), and a failure here can NEVER skip the clean DISCONNECT.
                    try
                    {
                        var outcome = await PublishCoreAsync(
                            BuildMessage(BuildFullTopic(_options.LastWill.Topic),
                                Encoding.UTF8.GetBytes(gracefulPayload ?? string.Empty),
                                _options.LastWill.Retain, ResilientMqttQos.AtLeastOnce),
                            bypassBreaker: true, allowBuffer: false, ct).ConfigureAwait(false);
                        if (outcome != ResilientMqttPublishOutcome.Sent)
                            _logger.LogWarning(ResilientMqttLogEvents.GracefulDisconnectFailed,
                                "[ResilientMqttClient] Graceful OFFLINE status not delivered ({Outcome}) — disconnecting anyway", outcome);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ResilientMqttLogEvents.GracefulDisconnectFailed, ex,
                            "[ResilientMqttClient] Graceful OFFLINE publish failed — disconnecting anyway");
                    }
                }

                await WithTimeout(
                    t => _client.DisconnectAsync(new MqttClientDisconnectOptionsBuilder().Build(), t),
                    ConnectTimeout, ct).ConfigureAwait(false);

                SetState(ResilientMqttConnectionState.Disconnected);
                _logger.LogInformation(ResilientMqttLogEvents.DisconnectedGracefully, "[ResilientMqttClient] Disconnected gracefully");

                // Notify user handlers — fire-and-forget so a slow user handler
                // can't block app shutdown beyond reason.
                _ = Task.Run(RaiseOnDisconnectedAsync);
            }
            catch (Exception ex)
            {
                _logger.LogError(ResilientMqttLogEvents.GracefulDisconnectFailed, ex, "[ResilientMqttClient] Graceful disconnect failed");
                SetState(ResilientMqttConnectionState.Disconnected);
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 1) return;
            Interlocked.Exchange(ref _isShuttingDown, 1);
            try { _shutdownCts.Cancel(); } catch (ObjectDisposedException) { }
            CancelReconnectCts();
            _healthCheck.Stop();

            // #42 — disconnect BEFORE marking resources disposed, so the graceful OFFLINE status still goes out
            // when the host disposes the client without calling DisconnectAsync first.
            try { await DisconnectAsync().ConfigureAwait(false); }
            catch { /* logged */ }

            try { _lifetimeCts.Cancel(); } catch (ObjectDisposedException) { }   // #12 — signal handlers to stop
            await WaitForHandlersAsync().ConfigureAwait(false);                 // #35 — let concurrent handlers finish (bounded)

            Interlocked.Exchange(ref _resourcesDisposed, 1);
            _client.DisconnectedAsync -= OnDisconnectedAsync;
            _client.ApplicationMessageReceivedAsync -= OnMessageReceivedAsync;
            _client.Dispose();

            // #17 — a background publish/drain/reconnect may still be mid-flight; swallow the
            // ObjectDisposedException that a disposed semaphore/CTS would throw during shutdown.
            try { _publishLock.Dispose(); } catch (ObjectDisposedException) { }
            try { _connectLock.Dispose(); } catch (ObjectDisposedException) { }
            try { _dispatchGate?.Dispose(); } catch (ObjectDisposedException) { }
            try { _dispatchCapacity?.Dispose(); } catch (ObjectDisposedException) { }
            lock (_ctsGate) { try { _reconnectCts?.Dispose(); } catch (ObjectDisposedException) { } }
            try { _lifetimeCts.Dispose(); } catch (ObjectDisposedException) { }
            try { _shutdownCts.Dispose(); } catch (ObjectDisposedException) { }
            try { _heldSlots.Dispose(); } catch (ObjectDisposedException) { }
            _healthCheck.Dispose();
        }

        private async Task WaitForHandlersAsync()
        {
            if (!_concurrentDispatch) return;
            var deadline = Now + TimeSpan.FromSeconds(Math.Max(0, _options.Dispatch.ShutdownWaitSeconds));
            while (_dispatchCapacity!.CurrentCount < _dispatchCapacityTotal && Now < deadline)
                await Task.Delay(25).ConfigureAwait(false);
        }

        // ════════════════════════════════════════════════════════════════
        public ResilientMqttConnectionInfo ConnectionInfo
        {
            get
            {
                var since = Interlocked.Read(ref _connectedSinceTicks);
                var down = Interlocked.Read(ref _downSinceTicks);
                return new ResilientMqttConnectionInfo(
                    State,
                    IsConnected,
                    since == 0 ? null : new DateTime(since, DateTimeKind.Utc),
                    down == 0 ? null : new DateTime(down, DateTimeKind.Utc),
                    Volatile.Read(ref _reconnectAttempt),
                    Interlocked.Read(ref _totalConnects),
                    _lastFailure,
                    _buffer.Count,
                    new Dictionary<string, string>(_rejectedSubscriptions),
                    Interlocked.Read(ref _deniedPublishes),
                    _isV5 ? "V500" : "V311",
                    _sessionPresent);
            }
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
            lock (_stateGate)   // #45 — check-and-set as one step, so two threads can't both "change" it
            {
                if (_state == newState) return;
                _state = newState;
            }

            var handler = StateChanged;
            if (handler is null) return;
            foreach (var single in handler.GetInvocationList())
            {
                try { ((EventHandler<ResilientMqttConnectionState>)single)(this, newState); }
                catch (Exception ex)
                {
                    _logger.LogError(ResilientMqttLogEvents.EventHandlerThrew, ex, "[ResilientMqttClient] StateChanged handler threw");
                }
            }
        }

        private static string FormatDuration(TimeSpan d)
            => d.TotalSeconds < 60 ? $"{d.TotalSeconds:F0} s"
             : d.TotalMinutes < 60 ? $"{d.TotalMinutes:F1} min"
             : $"{d.TotalHours:F1} h";

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
                    _logger.LogError(ResilientMqttLogEvents.EventHandlerThrew, ex, "[ResilientMqttClient] OnConnected handler threw");
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
                    _logger.LogError(ResilientMqttLogEvents.EventHandlerThrew, ex, "[ResilientMqttClient] OnDisconnected handler threw");
                }
            }
        }

        private async Task RaiseAsync<TArgs>(Func<TArgs, Task>? handler, TArgs args, string eventName)
        {
            if (handler is null) return;

            foreach (var single in handler.GetInvocationList())
            {
                try
                {
                    var task = ((Func<TArgs, Task>)single).Invoke(args);
                    if (task is not null) await task.ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ResilientMqttLogEvents.EventHandlerThrew, ex, "[ResilientMqttClient] {Event} handler threw", eventName);
                }
            }
        }

        private readonly record struct SubscriptionEntry(IResilientMqttMessageHandler Handler, ResilientMqttQos Qos);

        private readonly record struct HeldMessage(string Topic, string Payload, MqttApplicationMessageReceivedEventArgs? Ack, int Epoch);

        private sealed class DelegateHandler : IResilientMqttMessageHandler
        {
            private readonly Func<string, string, CancellationToken, Task> _fn;
            public DelegateHandler(Func<string, string, CancellationToken, Task> fn) => _fn = fn;
            public Task HandleAsync(string topic, string payload, CancellationToken ct) => _fn(topic, payload, ct);
        }
    }
}

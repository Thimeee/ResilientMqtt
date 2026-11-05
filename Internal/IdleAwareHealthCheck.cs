using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Logging;
using MQTTnet;

namespace ResilientMqtt.Internal
{
    /// <summary>
    /// Timer-based health monitor. On every tick:
    /// <list type="bullet">
    /// <item>client not connected → <c>onDisconnected</c> (start reconnecting);</item>
    /// <item>connected but nothing RECEIVED from the broker for the idle threshold → ping it. No reply →
    /// <c>onPingFailed</c>: the connection is a zombie (MQTTnet still says "connected") and must be reset.</item>
    /// </list>
    /// Only inbound traffic counts as activity (#25): MQTTnet's own keep-alive pings only when nothing was SENT,
    /// so a client that publishes regularly (e.g. QoS0 telemetry) would otherwise never verify the broker → client
    /// direction.
    /// </summary>
    internal sealed class IdleAwareHealthCheck : IHealthCheck
    {
        private readonly TimeSpan _interval;
        private readonly TimeSpan _idleThreshold;
        private readonly TimeSpan _pingTimeout;
        private readonly ILogger _logger;

        private Timer? _timer;
        private IMqttClient? _client;
        private Func<Task>? _onDisconnected;
        private Func<Task>? _onPingFailed;
        private long _lastActivityTicks;
        private int _isRunning;
        private int _disposed;

        public IdleAwareHealthCheck(TimeSpan interval, TimeSpan idleThreshold, TimeSpan pingTimeout, ILogger logger)
        {
            _interval = interval > TimeSpan.Zero ? interval : TimeSpan.FromMinutes(1);
            _idleThreshold = idleThreshold;
            _pingTimeout = pingTimeout > TimeSpan.Zero ? pingTimeout : TimeSpan.FromSeconds(10);
            _logger = logger;
            _lastActivityTicks = DateTime.UtcNow.Ticks;
        }

        public void Start(IMqttClient client, Func<Task> onDisconnected, Func<Task> onPingFailed)
        {
            if (Volatile.Read(ref _disposed) == 1) return;

            _client = client ?? throw new ArgumentNullException(nameof(client));
            _onDisconnected = onDisconnected ?? throw new ArgumentNullException(nameof(onDisconnected));
            _onPingFailed = onPingFailed ?? throw new ArgumentNullException(nameof(onPingFailed));

            var old = Interlocked.Exchange(ref _timer, null);
            old?.Dispose();

            // #15 — clear any stale run-latch left by an in-flight tick of the previous timer,
            // so the first tick of the new timer can't be skipped.
            Interlocked.Exchange(ref _isRunning, 0);

            NotifyActivity();
            _timer = new Timer(OnTick, null, _interval, _interval);
        }

        public void NotifyActivity()
            => Interlocked.Exchange(ref _lastActivityTicks, DateTime.UtcNow.Ticks);

        // Intentional async void: timer callback; the whole body is guarded.
        private async void OnTick(object? _)
        {
            if (Interlocked.CompareExchange(ref _isRunning, 1, 0) != 0) return;

            try
            {
                var client = _client;
                var onDisconnected = _onDisconnected;
                var onPingFailed = _onPingFailed;
                if (client is null || onDisconnected is null || onPingFailed is null) return;

                if (!client.IsConnected)
                {
                    _logger.LogWarning(ResilientMqttLogEvents.HealthClientDisconnected,
                        "[ResilientMqtt] Health check: client reports disconnected");
                    await onDisconnected().ConfigureAwait(false);
                    return;
                }

                var lastActivity = new DateTime(Interlocked.Read(ref _lastActivityTicks), DateTimeKind.Utc);
                if (DateTime.UtcNow - lastActivity <= _idleThreshold) return;

                try
                {
                    // #9 — bound the ping so a hung ping can't leave _isRunning latched and
                    // silently kill the monitor. A timed-out ping is treated as a failed one.
                    using var cts = new CancellationTokenSource(_pingTimeout);
                    await client.PingAsync(cts.Token).ConfigureAwait(false);
                    NotifyActivity();
                }
                catch (Exception ex)
                {
                    if (!client.IsConnected)
                    {
                        // Lost while pinging — the normal disconnect path handles it.
                        await onDisconnected().ConfigureAwait(false);
                        return;
                    }

                    _logger.LogWarning(ResilientMqttLogEvents.HealthPingFailed, ex,
                        "[ResilientMqtt] Health check: no reply to ping within {Seconds}s while the connection looks up — resetting it",
                        _pingTimeout.TotalSeconds);
                    await onPingFailed().ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ResilientMqttLogEvents.HealthTickFailed, ex, "[ResilientMqtt] Health check tick threw");
            }
            finally
            {
                Interlocked.Exchange(ref _isRunning, 0);
            }
        }

        public void Stop()
        {
            var t = Interlocked.Exchange(ref _timer, null);
            t?.Dispose();
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 1) return;
            Stop();
        }
    }
}

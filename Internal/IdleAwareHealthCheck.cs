using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Logging;
using MQTTnet;

namespace ResilientMqtt.Internal
{
    /// <summary>
    /// Timer-based health monitor. Pings broker on idle channels.
    /// </summary>
    internal sealed class IdleAwareHealthCheck : IHealthCheck
    {
        private readonly TimeSpan _interval;
        private readonly TimeSpan _idleThreshold;
        private readonly TimeSpan _pingTimeout;
        private readonly ILogger _logger;

        private Timer? _timer;
        private IMqttClient? _client;
        private Func<Task>? _onUnhealthy;
        private long _lastActivityTicks;
        private int _isRunning;
        private int _disposed;

        public IdleAwareHealthCheck(TimeSpan interval, TimeSpan idleThreshold, TimeSpan pingTimeout, ILogger logger)
        {
            _interval = interval;
            _idleThreshold = idleThreshold;
            _pingTimeout = pingTimeout > TimeSpan.Zero ? pingTimeout : TimeSpan.FromSeconds(10);
            _logger = logger;
            _lastActivityTicks = DateTime.UtcNow.Ticks;
        }

        public void Start(IMqttClient client, Func<Task> onUnhealthy)
        {
            if (Volatile.Read(ref _disposed) == 1) return;

            _client = client ?? throw new ArgumentNullException(nameof(client));
            _onUnhealthy = onUnhealthy ?? throw new ArgumentNullException(nameof(onUnhealthy));

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

        private async void OnTick(object? _)
        {
            if (Interlocked.CompareExchange(ref _isRunning, 1, 0) != 0) return;

            try
            {
                var client = _client;
                var onUnhealthy = _onUnhealthy;
                if (client is null || onUnhealthy is null) return;

                if (!client.IsConnected)
                {
                    _logger.LogWarning("[ResilientMqtt] Health check: client reports disconnected");
                    await onUnhealthy().ConfigureAwait(false);
                    return;
                }

                var lastActivity = new DateTime(Interlocked.Read(ref _lastActivityTicks), DateTimeKind.Utc);
                if (DateTime.UtcNow - lastActivity > _idleThreshold)
                {
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
                        _logger.LogWarning(ex, "[ResilientMqtt] Health check: ping failed");
                        await onUnhealthy().ConfigureAwait(false);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[ResilientMqtt] Health check tick threw");
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

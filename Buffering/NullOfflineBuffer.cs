using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Logging;
using MQTTnet;
using ResilientMqtt.Internal;

namespace ResilientMqtt.Buffering
{
    internal sealed class NullOfflineBuffer : IOfflineBuffer
    {
        public int Count => 0;
        public bool TryEnqueue(MqttApplicationMessage message) => false;
        public bool TryPeek(out MqttApplicationMessage? message) { message = null; return false; }
        public bool TryDequeue(out MqttApplicationMessage? message) { message = null; return false; }
        public Task ClearAsync(CancellationToken ct = default) => Task.CompletedTask;
    }

    /// <summary>In-memory FIFO buffer with drop-oldest overflow policy.</summary>
    internal sealed class InMemoryOfflineBuffer : IOfflineBuffer
    {
        private readonly ConcurrentQueue<MqttApplicationMessage> _queue = new();
        private readonly int _maxSize;
        private readonly ILogger _logger;
        private readonly TimeSpan _logWindow;
        private readonly LogThrottle _overflowLog = new();   // #71

        public InMemoryOfflineBuffer(int maxSize, ILogger logger, TimeSpan? logWindow = null)
        {
            _logWindow = logWindow ?? TimeSpan.FromMinutes(1);
            _maxSize = Math.Max(1, maxSize);
            _logger = logger;
        }

        public int Count => _queue.Count;

        public bool TryEnqueue(MqttApplicationMessage message)
        {
            while (_queue.Count >= _maxSize)
            {
                if (!_queue.TryDequeue(out _)) break;
                if (_overflowLog.ShouldLog(_logWindow, out var skipped))
                    _logger.LogWarning(ResilientMqttLogEvents.BufferOverflow,
                        "[ResilientMqtt] Offline buffer full ({Max}) — oldest dropped{Skipped}", _maxSize, LogThrottle.Suffix(skipped));
            }
            _queue.Enqueue(message);
            return true;
        }

        public bool TryPeek(out MqttApplicationMessage? message)
        {
            var ok = _queue.TryPeek(out var m);
            message = m;
            return ok;
        }

        public bool TryDequeue(out MqttApplicationMessage? message)
        {
            var ok = _queue.TryDequeue(out var m);
            message = m;
            var skipped = _overflowLog.TakeSkipped();   // draining = the overflow is over: report what wasn't logged
            if (skipped > 0)
                _logger.LogInformation(ResilientMqttLogEvents.BufferDropsNotLogged,
                    "[ResilientMqtt] Offline buffer draining — {Count} more dropped message(s) were not logged individually.", skipped);
            return ok;
        }

        public Task ClearAsync(CancellationToken ct = default)
        {
            while (_queue.TryDequeue(out _)) { }
            return Task.CompletedTask;
        }
    }
}

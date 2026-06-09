using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Logging;
using MQTTnet;

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

        public InMemoryOfflineBuffer(int maxSize, ILogger logger)
        {
            _maxSize = maxSize;
            _logger = logger;
        }

        public int Count => _queue.Count;

        public bool TryEnqueue(MqttApplicationMessage message)
        {
            while (_queue.Count >= _maxSize)
            {
                if (_queue.TryDequeue(out _))
                    _logger.LogWarning("[ResilientMqtt] Offline buffer full ({Max}) — oldest dropped", _maxSize);
                else break;
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
            return ok;
        }

        public Task ClearAsync(CancellationToken ct = default)
        {
            while (_queue.TryDequeue(out _)) { }
            return Task.CompletedTask;
        }
    }
}

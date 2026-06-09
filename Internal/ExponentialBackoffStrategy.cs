using System;
using System.Collections.Generic;
using System.Text;

namespace ResilientMqtt.Internal
{
    /// <summary>
    /// Exponential backoff with optional jitter.
    /// Jitter prevents synchronized reconnect storms across many clients.
    /// </summary>
    internal sealed class ExponentialBackoffStrategy : IRetryStrategy
    {
        private readonly TimeSpan _baseDelay;
        private readonly TimeSpan _maxDelay;
        private readonly bool _useJitter;
        private readonly Random _random = new();
        private readonly object _lock = new();

        public ExponentialBackoffStrategy(TimeSpan baseDelay, TimeSpan maxDelay, bool useJitter)
        {
            _baseDelay = baseDelay;
            _maxDelay = maxDelay;
            _useJitter = useJitter;
        }

        public TimeSpan GetNextDelay(int attempt)
        {
            var safeAttempt = Math.Max(0, attempt - 1);
            var exponential = Math.Min(
                _baseDelay.TotalMilliseconds * Math.Pow(2, safeAttempt),
                _maxDelay.TotalMilliseconds);

            if (!_useJitter)
                return TimeSpan.FromMilliseconds(exponential);

            // Full jitter: random in [baseDelay, exponential]
            double jittered;
            lock (_lock)
            {
                jittered = _random.NextDouble() * exponential;
            }
            jittered = Math.Max(jittered, _baseDelay.TotalMilliseconds);
            return TimeSpan.FromMilliseconds(jittered);
        }
    }
}

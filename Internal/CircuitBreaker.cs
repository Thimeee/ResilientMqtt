using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Logging;

namespace ResilientMqtt.Internal
{
    internal enum CircuitState { Closed, Open, HalfOpen }

    /// <summary>
    /// Internal circuit breaker abstraction so it can be disabled via NullCircuitBreaker.
    /// </summary>
    internal interface ICircuitBreaker
    {
        CircuitState State { get; }
        bool TryAcquire();
        void RecordSuccess();
        void RecordFailure();
        void Reset();
    }

    /// <summary>Always-allow stub used when the feature is disabled.</summary>
    internal sealed class NullCircuitBreaker : ICircuitBreaker
    {
        public CircuitState State => CircuitState.Closed;
        public bool TryAcquire() => true;
        public void RecordSuccess() { }
        public void RecordFailure() { }
        public void Reset() { }
    }

    /// <summary>Thread-safe circuit breaker.</summary>
    internal sealed class CircuitBreaker : ICircuitBreaker
    {
        private readonly int _failureThreshold;
        private readonly TimeSpan _resetTimeout;
        private readonly ILogger _logger;
        private readonly object _gate = new();

        private CircuitState _state = CircuitState.Closed;
        private int _consecutiveFailures;
        private DateTime _openedAtUtc = DateTime.MinValue;
        private bool _probeInFlight;   // #10 — only ONE probe allowed through while HalfOpen

        public CircuitBreaker(int failureThreshold, TimeSpan resetTimeout, ILogger logger)
        {
            _failureThreshold = failureThreshold;
            _resetTimeout = resetTimeout;
            _logger = logger;
        }

        public CircuitState State { get { lock (_gate) return _state; } }

        public bool TryAcquire()
        {
            lock (_gate)
            {
                switch (_state)
                {
                    case CircuitState.Closed:
                        return true;
                    case CircuitState.HalfOpen:
                        // #10 — admit a SINGLE probe; hold the rest until it resolves, so a recovering
                        // broker isn't flooded by every queued/new publish the instant it half-opens.
                        if (_probeInFlight) return false;
                        _probeInFlight = true;
                        return true;
                    case CircuitState.Open:
                        if (DateTime.UtcNow - _openedAtUtc >= _resetTimeout)
                        {
                            _state = CircuitState.HalfOpen;
                            _probeInFlight = true;   // this caller IS the probe
                            _logger.LogInformation("[ResilientMqtt] Circuit breaker → HALF-OPEN");
                            return true;
                        }
                        return false;
                }
                return false;
            }
        }

        public void RecordSuccess()
        {
            lock (_gate)
            {
                _consecutiveFailures = 0;
                _probeInFlight = false;   // #10 — probe succeeded (or normal success); allow flow again
                if (_state != CircuitState.Closed)
                {
                    _state = CircuitState.Closed;
                    _logger.LogInformation("[ResilientMqtt] Circuit breaker → CLOSED");
                }
            }
        }

        public void RecordFailure()
        {
            lock (_gate)
            {
                _consecutiveFailures++;
                if (_state == CircuitState.HalfOpen)
                {
                    _state = CircuitState.Open;
                    _openedAtUtc = DateTime.UtcNow;
                    _probeInFlight = false;   // #10 — probe failed; a fresh probe is allowed after the next reset
                    _logger.LogWarning("[ResilientMqtt] Circuit breaker → OPEN (probe failed)");
                }
                else if (_state == CircuitState.Closed && _consecutiveFailures >= _failureThreshold)
                {
                    _state = CircuitState.Open;
                    _openedAtUtc = DateTime.UtcNow;
                    _logger.LogWarning("[ResilientMqtt] Circuit breaker → OPEN after {Failures} failures",
                        _consecutiveFailures);
                }
            }
        }

        public void Reset()
        {
            lock (_gate)
            {
                _consecutiveFailures = 0;
                _state = CircuitState.Closed;
                _probeInFlight = false;
            }
        }
    }
}

using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Logging;

namespace ResilientMqtt.Internal
{
    internal enum CircuitState { Closed, Open, HalfOpen }

    /// <summary>
    /// Internal circuit breaker abstraction so it can be disabled via NullCircuitBreaker.
    /// <para>Contract: every successful <see cref="TryAcquire"/> is followed by exactly one of
    /// <see cref="RecordSuccess"/>, <see cref="RecordFailure"/> or <see cref="RecordAbandoned"/> — the caller that
    /// was handed the HalfOpen probe must release it on every exit path (#28).</para>
    /// </summary>
    internal interface ICircuitBreaker
    {
        CircuitState State { get; }

        /// <summary>true = go ahead. <paramref name="isProbe"/> = this caller is the single HalfOpen probe.</summary>
        bool TryAcquire(out bool isProbe);

        /// <summary>Returns true when this success CLOSED a previously open/half-open circuit.</summary>
        bool RecordSuccess();

        void RecordFailure();

        /// <summary>The acquired call ended without a connectivity verdict (not connected, caller cancelled).</summary>
        void RecordAbandoned(bool isProbe);

        void Reset();
    }

    /// <summary>Always-allow stub used when the feature is disabled.</summary>
    internal sealed class NullCircuitBreaker : ICircuitBreaker
    {
        public CircuitState State => CircuitState.Closed;
        public bool TryAcquire(out bool isProbe) { isProbe = false; return true; }
        public bool RecordSuccess() => false;
        public void RecordFailure() { }
        public void RecordAbandoned(bool isProbe) { }
        public void Reset() { }
    }

    /// <summary>Thread-safe circuit breaker.</summary>
    internal sealed class CircuitBreaker : ICircuitBreaker
    {
        private readonly int _failureThreshold;
        private readonly TimeSpan _resetTimeout;
        private readonly ILogger _logger;
        private readonly Func<DateTime> _utcNow;
        private readonly object _gate = new();

        private CircuitState _state = CircuitState.Closed;
        private int _consecutiveFailures;
        private DateTime _openedAtUtc = DateTime.MinValue;
        private bool _probeInFlight;   // #10 — only ONE probe allowed through while HalfOpen

        public CircuitBreaker(int failureThreshold, TimeSpan resetTimeout, ILogger logger, Func<DateTime>? utcNow = null)
        {
            _failureThreshold = Math.Max(1, failureThreshold);
            _resetTimeout = resetTimeout;
            _logger = logger;
            _utcNow = utcNow ?? (() => DateTime.UtcNow);
        }

        public CircuitState State { get { lock (_gate) return _state; } }

        public bool TryAcquire(out bool isProbe)
        {
            lock (_gate)
            {
                isProbe = false;
                switch (_state)
                {
                    case CircuitState.Closed:
                        return true;
                    case CircuitState.HalfOpen:
                        // #10 — admit a SINGLE probe; hold the rest until it resolves, so a recovering
                        // broker isn't flooded by every queued/new publish the instant it half-opens.
                        if (_probeInFlight) return false;
                        _probeInFlight = true;
                        isProbe = true;
                        return true;
                    case CircuitState.Open:
                        if (_utcNow() - _openedAtUtc >= _resetTimeout)
                        {
                            _state = CircuitState.HalfOpen;
                            _probeInFlight = true;   // this caller IS the probe
                            isProbe = true;
                            _logger.LogInformation(ResilientMqttLogEvents.CircuitHalfOpen, "[ResilientMqtt] Circuit breaker → HALF-OPEN");
                            return true;
                        }
                        return false;
                }
                return false;
            }
        }

        public bool RecordSuccess()
        {
            lock (_gate)
            {
                _consecutiveFailures = 0;
                _probeInFlight = false;   // #10 — probe succeeded (or normal success); allow flow again
                if (_state == CircuitState.Closed) return false;

                _state = CircuitState.Closed;
                _logger.LogInformation(ResilientMqttLogEvents.CircuitClosed, "[ResilientMqtt] Circuit breaker → CLOSED");
                return true;
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
                    _openedAtUtc = _utcNow();
                    _probeInFlight = false;   // #10 — probe failed; a fresh probe is allowed after the next reset
                    _logger.LogWarning(ResilientMqttLogEvents.CircuitOpened, "[ResilientMqtt] Circuit breaker → OPEN (probe failed)");
                }
                else if (_state == CircuitState.Closed && _consecutiveFailures >= _failureThreshold)
                {
                    _state = CircuitState.Open;
                    _openedAtUtc = _utcNow();
                    _logger.LogWarning(ResilientMqttLogEvents.CircuitOpened,
                        "[ResilientMqtt] Circuit breaker → OPEN after {Failures} failures", _consecutiveFailures);
                }
            }
        }

        // #28 — the probe holder left without a verdict (e.g. the client was disconnected, or the caller
        // cancelled). Free the probe slot so the next publish can probe, instead of blocking every publish
        // until the next reconnect resets the breaker.
        public void RecordAbandoned(bool isProbe)
        {
            if (!isProbe) return;
            lock (_gate) { _probeInFlight = false; }
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

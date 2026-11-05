using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Extensions.Options;

namespace ResilientMqtt.Internal
{
    /// <summary>
    /// Validates <see cref="ResilientMqttOptions"/> with one clear message per problem (registered by
    /// <c>ValidateOnStart()</c>). Replaces the 1.0.3 single "Invalid ResilientMqtt configuration" message.
    /// </summary>
    internal sealed class ResilientMqttOptionsValidator : IValidateOptions<ResilientMqttOptions>
    {
        public ValidateOptionsResult Validate(string? name, ResilientMqttOptions options)
        {
            var errors = Check(options);
            return errors.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(errors);
        }

        internal static List<string> Check(ResilientMqttOptions o)
        {
            var e = new List<string>();
            const string P = "ResilientMqtt: ";

            if (string.IsNullOrWhiteSpace(o.Host)) e.Add(P + "Host is required.");
            if (o.Port <= 0 || o.Port > 65535) e.Add(P + $"Port must be 1–65535 (got {o.Port}).");
            if (string.IsNullOrWhiteSpace(o.ClientId)) e.Add(P + "ClientId is required.");
            if (o.KeepAliveSeconds < 0) e.Add(P + "KeepAliveSeconds must be >= 0 (0 = no keep-alive).");
            if (o.PublishTimeoutSeconds <= 0) e.Add(P + "PublishTimeoutSeconds must be > 0.");

            try { ResilientMqttClient.ParseProtocolVersion(o.ProtocolVersion); }
            catch (InvalidOperationException ex) { e.Add(ex.Message); }
            if (o.SessionExpirySeconds is < 0) e.Add(P + "SessionExpirySeconds must be >= 0 (or omitted).");

            // Reconnect
            if (o.Reconnect.ConnectTimeoutSeconds <= 0) e.Add(P + "Reconnect.ConnectTimeoutSeconds must be > 0.");
            if (o.Reconnect.StillDownAfterSeconds < 0) e.Add(P + "Reconnect.StillDownAfterSeconds must be >= 0 (0 = off).");
            if (o.Reconnect.BaseDelaySeconds < 0) e.Add(P + "Reconnect.BaseDelaySeconds must be >= 0.");
            if (o.Reconnect.MaxDelaySeconds < o.Reconnect.BaseDelaySeconds)
                e.Add(P + "Reconnect.MaxDelaySeconds must be >= Reconnect.BaseDelaySeconds.");
            if (o.Reconnect.AuthRejectedDelaySeconds < 0) e.Add(P + "Reconnect.AuthRejectedDelaySeconds must be >= 0 (0 = off).");

            // Health check / buffer / breaker
            if (o.HealthCheck.PingTimeoutSeconds <= 0) e.Add(P + "HealthCheck.PingTimeoutSeconds must be > 0.");
            if (o.HealthCheck.Enabled && o.HealthCheck.IntervalMinutes <= 0) e.Add(P + "HealthCheck.IntervalMinutes must be > 0.");
            if (o.HealthCheck.IdleThresholdMinutes < 0) e.Add(P + "HealthCheck.IdleThresholdMinutes must be >= 0.");
            if (o.OfflineBuffer.Enabled && o.OfflineBuffer.MaxSize <= 0) e.Add(P + "OfflineBuffer.MaxSize must be > 0.");
            if (o.OfflineBuffer.DrainPauseMs < 0) e.Add(P + "OfflineBuffer.DrainPauseMs must be >= 0.");
            if (o.CircuitBreaker.Enabled && o.CircuitBreaker.FailureThreshold <= 0) e.Add(P + "CircuitBreaker.FailureThreshold must be > 0.");
            if (o.CircuitBreaker.ResetMinutes < 0) e.Add(P + "CircuitBreaker.ResetMinutes must be >= 0.");

            // Dispatch
            try { ResilientMqttClient.ParseDispatchMode(o.Dispatch.Mode); }
            catch (InvalidOperationException ex) { e.Add(ex.Message); }
            if (o.Dispatch.MaxConcurrency <= 0) e.Add(P + "Dispatch.MaxConcurrency must be > 0.");
            if (o.Dispatch.ShutdownWaitSeconds < 0) e.Add(P + "Dispatch.ShutdownWaitSeconds must be >= 0.");
            if (o.Dispatch.MaxPending < 0) e.Add(P + "Dispatch.MaxPending must be >= 0.");

            // TLS
            if (o.Tls.Enabled)
            {
                switch (TlsConfigurator.ParseMode(o.Tls.Mode))
                {
                    case TlsMode.Hard:
                        if (string.IsNullOrWhiteSpace(o.Tls.TrustedThumbprint))
                            e.Add(P + "Tls.Mode='Hard' needs Tls.TrustedThumbprint.");
                        break;
                    case TlsMode.Ca:
                        if (string.IsNullOrWhiteSpace(o.Tls.CaCertificatePath) || !File.Exists(o.Tls.CaCertificatePath))
                            e.Add(P + $"Tls.Mode='Ca' needs an existing Tls.CaCertificatePath (got '{o.Tls.CaCertificatePath}').");
                        break;
                    case null:
                        e.Add(P + $"Tls.Mode '{o.Tls.Mode}' is unknown. Use None, Normal, Hard or Ca.");
                        break;
                }

                if (!string.IsNullOrWhiteSpace(o.Tls.ClientCertificatePath) && !File.Exists(o.Tls.ClientCertificatePath))
                    e.Add(P + $"Tls.ClientCertificatePath not found: '{o.Tls.ClientCertificatePath}'.");
                if (!string.IsNullOrWhiteSpace(o.Tls.ClientCertificatePath) && !string.IsNullOrWhiteSpace(o.Tls.ClientCertificateThumbprint))
                    e.Add(P + "Set either Tls.ClientCertificatePath or Tls.ClientCertificateThumbprint, not both.");
                var store = o.Tls.ClientCertificateStore ?? "LocalMachine";
                if (!store.Equals("LocalMachine", StringComparison.OrdinalIgnoreCase) && !store.Equals("CurrentUser", StringComparison.OrdinalIgnoreCase))
                    e.Add(P + "Tls.ClientCertificateStore must be LocalMachine or CurrentUser.");
            }

            return e;
        }
    }
}

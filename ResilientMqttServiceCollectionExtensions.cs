using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using ResilientMqtt.Buffering;
using ResilientMqtt.Internal;
using MQTTnet;

namespace ResilientMqtt
{


    /// <summary>
    /// Fluent builder returned by <c>AddResilientMqtt()</c>. Lets the host app
    /// override options, swap the offline buffer, or hook events.
    /// </summary>
    public sealed class ResilientMqttBuilder
    {
        public IServiceCollection Services { get; }

        internal ResilientMqttBuilder(IServiceCollection services)
        {
            Services = services;
        }

        /// <summary>Bind options from an <see cref="IConfiguration"/> section.</summary>
        public ResilientMqttBuilder Configure(IConfiguration configurationSection)
        {
            if (configurationSection is null) throw new ArgumentNullException(nameof(configurationSection));
            // Use OptionsBuilder.Bind to avoid potential extension-method ambiguity
            // with the other Configure(Action<>) overload.
            Services
                .AddOptions<ResilientMqttOptions>()
                .Bind(configurationSection);
            return this;
        }

        /// <summary>Override options in code (additive — runs after Configure).</summary>
        public ResilientMqttBuilder Configure(Action<ResilientMqttOptions> configure)
        {
            if (configure is null) throw new ArgumentNullException(nameof(configure));
            Services.PostConfigure(configure);
            return this;
        }

        /// <summary>
        /// Cross-bind options from another options object (e.g. derive ClientId
        /// from an app-level config). Runs after Configure.
        /// </summary>
        public ResilientMqttBuilder ConfigureFromDependency<TDep>(Action<ResilientMqttOptions, TDep> configure)
            where TDep : class
        {
            if (configure is null) throw new ArgumentNullException(nameof(configure));
            Services
                .AddOptions<ResilientMqttOptions>()
                .Configure<IOptions<TDep>>((mqtt, dep) => configure(mqtt, dep.Value));
            return this;
        }

        /// <summary>Replace the default in-memory offline buffer (e.g. with a SQLite-backed one).</summary>
        public ResilientMqttBuilder UseOfflineBuffer<TBuffer>() where TBuffer : class, IOfflineBuffer
        {
            Services.RemoveAll<IOfflineBuffer>();
            Services.AddSingleton<IOfflineBuffer, TBuffer>();
            return this;
        }

        /// <summary>Provide a buffer instance directly.</summary>
        public ResilientMqttBuilder UseOfflineBuffer(IOfflineBuffer buffer)
        {
            if (buffer is null) throw new ArgumentNullException(nameof(buffer));
            Services.RemoveAll<IOfflineBuffer>();
            Services.AddSingleton(buffer);
            return this;
        }

        /// <summary>Validate options at host startup. Recommended.</summary>
        public ResilientMqttBuilder ValidateOnStart()
        {
            Services
                .AddOptions<ResilientMqttOptions>()
                .Validate(o =>
                {
                    if (string.IsNullOrWhiteSpace(o.Host)) return false;
                    if (o.Port <= 0 || o.Port > 65535) return false;
                    if (string.IsNullOrWhiteSpace(o.ClientId)) return false;
                    if (o.Tls.Enabled && o.Tls.Mode == "Hard" && string.IsNullOrWhiteSpace(o.Tls.TrustedThumbprint))
                        return false;
                    return true;
                }, "Invalid ResilientMqtt configuration. Check Host, Port, ClientId, and Tls settings.")
                .ValidateOnStart();
            return this;
        }
    }

    /// <summary>
    /// One-stop entry point. Add to your DI container with one of:
    /// <code>
    /// // Option A: from configuration
    /// services.AddResilientMqtt(builder.Configuration.GetSection("Mqtt"));
    ///
    /// // Option B: code-only
    /// services.AddResilientMqtt(opts => {
    ///     opts.Host = "broker.lk";
    ///     opts.Port = 1883;
    ///     opts.ClientId = "my-app";
    ///     opts.OfflineBuffer.Enabled = false;
    /// });
    ///
    /// // Option C: combined + chained
    /// services.AddResilientMqtt()
    ///         .Configure(config.GetSection("Mqtt"))
    ///         .Configure(opts => opts.ClientId = $"app-{Environment.MachineName}")
    ///         .UseOfflineBuffer&lt;MySqliteBuffer&gt;()
    ///         .ValidateOnStart();
    /// </code>
    /// </summary>
    public static class ResilientMqttServiceCollectionExtensions
    {
        public static ResilientMqttBuilder AddResilientMqtt(this IServiceCollection services)
        {
            if (services is null) throw new ArgumentNullException(nameof(services));

            // Default options if no Configure call is made
            services.AddOptions<ResilientMqttOptions>();

            // MQTTnet client (singleton)
            services.TryAddSingleton(_ => new MqttClientFactory().CreateMqttClient());

            // Retry strategy
            services.TryAddSingleton<IRetryStrategy>(sp =>
            {
                var opts = sp.GetRequiredService<IOptions<ResilientMqttOptions>>().Value;
                return new ExponentialBackoffStrategy(
                    TimeSpan.FromSeconds(opts.Reconnect.BaseDelaySeconds),
                    TimeSpan.FromSeconds(opts.Reconnect.MaxDelaySeconds),
                    opts.Reconnect.UseJitter);
            });

            // Health check (real or null)
            services.TryAddSingleton<IHealthCheck>(sp =>
            {
                var opts = sp.GetRequiredService<IOptions<ResilientMqttOptions>>().Value;
                if (!opts.HealthCheck.Enabled)
                    return new NullHealthCheck();

                var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
                return new IdleAwareHealthCheck(
                    TimeSpan.FromMinutes(opts.HealthCheck.IntervalMinutes),
                    TimeSpan.FromMinutes(opts.HealthCheck.IdleThresholdMinutes),
                    loggerFactory.CreateLogger("ResilientMqtt.HealthCheck"));
            });

            // Offline buffer (real or null)
            services.TryAddSingleton<IOfflineBuffer>(sp =>
            {
                var opts = sp.GetRequiredService<IOptions<ResilientMqttOptions>>().Value;
                if (!opts.OfflineBuffer.Enabled)
                    return new NullOfflineBuffer();

                var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
                return new InMemoryOfflineBuffer(
                    opts.OfflineBuffer.MaxSize,
                    loggerFactory.CreateLogger("ResilientMqtt.OfflineBuffer"));
            });

            // Circuit breaker (real or null)
            services.TryAddSingleton<ICircuitBreaker>(sp =>
            {
                var opts = sp.GetRequiredService<IOptions<ResilientMqttOptions>>().Value;
                if (!opts.CircuitBreaker.Enabled)
                    return new NullCircuitBreaker();

                var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
                return new CircuitBreaker(
                    opts.CircuitBreaker.FailureThreshold,
                    TimeSpan.FromMinutes(opts.CircuitBreaker.ResetMinutes),
                    loggerFactory.CreateLogger("ResilientMqtt.CircuitBreaker"));
            });

            // Main client — register OUR interface mapping to OUR implementation.
            // (The underlying MQTTnet IMqttClient is already registered at line 128.)
            services.TryAddSingleton<IResilientMqttClient, ResilientMqttClient>();

            return new ResilientMqttBuilder(services);
        }

        /// <summary>Add with configuration section binding.</summary>
        public static ResilientMqttBuilder AddResilientMqtt(this IServiceCollection services, IConfiguration configurationSection)
            => services.AddResilientMqtt().Configure(configurationSection);

        /// <summary>Add with code-based configuration.</summary>
        public static ResilientMqttBuilder AddResilientMqtt(this IServiceCollection services, Action<ResilientMqttOptions> configure)
            => services.AddResilientMqtt().Configure(configure);
        /// <summary>
        /// Convenience: register a hosted service that automatically connects on
        /// app start and gracefully disconnects on app stop.
        /// </summary>
        public static ResilientMqttBuilder AddAutoStart(this ResilientMqttBuilder builder)
        {
            builder.Services.AddHostedService<ResilientMqttHostedService>();
            return builder;
        }
    }
}